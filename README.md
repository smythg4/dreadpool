# dreadpool
Let's build a work-stealing threadpool with maximum effort.

## Overview
This is a learning project to explore how work-stealing threadpools work in Rust.

**History** - I started originally with a simple threadpool and `flume` channels. Each thread had its own local queue with a central task dispatcher, but nothing prevented work starvation.

**The Goal** - Inspired by `Go` and `tokio`, I set out to end thread hunger. This README is the story of that journey.

## Getting Started
```
use dreadpool::ThreadPoolBuilder;
use std::sync::{Arc, Mutex};

fn main() {
    let pool = ThreadPoolBuilder::default()
        .with_threads(4)
        .with_thread_name("dreadpool-worker")
        .build();

    let counter = Arc::new(Mutex::new(0));

    for _ in 0..100 {
        let counter = Arc::clone(&counter);
        pool.spawn(move || {
            let mut n = counter.lock().unwrap();
            *n += 1;
        });
    }

    pool.join(); // blocks until all tasks complete

    println!("Final count: {}", *counter.lock().unwrap()); // 100
}
```

## Design
This project depends heavily on the excellent [`crossbeam`](https://docs.rs/crossbeam/latest/crossbeam/) crate. Specifically leveraging [`Deque`](https://docs.rs/crossbeam/latest/crossbeam/deque/index.html).

Here's the idea:
1. `ThreadPool::spawn` serves as a central dispatcher and pushes new `Task`s onto a global queue
2. Worker threads each maintain a local queue of `Task`s. They loop in the following order:
    * Top off the local queue to a target size by pulling off the global queue
    * Pull a `Task` off the local queue and run it.
    * If there's nothing on the local queue, it will steal a batch of `Tasks` from the first available other worker local queue.
    * If there's no work on anyone's queue, the worker checks a global flag to indicate whether it's time to shutdown.
    * If it's not time to shutdown, the worker thread goes to sleep. It will be notified to wake up by a call to `ThreadPool::spawn`.

### The `Task` Type
Our unit of work is the `Task`, defined as:
```
type Task = Box<dyn FnOnce() + Send>;
```
`FnOnce` covers any closure that can be called exactly once — the natural fit for a unit of work
that runs and completes. `Send` is required since tasks will cross thread boundaries. We wrap it in
`Box` because trait objects are unsized and need a fixed-size pointer to be stored and passed
around.

Callers are required to pass `'static` closures to `spawn`, meaning closures must own their captures
rather than borrow from the caller's stack — the compiler enforces this at the call site.

### `WorkerContext`
Each worker thread is passed a `WorkerContext` containing everything it needs to operate independently:
```
struct WorkerContext {
    name: Option<String>,
    global: Arc<Injector<Task>>,
    worker: Worker<Task>,
    stealers: Arc<Vec<Stealer<Task>>>,
    id: usize,
    shutdown: Arc<AtomicBool>,
    mcv: Arc<(Condvar, Mutex<()>)>,
    stack_size: Option<usize>,
}
```
- **`global`** — the shared global queue from which a worker thread can pull `Task`s (see below)
- **`worker`** — the local queue owned by this worker thread (see below)
- **`stealers`** — sneaky backdoors to all the other threads' `Worker` queues (see below)
- **`shutdown`** — an `AtomicBool` set to `true` when the `ThreadPool` drops, signaling workers to
drain remaining tasks and terminate
- **`mcv`** — a `(Condvar, Mutex<()>)` pair used to sleep idle workers and wake them on `spawn`
- **`name`** / **`stack_size`** — optional configuration set during the build phase

## Required Equipment
[`crossbeam::deque`](https://docs.rs/crossbeam/latest/crossbeam/deque/index.html) gives us some nifty tools to tackle this challenge. This `deque` comes in three flavors for us: `Injector`, `Worker`, and `Stealer`.

### The Global Queue - `crossbeam::deque::Injector`
This FIFO queue serves as our main entry point for `Task` scheduling. When a caller calls `ThreadPool::spawn(t)` where `t` is of type `impl FnOnce() + Send + 'static`, `spawn` puts it in a `Box` (making it a `Task`) and pushes it onto the global queue. `Send` is required since these tasks will likely move between threads. `'static` is required since we need to assure the compiler that our data will outlive the thread's lifetime.

In the worker thread, we run an infinite loop that start each iteration by comparing its current local `Task` queue size to a pre-defined ideal backlog size. If it doesn't have a sufficient backlog, we call `steal_batch_with_limit_and_pop` on the global queue. This will remove roughly half of the global queue's `Task`s but no more than the limit we provide it (which we calculate to the be the difference between the current queue length and the ideal). This method also usefully `pop`s the first element off to give us a `Task` to work on right away.

### The Local Queues - `crossbeam::deque::Worker`
We use the FIFO variety of `Worker`, which is owned by a single thread, but other threads may steal from it (more on that later). In the worker thread loop, assuming we have a sufficient local backlog we pull the first `Task` off the local queue and run it.

You'll notice that we essentially have a race between workers to gobble up work from the global queue, then start working on their local queues.

But what happens when a worker thread's local queue is empty?

### The Secret Sauce - `crossbeam::deque::Stealer`
Before we spawn our threads, we make a series of `Worker` queues
```
    let workers: Vec<_> = (0..num_workers)
        .map(|_| Worker::<Task>::new_fifo())
        .collect();
```

`Worker` has a method called `stealer()` that creates a `Stealer` queue that can be shared across threads and cloned.
```
    let stealers: Arc<Vec<_>> = Arc::new(
        workers.iter()
                .map(|w| w.stealer())
                .collect()
        );
```

Each worker thread is spawned with its own, owned `Worker` queue, as well as a list of `Stealer` queues so it can steal from all its friends. Tasks are stolen from the end opposite to where they get pushed.

If both the global and local queue are empty, the worker will check the list of `Stealers` and call `steal_batch_and_pop`, similar to the call the global queue, but without a fixed limit. It will transfer roughly half of the other thread's `Worker` queue into our local queue, popping one off for immediate handling in the process.

## Lesson Learning - The Lost Wakeup
When I initially wrote this, the workers spun in busy loops constantly looking for work until they were told to shutdown. In an effort to increase efficiency and all threads to go to sleep, I added the `mcv` construct to the `WorkerContext`.

It had been a minute since I'd used `CondVar`s, and wrote my sleep logic in `worker_loop` like this:
```
// no work found in global or local queues and nothing to steal
...
    if ctx.shutdown.load(Ordering::Acquire) {
        println!("[{id}] got the signal to shutdown and all the queues are empty...");
        break;
    }
    let guard = ctx.mcv.1.lock().unwrap();
    let _guard = ctx.mcv.0.wait(guard).unwrap();
```
and my wakeup logic in `spawn` like this:
```
...
    self.global.push(Box::new(f));
    self.mcv.0.notify_one();
```

I had two critical bugs here:
1. The worker thread could be at the end of its loop, after finding nothing when someone pushes something onto the global task queue. This thread would miss that signal, go to sleep, and never get the signal to wake up.
2. In `spawn` it was possible for a thread to be calling `.wait` on the `CondVar` at the exact time `spawn` is calling `.notify_one`.

### The Fixes
1. **Global Queue Capacity Double Check** - before calling `.wait`, the worker thread double checks the global queue's length.
```
    ...
    let guard = ctx.mcv.1.lock().unwrap();
    // double check if work was plopped in the queue before deciding to sleep
    if !ctx.global.is_empty() {
        continue;
    }
    if ctx.shutdown.load(Ordering::Acquire) {
        break;
    }
    let _guard = ctx.mcv.0.wait(guard).unwrap();
```
We hold the `Mutex` guard across the global queue and `shutdown` checks to prevent a wake up occurring while a worker thread is trying to register with `.wait`

2. **`spawn` Lock Acquisition** - take the lock before calling notify on the `CondVar`. This forces `spawn` to wait until worker threads have registered that they want to be woken up. Without this, it's possible for `spawn` to quickly work through this section and issue a wakeup command before a worker thread has actually gone to sleep.
```
    // take the lock to force a wait on any worker threads trying to register
    let _guard = self.mcv.1.lock().unwrap();
    self.global.push(Box::new(f));
    // wake up a waiting thead
    self.mcv.0.notify_one();
```
This issue also applied to `ThreadPool`'s `Drop` implementation, where it calls `.notify_all` to wake up any sleeping worker threads to drain the queue or see that all the work is complete.

## Todo
* Add capacity to detect thread panics and add a new thread to the pool in that event
* Add doc comments to project
