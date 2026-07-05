# dreadpool
Let's build a work-stealing threadpool with maximum effort.

## Overview
This is a learning project to explore how work-stealing threadpools work in Rust. I started originally with a simple threadpool and `flume` channels. Each thread had its own local queue with a central task dispatcher, but nothing prevented work starvation. Inspired by `Go` and `tokio`, I set out to end thread hunger. This README is the story of that journey.

## Design
This project depends heavily on the excellent [`crossbeam`](https://docs.rs/crossbeam/latest/crossbeam/) crate. Specifically leveraging [`Deque`](https://docs.rs/crossbeam/latest/crossbeam/deque/index.html).

Here's the idea:
1. `threadpool::spawn` serves as a central dispatcher and pushes new `Task`s onto a global queue
2. Worker threads each maintain a local queue of `Task`s. They loop in the following order:
    * Top off the local queue to a target size by pulling off the global queue
    * Pull a `Task` off the local queue and run it.
    * If there's nothing on the local queue, it will steal a batch of `Tasks` from the first available other worker local queue.
    * If there's no work on anyone's queue, the worker checks a global flag to indicate whether it's time to shutdown.
    * If it's not time to shutdown, the worker thread goes to sleep. It will be notified to wake up by a call to `threadpool::spawn`.

## `WorkerContext`
Each thread is passed a `WorkerContext` containing everything it needs to operate independently:
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
- **`global`** — the shared global queue from which a worker thread can pull `Task`s
- **`worker`** — the local queue owned by this worker thread
- **`stealers`** — sneaky backdoors to all the other threads' `Worker` queues
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

Each worker thread is spawned with its own, owned `Worker` queue, as well as a list of `Stealer` queues so it can steal from all its friends.

If both the global and local queue are empty, the worker will check the list of `Stealers` and call `steal_batch_and_pop`, similar to the call the global queue, but without a fixed limit. It will transfer roughly half of the other thread's `Worker` queue into our local queue, popping one off for immediate handling in the process.

## Todo
* Add capacity to detect thread panics and add a new thread to the pool in that event
* Beef up this README by explaining thread safety concerns and the `Task` type.
* Add examples to README
* Add doc comments to project
* A README section about my "lost wakeup" adventure