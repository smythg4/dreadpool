use crossbeam::deque::{Steal, Stealer, Worker};
use crossbeam::queue::SegQueue;
use std::{
    sync::atomic::AtomicBool,
    sync::atomic::Ordering,
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
};

type Task = Box<dyn FnOnce() + Send>;

const IDEAL_WORKER_BACKLOG: usize = 8;
const BATCH_GRAB_COUNT: usize = IDEAL_WORKER_BACKLOG / 2;

struct WorkerContext {
    global: Arc<SegQueue<Task>>,
    worker: Worker<Task>,
    stealers: Arc<Vec<Stealer<Task>>>,
    index: usize, // so a worker skips stealing from itself
    shutdown: Arc<AtomicBool>,
    mcv: Arc<(Condvar, Mutex<()>)>, // Mutex guards a value that indicates the number of pending tasks for processing.
}

fn worker_loop(ctx: WorkerContext) {
    let id = ctx.index;
    loop {
        // top off the local queue with global queue values
        if !ctx.global.is_empty() && ctx.worker.len() < IDEAL_WORKER_BACKLOG {
            let mut count = 0;
            for _ in 0..BATCH_GRAB_COUNT {
                if let Some(task) = ctx.global.pop() {
                    ctx.worker.push(task);
                    count += 1;
                }
            }
            println!("[{id}] pulled {count} tasks from the GLOBAL queue into its LOCAL queue");
        }

        // drain local queue first
        if let Some(task) = ctx.worker.pop() {
            println!("[{id}] pulling a task from its LOCAL queue");
            task();
            continue;
        }

        // attempt to steal from other workers
        let mut all_empty = true;
        for (i, stealer) in ctx.stealers.iter().enumerate() {
            if i == id {
                continue;
            }
            match stealer.steal_batch_and_pop(&ctx.worker) {
                Steal::Success(task) => {
                    println!("[{id}] stealing tasks from worker {i}");
                    task();
                    all_empty = false;
                    break;
                }
                Steal::Retry => {
                    all_empty = false;
                }
                Steal::Empty => {}
            }
        }

        // only exit if shutdown is signaled AND everything is truly empty
        if !all_empty {
            continue;
        }

        let guard = ctx.mcv.1.lock().unwrap();
        // double check if work was plopped in the queue before deciding to sleep
        if !ctx.global.is_empty() {
            continue;
        }
        if ctx.shutdown.load(Ordering::Acquire) {
            println!("[{id}] got the signal to shutdown and all the queues are empty...");
            break;
        }
        // all queues were empty and the shutdown signal isn't there, time to sleep
        println!("[{id}] has no work to do, going to sleep...");

        let _guard = ctx.mcv.0.wait(guard).unwrap();
        println!("[{id}] someone woke me up, time to work!");
    }
}

pub struct ThreadPool {
    global: Arc<SegQueue<Task>>,    // the global queue of work to do
    shutdown: Arc<AtomicBool>,      // a global signal that the threadpool is shutting down
    mcv: Arc<(Condvar, Mutex<()>)>, // signal used for sleeping and waking threads in the pool
    workers: Vec<JoinHandle<()>>,
}

impl ThreadPool {
    pub fn new(num_workers: usize) -> Self {
        let global = Arc::new(SegQueue::default());
        let mut worker_threads = Vec::with_capacity(num_workers);

        // Generate workers list
        let workers: Vec<_> = (0..num_workers)
            .map(|_| Worker::<Task>::new_fifo())
            .collect();
        let stealers: Arc<Vec<_>> = Arc::new(workers.iter().map(|w| w.stealer()).collect());

        // Generate Condvar construct
        let mcv = Arc::new((Condvar::new(), Mutex::new(())));
        // Generate shutdown signal
        let shutdown = Arc::new(AtomicBool::new(false));

        for (i, worker) in workers.into_iter().enumerate() {
            eprintln!("Creating Worker {i}...");

            let ctx = WorkerContext {
                global: Arc::clone(&global),
                worker,
                stealers: Arc::clone(&stealers),
                index: i,
                shutdown: Arc::clone(&shutdown),
                mcv: Arc::clone(&mcv),
            };
            let handle = thread::spawn(move || worker_loop(ctx));
            worker_threads.push(handle);
        }
        Self {
            global,
            shutdown,
            mcv,
            workers: worker_threads,
        }
    }

    pub fn spawn(&self, f: impl FnOnce() + Send + 'static) {
        let _guard = self.mcv.1.lock().unwrap();
        self.global.push(Box::new(f));
        // wake up a waiting thead
        self.mcv.0.notify_one();
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        println!("Shutting pool down...");
        let _guard = self.mcv.1.lock().unwrap();
        self.shutdown.store(true, Ordering::Release);
        println!("Waking up any sleeping threads...");
        self.mcv.0.notify_all();
        drop(_guard);
        for (i, worker) in self.workers.drain(..).enumerate() {
            println!("Waiting on Worker {i}...");
            worker.join().unwrap();
        }
    }
}

fn main() {
    println!("Run the test...");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn test_tasks_execute() {
        let num_threads = 5;
        let max_wait = 50;
        let random_task = move |counter: Arc<Mutex<usize>>| {
            let rand = rand::random_range(0..=max_wait);
            std::thread::sleep(Duration::from_millis(rand));
            let mut n = counter.lock().unwrap();
            *n += 1;
        };
        let expected = 100;
        let pool = ThreadPool::new(num_threads);
        let counter = Arc::new(Mutex::new(0));
        let start = std::time::Instant::now();
        for _ in 0..expected {
            let counter = Arc::clone(&counter);
            pool.spawn(move || random_task.clone()(counter));
        }
        drop(pool); // waits for all tasks to finish
        let elapsed = start.elapsed();
        println!(
            "Took {elapsed:?} to complete compared to the max sequential time of {} ms",
            num_threads as u64 * max_wait
        );
        assert_eq!(*counter.lock().unwrap(), expected);
    }
}
