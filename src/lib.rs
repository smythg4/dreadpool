use crossbeam::deque::{Injector, Steal, Stealer, Worker};
use std::{
    sync::atomic::AtomicBool,
    sync::atomic::Ordering,
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
};

type Task = Box<dyn FnOnce() + Send>;

const IDEAL_WORKER_BACKLOG: usize = 10; // completely arbitrary choice

struct WorkerContext {
    name: Option<String>,
    global: Arc<Injector<Task>>,
    worker: Worker<Task>,
    stealers: Arc<Vec<Stealer<Task>>>,
    id: usize, // so a worker skips stealing from itself
    shutdown: Arc<AtomicBool>,
    mcv: Arc<(Condvar, Mutex<()>)>,
    stack_size: Option<usize>,
}

fn worker_loop(ctx: WorkerContext) {
    let id = ctx.id;
    loop {
        let to_grab = IDEAL_WORKER_BACKLOG.saturating_sub(ctx.worker.len());
        match ctx
            .global
            .steal_batch_with_limit_and_pop(&ctx.worker, to_grab)
        {
            Steal::Success(task) => {
                println!("[{id}] running a task from the GLOBAL queue");
                task();
                continue;
            }
            Steal::Retry => continue, // let's try once more
            Steal::Empty => {}
        };

        // drain local queue first
        if let Some(task) = ctx.worker.pop() {
            println!("[{id}] running a task from its LOCAL queue");
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
    global: Arc<Injector<Task>>,    // the global queue of work to do
    shutdown: Arc<AtomicBool>,      // a global signal that the threadpool is shutting down
    mcv: Arc<(Condvar, Mutex<()>)>, // signal used for sleeping and waking threads in the pool
    workers: Vec<JoinHandle<()>>,
}

#[derive(Default, Clone)]
pub struct ThreadPoolBuilder {
    num_threads: Option<usize>,
    thread_name: Option<String>,
    thread_stack_size: Option<usize>,
}

impl ThreadPoolBuilder {
    pub fn with_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = Some(num_threads);
        self
    }

    pub fn with_thread_name<S: Into<String>>(mut self, name: S) -> Self {
        self.thread_name = Some(name.into());
        self
    }

    pub fn with_stack_size(mut self, stack_size: usize) -> Self {
        self.thread_stack_size = Some(stack_size);
        self
    }

    fn spawn_in_pool(ctx: WorkerContext) -> thread::JoinHandle<()> {
        let mut builder = thread::Builder::new();
        if let Some(ref name) = ctx.name {
            builder = builder.name(name.clone());
        }
        if let Some(ref stack_size) = ctx.stack_size {
            builder = builder.stack_size(*stack_size);
        }
        builder
            .spawn(move || worker_loop(ctx))
            .expect("failed to spawn a fresh thread")
    }

    pub fn build(self) -> ThreadPool {
        let global = Arc::new(Injector::default());

        let num_workers = self.num_threads.unwrap_or_else(num_cpus::get);
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
            println!("Creating Worker {i}...");

            let ctx = WorkerContext {
                global: Arc::clone(&global),
                worker,
                stealers: Arc::clone(&stealers),
                id: i,
                shutdown: Arc::clone(&shutdown),
                mcv: Arc::clone(&mcv),
                stack_size: self.thread_stack_size,
                name: self.thread_name.clone(),
            };

            let handle = Self::spawn_in_pool(ctx);
            worker_threads.push(handle);
        }
        ThreadPool {
            global,
            shutdown,
            mcv,
            workers: worker_threads,
        }
    }
}

impl ThreadPool {
    pub fn builder() -> ThreadPoolBuilder {
        ThreadPoolBuilder::default()
    }

    pub fn spawn(&self, f: impl FnOnce() + Send + 'static) {
        let _guard = self.mcv.1.lock().unwrap();
        self.global.push(Box::new(f));
        // wake up a waiting thead
        self.mcv.0.notify_one();
    }

    pub fn join(self) {
        drop(self)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn test_tasks_execute() {
        let expected = 10;
        let num_threads = 3;
        let stack_size = 1024 * 1024; // 1 MB thread stacks
        let max_wait = 500; // measured in ms

        let random_task = move |counter: Arc<Mutex<usize>>| {
            let rand = rand::random_range(0..=max_wait);
            thread::sleep(Duration::from_millis(rand));
            let mut n = counter.lock().unwrap();
            *n += 1;
        };
        let pool = ThreadPoolBuilder::default()
            .with_threads(num_threads)
            .with_stack_size(stack_size)
            .with_thread_name("test pool")
            .build();

        let counter = Arc::new(Mutex::new(0));
        let start = std::time::Instant::now();
        for _ in 0..expected {
            let counter = Arc::clone(&counter);
            pool.spawn(move || random_task.clone()(counter));
        }

        pool.join(); // waits for all tasks to finish
        let elapsed = start.elapsed();
        println!(
            "Took {elapsed:?} to complete compared to the max sequential time of {} ms",
            expected as u64 * max_wait
        );
        assert_eq!(*counter.lock().unwrap(), expected);
    }
}
