use crossbeam::deque::{Injector, Steal, Stealer, Worker};
use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle, panicking},
};

/// The unit of work for the `ThreadPool`. `Tasks` are scheduled using `ThreadPool::spawn`
/// and placed in a global queue managed by the `ThreadPool`
type Task = Box<dyn FnOnce() + Send>;

const IDEAL_WORKER_BACKLOG: usize = 10; // completely arbitrary choice

/// Holds the information that worker threads need to operate independently.
struct WorkerContext {
    name: Option<String>,
    global: Arc<Injector<Task>>,
    worker: Worker<Task>,
    stealers: Arc<Vec<Stealer<Task>>>,
    id: usize, // so a worker skips stealing from itself
    shutdown: Arc<AtomicBool>,
    mcv: Arc<(Condvar, Mutex<()>)>,
    stack_size: Option<usize>,
    threads: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

/// In the event of a `drop` due to panic, we will spin up a replacement thread for the pool
impl Drop for WorkerContext {
    fn drop(&mut self) {
        if panicking() {
            println!("[{}] panicked! Spinning up a replacement...", self.id);

            // pull an owned version of `worker` from the &mut, dumping a blank one in its place
            let worker = std::mem::replace(&mut self.worker, Worker::new_fifo());

            let ctx = WorkerContext {
                global: Arc::clone(&self.global),
                worker,
                stealers: Arc::clone(&self.stealers),
                id: self.id,
                shutdown: Arc::clone(&self.shutdown),
                mcv: Arc::clone(&self.mcv),
                name: self.name.clone(),
                stack_size: self.stack_size,
                threads: Arc::clone(&self.threads),
            };
            ThreadPoolBuilder::spawn_in_pool(ctx);
        }
    }
}

/// Worker threads run an infinite loop until being notified by the `ThreadPool` that it's time to shutdown.
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

/// Holds a reference to a global `Task` queue, a flag to indicate shutdown to worker threads,
/// a `CondVar` and associated `Mutex` for thread sleep control, and a protected list
/// of all worker thread `JoinHandles`
pub struct ThreadPool {
    global: Arc<Injector<Task>>,    // the global queue of work to do
    shutdown: Arc<AtomicBool>,      // a global signal that the threadpool is shutting down
    mcv: Arc<(Condvar, Mutex<()>)>, // signal used for sleeping and waking threads in the pool
    workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

/// Used to generate a configured `ThreadPool` by calling `.build()`. Caller can specify
/// number of threads for the pool, a name for logging purposes, and a preferred thread stack size.
#[derive(Default, Clone)]
pub struct ThreadPoolBuilder {
    num_threads: Option<usize>,
    thread_name: Option<String>,
    thread_stack_size: Option<usize>,
}

impl ThreadPoolBuilder {
    /// Used to select the number of threads you want your `ThreadPool` to manage.
    /// Defaults to the number of logical CPUs if not specified.
    ///
    /// # Examples
    /// ```
    /// use dreadpool::ThreadPoolBuilder;
    /// let pool = ThreadPoolBuilder::default().with_threads(4).build();
    /// ```
    pub fn with_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = Some(num_threads);
        self
    }

    /// Used to give your pool a name for logging purposes.
    pub fn with_thread_name<S: Into<String>>(mut self, name: S) -> Self {
        self.thread_name = Some(name.into());
        self
    }

    /// Used to specify desired stack size for your worker threads.
    pub fn with_stack_size(mut self, stack_size: usize) -> Self {
        self.thread_stack_size = Some(stack_size);
        self
    }

    fn spawn_in_pool(ctx: WorkerContext) {
        let thread_list = Arc::clone(&ctx.threads);
        let mut builder = thread::Builder::new();
        if let Some(ref name) = ctx.name {
            builder = builder.name(name.clone());
        }
        if let Some(ref stack_size) = ctx.stack_size {
            builder = builder.stack_size(*stack_size);
        }
        let jh = builder
            .spawn(move || worker_loop(ctx))
            .expect("failed to spawn a fresh thread");
        thread_list.lock().unwrap().push(jh);
    }

    /// Generates a `ThreadPool` with settings specified by the `ThreadPoolBuilder`
    pub fn build(self) -> ThreadPool {
        let global = Arc::new(Injector::default());

        let num_workers = self.num_threads.unwrap_or_else(num_cpus::get);
        // Generate workers list
        let workers: Vec<_> = (0..num_workers)
            .map(|_| Worker::<Task>::new_fifo())
            .collect();
        let stealers: Arc<Vec<_>> = Arc::new(workers.iter().map(|w| w.stealer()).collect());
        // Generate Condvar construct
        let mcv = Arc::new((Condvar::new(), Mutex::new(())));
        // Generate shutdown signal
        let shutdown = Arc::new(AtomicBool::new(false));
        let threads = Arc::new(Mutex::new(Vec::with_capacity(num_workers)));

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
                threads: Arc::clone(&threads),
            };

            Self::spawn_in_pool(ctx);
        }
        ThreadPool {
            global,
            shutdown,
            mcv,
            workers: threads,
        }
    }
}

impl ThreadPool {
    /// Returns a blank `ThreadPoolBuilder`
    ///
    /// # Examples
    /// ```
    /// use dreadpool::ThreadPool;
    /// let pool = ThreadPool::builder()
    ///     .with_threads(4)
    ///     .build();
    ///
    pub fn builder() -> ThreadPoolBuilder {
        ThreadPoolBuilder::default()
    }

    /// Takes a closure, converts it to a `Task` and puts it in the `ThreadPool`'s global queue
    /// for assignment. Calling `spawn` will wake up a single sleeping worker thread if one exists.
    ///
    /// # Examples
    /// ```
    /// use dreadpool::ThreadPool;
    /// use std::sync::{Arc, Mutex};
    ///
    /// let pool = ThreadPool::builder().with_threads(2).build();
    /// let counter = Arc::new(Mutex::new(0));
    /// let c = Arc::clone(&counter);
    /// pool.spawn(move || {
    ///     *c.lock().unwrap() += 1;
    /// });
    /// pool.join();
    /// assert_eq!(*counter.lock().unwrap(), 1);
    /// `
    pub fn spawn(&self, f: impl FnOnce() + Send + 'static) {
        let _guard = self.mcv.1.lock().unwrap();
        self.global.push(Box::new(f));
        // wake up a waiting thead
        self.mcv.0.notify_one();
    }

    /// Blocks until all spawned tasks complete, then shuts down the pool.
    ///
    /// # Examples
    /// ```
    /// use dreadpool::ThreadPool;
    /// use std::sync::{Arc, Mutex};
    ///
    /// let pool = ThreadPool::builder().with_threads(2).build();
    /// let counter = Arc::new(Mutex::new(0));
    /// for _ in 0..10 {
    ///     let c = Arc::clone(&counter);
    ///     pool.spawn(move || *c.lock().unwrap() += 1);
    /// }
    /// pool.join();
    /// assert_eq!(*counter.lock().unwrap(), 10);
    /// ```
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
        for (i, worker) in self.workers.lock().unwrap().drain(..).enumerate() {
            println!("Waiting on Worker {i}...");
            match worker.join() {
                Ok(_) => {}
                Err(e) => println!("[{i}] panicked: {:?}", e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::{self, Duration};

    #[test]
    fn test_tasks_execute() {
        let expected = 100;
        let num_threads = 3;
        let stack_size = 1024 * 1024; // 1 MB thread stacks
        let long_wait = 2000; // measured in ms
        let normal_wait = 50; // measured in ms

        let random_task = move |counter: Arc<Mutex<usize>>| {
            match rand::random_range(0..=4) {
                4 => thread::sleep(Duration::from_millis(long_wait)),
                _ => thread::sleep(Duration::from_millis(normal_wait)),
            };
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
        let short_wait = normal_wait * 3 / 4;
        let long_wait = long_wait * 1 / 4;
        let avg_wait = short_wait + long_wait;
        println!(
            "Took {elapsed:?} to complete compared to the sequential time of {} ms",
            expected as u64 * avg_wait
        );
        assert_eq!(*counter.lock().unwrap(), expected);
    }

    #[test]
    fn test_thread_replacement_on_panic() {
        let pool = ThreadPoolBuilder::default().with_threads(2).build();

        let counter = Arc::new(Mutex::new(0));

        // trigger a panic on one worker
        pool.spawn(|| panic!("intentional panic"));

        // give the replacement thread time to spin up
        thread::sleep(Duration::from_millis(100));

        // verify the pool is still functional
        for _ in 0..10 {
            let counter = Arc::clone(&counter);
            pool.spawn(move || {
                let mut n = counter.lock().unwrap();
                *n += 1;
            });
        }

        pool.join();
        assert_eq!(*counter.lock().unwrap(), 10);
    }

    fn fib(n: usize) -> usize {
        if n == 1 || n == 0 {
            return 1;
        }
        fib(n - 1) + fib(n - 2)
    }

    #[test]
    fn test_fib() {
        // establish a baseline for singlethreaded fib compute time.
        let start = time::Instant::now();
        let answer = fib(42);
        let baseline = start.elapsed();

        let num_threads = num_cpus::get();
        let num_tasks = num_threads * 3;

        let pool = ThreadPoolBuilder::default()
            .with_threads(num_threads)
            .build();

        // hand out fibs to the threadpool
        let thread_start = time::Instant::now();
        for i in 0..num_tasks {
            pool.spawn(move || {
                let n = fib(42);
                assert_eq!(n, answer);
                println!("[{i}]: {n}");
            });
        }
        pool.join();
        let thread_end = thread_start.elapsed();
        println!(
            "Single thread took: {baseline:?}. Threadpool took {thread_end:?} for {num_tasks} runs"
        );
        assert!(thread_end < baseline * num_tasks as u32);
    }
}
