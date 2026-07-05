# dreadpool
Let's build a work-stealing threadpool with maximum effort.

## Overview
This is a learning project to explore how work-stealing threadpools work in Rust. I started originally with a simple threadpool and `flume` channels. Each thread had its own local queue with a central task dispatcher, but nothing prevented work starvation. Inspired by `Go` and `tokio`, I set out to right this wrong. This README is the story of that journey.

## Design
This project depends heavily on the excellent [`crossbeam`](https://docs.rs/crossbeam/latest/crossbeam/) crate. Specifically leveraging [`SegQueue`](https://docs.rs/crossbeam/latest/crossbeam/queue/struct.SegQueue.html) and [`Deque`](https://docs.rs/crossbeam/latest/crossbeam/deque/index.html).

Here's the idea:
1. `threadpool::spawn` serves as a central dispatcher and pushes new `Task`s onto a global queue, which is a `crossbeam::SegQueue`, a lock-free MPMC queue.
2. Worker threads each maintain a local queue of `Task`s. They loop in the following order:
    * Top off the local queue to a predetermined value by pulling off the global queue
    * Pull a `Task` off the local queue and run it.
    * If there's nothing on the local queue, it will steal a batch of `Tasks` from the first available other worker local queue.
    * If there's no work on anyone's queue, the worker checks a global flag to indicate it's time to shutdown.
    * If it's not time to shutdown, the worker thread goes to sleep. It will be notified to wake up by a call to `threadpool::spawn`.

## Todo
* Beef up this README by explaining `WorkerContext`, thread safety concerns, the `crossbeam` datastructures, `shutdown` flag, `Condvar` usage, and the `Task` type.
* Add examples to README
* Add doc comments to project
* A README section about my "lost wakeup" adventure