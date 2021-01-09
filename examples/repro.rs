use futures::stream::{FuturesUnordered, StreamExt};
use sled::Config;
use std::sync::{Arc, Barrier};
use std::thread::Builder as ThreadBuilder;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Reproduction for issue #1241, just `cargo run --example repro` and you should see a lockup.
/// After what seems to be a lockup, the process will hang in order to have a debugger attached.
fn main() {
    env_logger::init();
    // derived from a test setup in rust-ipfs where multiple databases are opened separatedly

    let (tx, rx) = std::sync::mpsc::channel();

    // the lockup could be observed without this thread as well, but it's nice to get something to
    // stdout when it actually happens.
    let watcher = ThreadBuilder::new()
        .name(String::from("repro watcher"))
        .spawn(move || {
            for round in 1.. {
                if let Err(_) = rx.recv() {
                    break;
                }

                println!("{}: start", round);
                let last = Instant::now();

                let recv_timeout = Duration::from_secs(1);
                if let Err(_) = rx.recv_timeout(recv_timeout) {
                    println!("{}: lockup", round);
                    return;
                }
                println!("{}: {:?}", round, last.elapsed());
            }
        })
        .unwrap();

    // See branch history for tokio 0.2, tokio 0.3 and async-std 1 repros.
    let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();

    let spawner = ThreadBuilder::new()
        .name(String::from("repro spawner"))
        .spawn(move || {
            for _ in 0.. {
                let n = 7;

                let barrier = Arc::new(Barrier::new(n));

                // each thread creates a new database to a new tempdir, followed by a write and flush_async
                // future
                let round = (0..n).map(|_| {
                    let barrier = barrier.clone();
                    let handle = rt.handle().to_owned();
                    std::thread::spawn(move || {
                        let tempdir = TempDir::new().expect("tempdir creation failed");
                        let p = tempdir.path().to_owned();
                        let db = Config::new().create_new(true).path(p).open().unwrap();
                        db.insert("some_key", "any_value").unwrap();
                        barrier.wait();
                        let fut = async move {
                            Verbose(db.flush_async()).await
                        };

                        // keep the tempdir alive not to remove it on drop (it doesn't seem to matter, but
                        // just to filter it out).
                        (tempdir, handle.spawn(fut))
                    })
                });

                let join_handles = round.collect::<Vec<_>>();

                // just to get some timing going on to justify the recv_timeout of only 1s; in multiple
                // runs you should be able to see that a passing round completes much faster. if it doesn't
                // you might want to increase the recv_timeout.
                tx.send(()).unwrap();

                let mut all_futures = FuturesUnordered::new();
                let mut tempdirs = Vec::new();

                for jh in join_handles {
                    let (tempdir, task_handle) = jh.join().unwrap();
                    all_futures.push(task_handle);
                    tempdirs.push(tempdir);
                }

                rt.block_on(async {
                    while let Some(res) = all_futures.next().await {
                        res.expect("spawned task completed").expect("sync succeeded");
                    }
                });

                tx.send(()).expect("miraculous recovery: try increasing the recv_timeout from 1s");

                tempdirs.into_iter().for_each(|tempdir| assert!(tempdir.path().exists()));
            }
        })
        .unwrap();

    watcher.join().unwrap();
    println!("watcher completed as lockup happened as expected");

    let (tx, rx) = std::sync::mpsc::channel();

    let _waiter = ThreadBuilder::new()
        .name(String::from("repro spawner waiter"))
        .spawn({
            move || {
                tx.send(()).unwrap();
                let res = spawner.join();
                drop(tx);
                res
            }})
        .unwrap();

    // wait for waiter to start
    rx.recv().unwrap();

    // make sure it spawner doesn't miraculously recover (the case for too small recv_timeout)
    rx.recv_timeout(Duration::from_secs(1)).unwrap_err();

    // now ready to have a debugger attached
    println!("---");
    if std::env::var_os("RUST_LOG").is_none() {
        println!("RUST_LOG is unset, try setting it to enable logging");
    }
    #[cfg(feature = "testing")]
    {
        println!("feature \"testing\" detected: note the stacktraces will be different from runs without the feature.");
    }
    println!("process id: {}", std::process::id());
    println!("things should be steadily locked up now, entering sleep");

    rx.recv().unwrap_err();

    unreachable!("spawner thread should had never completed")
}

/// Wrapper to println on calls to [`Future::poll`]
struct Verbose<F>(F);

impl<F: std::future::Future> std::future::Future for Verbose<F> {
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use std::task::Poll::*;
        let pinned = &*self as *const _;
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        match inner.poll(cx) {
            Pending => {
                println!("polled pending: {:p}", pinned);
                return Pending;
            }
            Ready(t) => {
                println!("polled ready: {:p}", pinned);
                return Ready(t);
            }
        }
    }
}
