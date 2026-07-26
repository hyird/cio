use std::future::{pending, poll_fn, Future};
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::{broadcast, mpsc, oneshot, watch, Notify, Semaphore};
use tokio::time::{self, Duration, Instant};

fn emit(name: &str, value: bool) {
    println!("{name}={}", if value { 1 } else { 0 });
}

async fn spawn_deferred_case() -> bool {
    let polled = Arc::new(AtomicBool::new(false));
    let task_polled = Arc::clone(&polled);
    let handle = tokio::spawn(async move {
        task_polled.store(true, Ordering::Release);
    });
    let deferred = !polled.load(Ordering::Acquire);
    let joined = handle.await.is_ok();
    deferred && joined && polled.load(Ordering::Acquire)
}

async fn abort_before_poll_case() -> bool {
    let polled = Arc::new(AtomicBool::new(false));
    let task_polled = Arc::clone(&polled);
    let handle = tokio::spawn(async move {
        task_polled.store(true, Ordering::Release);
    });
    handle.abort();
    let cancelled = handle.await.is_err_and(|error| error.is_cancelled());
    cancelled && !polled.load(Ordering::Acquire)
}

async fn join_drop_detaches_case() -> bool {
    let (sender, receiver) = oneshot::channel();
    let handle = tokio::spawn(async move {
        let _ = sender.send(());
    });
    drop(handle);
    receiver.await.is_ok()
}

async fn panic_join_error_case() -> bool {
    tokio::spawn(async move {
        panic!("Tokio 差分测试的预期 panic");
    })
    .await
    .is_err_and(|error| error.is_panic())
}

struct DropProbe(Arc<AtomicBool>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

async fn abort_destroys_before_join_case() -> bool {
    let destroyed = Arc::new(AtomicBool::new(false));
    let task_destroyed = Arc::clone(&destroyed);
    let (started_sender, started_receiver) = oneshot::channel();

    let handle = tokio::spawn(async move {
        let _probe = DropProbe(task_destroyed);
        let _ = started_sender.send(());
        pending::<()>().await;
    });

    if started_receiver.await.is_err() {
        return false;
    }
    handle.abort();
    let cancelled = handle.await.is_err_and(|error| error.is_cancelled());
    cancelled && destroyed.load(Ordering::Acquire)
}

async fn append(order: Arc<Mutex<Vec<i32>>>, value: i32) {
    order.lock().expect("order mutex poisoned").push(value);
}

async fn nested_future_current_poll_case() -> bool {
    let order = Arc::new(Mutex::new(Vec::new()));
    let spawned_order = Arc::clone(&order);
    let spawned = tokio::spawn(async move {
        append(spawned_order, 3).await;
    });

    order.lock().expect("order mutex poisoned").push(1);
    append(Arc::clone(&order), 2).await;
    let before_spawned_poll = *order.lock().expect("order mutex poisoned") == vec![1, 2];

    let joined = spawned.await.is_ok();
    let final_order = *order.lock().expect("order mutex poisoned") == vec![1, 2, 3];
    before_spawned_poll && joined && final_order
}

async fn paused_sleep_rounding_case() -> bool {
    let start = Instant::now();
    time::sleep(Duration::from_nanos(1)).await;
    Instant::now() - start == Duration::from_millis(1)
}

async fn timeout_immediate_zero_case() -> bool {
    time::timeout(Duration::ZERO, async { 42 })
        .await
        .is_ok_and(|value| value == 42)
}

async fn timeout_same_deadline_case() -> bool {
    time::timeout(Duration::from_secs(5), async {
        time::sleep(Duration::from_secs(5)).await;
        42
    })
    .await
    .is_ok_and(|value| value == 42)
}

async fn timeout_drops_loser_case() -> bool {
    let destroyed = Arc::new(AtomicBool::new(false));
    let task_destroyed = Arc::clone(&destroyed);
    let result = time::timeout(Duration::from_secs(3), async move {
        let _probe = DropProbe(task_destroyed);
        time::sleep(Duration::from_secs(5)).await;
        42
    })
    .await;
    result.is_err() && destroyed.load(Ordering::Acquire)
}

async fn sleep_reset_after_elapsed_case() -> bool {
    let start = Instant::now();
    let timer = time::sleep(Duration::from_secs(2));
    tokio::pin!(timer);
    timer.as_mut().await;
    timer
        .as_mut()
        .reset(Instant::now() + Duration::from_secs(3));
    timer.as_mut().await;
    Instant::now() - start == Duration::from_secs(5)
}

async fn interval_basic_case() -> bool {
    let start = Instant::now();
    let mut timer = time::interval(Duration::from_secs(2));
    let first = timer.tick().await;
    let second = timer.tick().await;
    let third = timer.tick().await;
    first == start
        && second - start == Duration::from_secs(2)
        && third - start == Duration::from_secs(4)
}

async fn interval_missed_ticks_case() -> bool {
    let delay_start = Instant::now();
    let mut delay = time::interval(Duration::from_secs(2));
    delay.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    delay.tick().await;
    time::advance(Duration::from_secs(10)).await;
    let missed = delay.tick().await;
    let next = delay.tick().await;
    let delay_ok = missed - delay_start == Duration::from_secs(2)
        && next - delay_start == Duration::from_secs(12);

    let skip_start = Instant::now();
    let mut skip = time::interval(Duration::from_secs(2));
    skip.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    skip.tick().await;
    time::advance(Duration::from_secs(9)).await;
    skip.tick().await;
    let skip_next = skip.tick().await;

    delay_ok && skip_next - skip_start == Duration::from_secs(10)
}

async fn consume_budget_yields_case() -> bool {
    let progress = Arc::new(std::sync::atomic::AtomicI32::new(0));
    let observed = Arc::new(std::sync::atomic::AtomicI32::new(-1));
    let hog_progress = Arc::clone(&progress);
    let hog = tokio::spawn(async move {
        for step in 0..1_000 {
            hog_progress.store(step + 1, Ordering::Release);
            tokio::task::consume_budget().await;
        }
    });
    let sentinel_progress = Arc::clone(&progress);
    let sentinel_observed = Arc::clone(&observed);
    let sentinel = tokio::spawn(async move {
        sentinel_observed.store(sentinel_progress.load(Ordering::Acquire), Ordering::Release);
    });
    sentinel.await.is_ok() && hog.await.is_ok() && observed.load(Ordering::Acquire) < 1_000
}

async fn blocking_running_abort_noop_case() -> bool {
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let job_started = Arc::clone(&started);
    let job_release = Arc::clone(&release);
    let handle = tokio::task::spawn_blocking(move || {
        job_started.store(true, Ordering::Release);
        while !job_release.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        42
    });
    while !started.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    handle.abort();
    release.store(true, Ordering::Release);
    handle.await.is_ok_and(|value| value == 42)
}

async fn blocking_queued_abort_case() -> bool {
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let first_started = Arc::clone(&started);
    let first_release = Arc::clone(&release);
    let first = tokio::task::spawn_blocking(move || {
        first_started.store(true, Ordering::Release);
        while !first_release.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
    });
    while !started.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    let second_calls = Arc::clone(&calls);
    let second = tokio::task::spawn_blocking(move || {
        second_calls.fetch_add(1, Ordering::Relaxed);
    });
    second.abort();
    release.store(true, Ordering::Release);
    first.await.is_ok()
        && second.await.is_err_and(|error| error.is_cancelled())
        && calls.load(Ordering::Acquire) == 0
}

async fn blocking_paused_inhibits_time_case() -> bool {
    let start = Instant::now();
    let timer_completed = Arc::new(AtomicBool::new(false));
    let timer_flag = Arc::clone(&timer_completed);
    let timer = tokio::spawn(async move {
        time::sleep(Duration::from_secs(5)).await;
        timer_flag.store(true, Ordering::Release);
    });
    let blocking = tokio::task::spawn_blocking(|| {
        std::thread::sleep(std::time::Duration::from_millis(10));
    });
    let blocking_ok = blocking.await.is_ok();
    let stayed_frozen = Instant::now() == start && !timer_completed.load(Ordering::Acquire);
    timer.abort();
    let timer_cancelled = timer.await.is_err_and(|error| error.is_cancelled());
    blocking_ok && stayed_frozen && timer_cancelled
}

async fn notify_permit_coalesces_case() -> bool {
    let notify = tokio::sync::Notify::new();
    notify.notify_one();
    notify.notify_one();
    let first = notify.notified();
    tokio::pin!(first);
    let first_ready = first.as_mut().enable();
    first.await;

    let second = notify.notified();
    tokio::pin!(second);
    let second_waited = !second.as_mut().enable();
    notify.notify_one();
    second.await;
    first_ready && second_waited
}

async fn notify_fifo_lifo_case() -> bool {
    let notify = tokio::sync::Notify::new();
    let mut first = Box::pin(notify.notified());
    let mut second = Box::pin(notify.notified());
    let mut third = Box::pin(notify.notified());
    let registered =
        !first.as_mut().enable() && !second.as_mut().enable() && !third.as_mut().enable();
    notify.notify_one();
    let fifo = first.as_mut().enable() && !second.as_mut().enable() && !third.as_mut().enable();
    notify.notify_last();
    let lifo = third.as_mut().enable() && !second.as_mut().enable();
    notify.notify_one();
    second.await;
    registered && fifo && lifo
}

async fn notify_waiters_snapshot_case() -> bool {
    let notify = tokio::sync::Notify::new();
    notify.notify_one();
    let first = notify.notified();
    let second = notify.notified();
    notify.notify_waiters();
    first.await;
    second.await;
    let permit = notify.notified();
    tokio::pin!(permit);
    let retained = permit.as_mut().enable();
    permit.await;

    let fresh = tokio::sync::Notify::new();
    fresh.notify_waiters();
    let after = fresh.notified();
    tokio::pin!(after);
    let no_stored_broadcast = !after.as_mut().enable();
    fresh.notify_one();
    after.await;
    retained && no_stored_broadcast
}

async fn notify_cancel_transfers_case() -> bool {
    let notify = tokio::sync::Notify::new();
    let mut first = Box::pin(notify.notified());
    let mut second = Box::pin(notify.notified());
    let registered = !first.as_mut().enable() && !second.as_mut().enable();
    notify.notify_one();
    drop(first);
    let transferred = second.as_mut().enable();
    second.await;
    registered && transferred
}

async fn semaphore_order_probe_child(
    semaphore: Arc<Semaphore>,
    permits: u32,
    label: i32,
    order: Arc<Mutex<Vec<i32>>>,
    release: Arc<Notify>,
) {
    let Ok(_permit) = semaphore.acquire_many_owned(permits).await else {
        return;
    };
    order
        .lock()
        .expect("semaphore order mutex poisoned")
        .push(label);
    release.notified().await;
}

async fn semaphore_fifo_head_blocking_case() -> bool {
    let semaphore = Arc::new(Semaphore::new(0));
    let release = Arc::new(Notify::new());
    let order = Arc::new(Mutex::new(Vec::new()));
    let first = tokio::spawn(semaphore_order_probe_child(
        Arc::clone(&semaphore),
        3,
        1,
        Arc::clone(&order),
        Arc::clone(&release),
    ));
    tokio::task::yield_now().await;
    let second = tokio::spawn(semaphore_order_probe_child(
        Arc::clone(&semaphore),
        1,
        2,
        Arc::clone(&order),
        Arc::clone(&release),
    ));
    tokio::task::yield_now().await;

    semaphore.add_permits(2);
    tokio::task::yield_now().await;
    let head_blocked = order
        .lock()
        .expect("semaphore order mutex poisoned")
        .is_empty();
    semaphore.add_permits(1);
    while order.lock().expect("semaphore order mutex poisoned").len() < 1 {
        tokio::task::yield_now().await;
    }
    let fifo_first = *order.lock().expect("semaphore order mutex poisoned") == vec![1];
    semaphore.add_permits(1);
    while order.lock().expect("semaphore order mutex poisoned").len() < 2 {
        tokio::task::yield_now().await;
    }
    let fifo_second = *order.lock().expect("semaphore order mutex poisoned") == vec![1, 2];

    release.notify_waiters();
    head_blocked
        && fifo_first
        && fifo_second
        && first.await.is_ok()
        && second.await.is_ok()
        && semaphore.available_permits() == 4
}

async fn semaphore_hold_probe_child(
    semaphore: Arc<Semaphore>,
    permits: u32,
    acquired: Arc<AtomicBool>,
    release: Arc<Notify>,
) {
    let Ok(_permit) = semaphore.acquire_many_owned(permits).await else {
        return;
    };
    acquired.store(true, Ordering::Release);
    release.notified().await;
}

async fn semaphore_cancel_partial_case() -> bool {
    let semaphore = Arc::new(Semaphore::new(0));
    let release = Arc::new(Notify::new());
    let first_acquired = Arc::new(AtomicBool::new(false));
    let second_acquired = Arc::new(AtomicBool::new(false));
    let first = tokio::spawn(semaphore_hold_probe_child(
        Arc::clone(&semaphore),
        3,
        Arc::clone(&first_acquired),
        Arc::clone(&release),
    ));
    tokio::task::yield_now().await;
    let second = tokio::spawn(semaphore_hold_probe_child(
        Arc::clone(&semaphore),
        1,
        Arc::clone(&second_acquired),
        Arc::clone(&release),
    ));
    tokio::task::yield_now().await;

    semaphore.add_permits(2);
    first.abort();
    let first_cancelled = first.await.is_err_and(|error| error.is_cancelled());
    while !second_acquired.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    let transferred = first_cancelled
        && !first_acquired.load(Ordering::Acquire)
        && semaphore.available_permits() == 1;
    release.notify_waiters();
    transferred && second.await.is_ok() && semaphore.available_permits() == 2
}

async fn semaphore_close_case() -> bool {
    let semaphore = Arc::new(Semaphore::new(1));
    let waiter_semaphore = Arc::clone(&semaphore);
    let waiter = tokio::spawn(async move { waiter_semaphore.acquire_many_owned(2).await.is_err() });
    tokio::task::yield_now().await;
    let partial = semaphore.available_permits() == 0;
    semaphore.close();
    let waiter_closed = waiter.await.is_ok_and(|closed| closed);
    let zero_closed = semaphore
        .clone()
        .try_acquire_many_owned(0)
        .is_err_and(|error| error == tokio::sync::TryAcquireError::Closed);
    semaphore.add_permits(2);
    partial
        && waiter_closed
        && zero_closed
        && semaphore.is_closed()
        && semaphore.available_permits() == 3
}

async fn semaphore_permit_ops_case() -> bool {
    let semaphore = Arc::new(Semaphore::new(5));
    let Ok(mut permit) = semaphore.clone().try_acquire_many_owned(3) else {
        return false;
    };
    let Some(split) = permit.split(1) else {
        return false;
    };
    if permit.num_permits() != 2 || split.num_permits() != 1 {
        return false;
    }
    permit.merge(split);
    let merged = permit.num_permits() == 3 && permit.semaphore().available_permits() == 2;
    permit.forget();
    semaphore.add_permits(3);
    let forgotten = semaphore.forget_permits(9);
    merged && forgotten == 5 && semaphore.available_permits() == 0
}

async fn mutex_order_probe_child(
    mutex: Arc<tokio::sync::Mutex<i32>>,
    label: i32,
    order: Arc<Mutex<Vec<i32>>>,
) {
    let mut guard = mutex.lock_owned().await;
    order
        .lock()
        .expect("mutex order mutex poisoned")
        .push(label);
    *guard += 1;
    tokio::task::yield_now().await;
}

async fn mutex_fifo_case() -> bool {
    let mutex = Arc::new(tokio::sync::Mutex::new(0));
    let owner = mutex.clone().lock_owned().await;
    let order = Arc::new(Mutex::new(Vec::new()));
    let first = tokio::spawn(mutex_order_probe_child(
        Arc::clone(&mutex),
        1,
        Arc::clone(&order),
    ));
    tokio::task::yield_now().await;
    let second = tokio::spawn(mutex_order_probe_child(
        Arc::clone(&mutex),
        2,
        Arc::clone(&order),
    ));
    tokio::task::yield_now().await;
    let third = tokio::spawn(mutex_order_probe_child(
        Arc::clone(&mutex),
        3,
        Arc::clone(&order),
    ));
    tokio::task::yield_now().await;
    let waited = order.lock().expect("mutex order mutex poisoned").is_empty();
    drop(owner);
    let joined = first.await.is_ok() && second.await.is_ok() && third.await.is_ok();
    let ordered = *order.lock().expect("mutex order mutex poisoned") == vec![1, 2, 3];
    let final_value = *mutex.lock().await;
    waited && joined && ordered && final_value == 3
}

async fn mutex_cancel_transfers_case() -> bool {
    let mutex = Arc::new(tokio::sync::Mutex::new(0));
    let owner = mutex.clone().lock_owned().await;
    let order = Arc::new(Mutex::new(Vec::new()));
    let first = tokio::spawn(mutex_order_probe_child(
        Arc::clone(&mutex),
        1,
        Arc::clone(&order),
    ));
    tokio::task::yield_now().await;
    let second = tokio::spawn(mutex_order_probe_child(
        Arc::clone(&mutex),
        2,
        Arc::clone(&order),
    ));
    tokio::task::yield_now().await;
    drop(owner);
    first.abort();
    let first_cancelled = first.await.is_err_and(|error| error.is_cancelled());
    let second_ok = second.await.is_ok();
    let ordered = *order.lock().expect("mutex order mutex poisoned") == vec![2];
    let unlocked = mutex.try_lock().is_ok();
    first_cancelled && second_ok && ordered && unlocked
}

async fn mutex_no_poison_case() -> bool {
    let mutex = Arc::new(tokio::sync::Mutex::new(0));
    let child_mutex = Arc::clone(&mutex);
    let child = tokio::spawn(async move {
        let mut guard = child_mutex.lock_owned().await;
        *guard = 7;
        panic!("expected mutex holder panic");
    });
    let panicked = child.await.is_err_and(|error| error.is_panic());
    let after = mutex.try_lock();
    panicked && after.is_ok_and(|guard| *guard == 7)
}

struct MutexProbeNested {
    value: i32,
}

struct MutexProbeRecord {
    first: i32,
    nested: MutexProbeNested,
}

async fn mutex_owned_map_case() -> bool {
    let mutex = Arc::new(tokio::sync::Mutex::new(MutexProbeRecord {
        first: 1,
        nested: MutexProbeNested { value: 2 },
    }));
    {
        let guard = mutex.clone().lock_owned().await;
        let nested = tokio::sync::OwnedMutexGuard::map(guard, |value| &mut value.nested);
        let mut value =
            tokio::sync::OwnedMappedMutexGuard::map(nested, |nested_value| &mut nested_value.value);
        *value = 9;
    }
    {
        let guard = mutex.clone().lock_owned().await;
        let failed = tokio::sync::OwnedMutexGuard::try_map(guard, |value| {
            if value.first == -1 {
                Some(&mut value.first)
            } else {
                None
            }
        });
        match failed {
            Ok(_) => false,
            Err(mut original) => {
                original.first = 4;
                drop(original);
                let final_guard = mutex.lock().await;
                final_guard.first == 4 && final_guard.nested.value == 9
            }
        }
    }
}

async fn mutex_blocking_bridge_case() -> bool {
    let mutex = Arc::new(tokio::sync::Mutex::new(1));
    let owner = mutex.clone().lock_owned().await;
    let started = Arc::new(AtomicBool::new(false));
    let child_mutex = Arc::clone(&mutex);
    let child_started = Arc::clone(&started);
    let blocking = tokio::task::spawn_blocking(move || {
        child_started.store(true, Ordering::Release);
        let mut guard = child_mutex.blocking_lock_owned();
        *guard = 9;
    });
    while !started.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    let stayed_locked = *owner == 1;
    drop(owner);
    let blocking_ok = blocking.await.is_ok();
    let final_value = *mutex.lock().await;
    stayed_locked && blocking_ok && final_value == 9
}

async fn rwlock_hold_reader_child(
    rwlock: Arc<tokio::sync::RwLock<i32>>,
    acquired: Arc<AtomicBool>,
    release: Arc<Notify>,
) {
    let _guard = rwlock.read_owned().await;
    acquired.store(true, Ordering::Release);
    release.notified().await;
}

async fn rwlock_shared_max_readers_case() -> bool {
    let rwlock = Arc::new(tokio::sync::RwLock::with_max_readers(7, 2));
    let first = rwlock.clone().read_owned().await;
    let second = rwlock.clone().read_owned().await;
    let release = Arc::new(Notify::new());
    let third_acquired = Arc::new(AtomicBool::new(false));
    let third = tokio::spawn(rwlock_hold_reader_child(
        Arc::clone(&rwlock),
        Arc::clone(&third_acquired),
        Arc::clone(&release),
    ));
    tokio::task::yield_now().await;
    let capped = !third_acquired.load(Ordering::Acquire) && rwlock.try_write().is_err();

    drop(first);
    while !third_acquired.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    let shared = *second == 7 && rwlock.try_write().is_err();

    release.notify_waiters();
    let third_ok = third.await.is_ok();
    drop(second);
    let writer = rwlock.try_write();
    capped && shared && third_ok && writer.is_ok_and(|guard| *guard == 7)
}

async fn rwlock_writer_order_child(
    rwlock: Arc<tokio::sync::RwLock<i32>>,
    order: Arc<Mutex<Vec<i32>>>,
    release: Arc<Notify>,
) {
    let mut guard = rwlock.write_owned().await;
    order
        .lock()
        .expect("rwlock writer order mutex poisoned")
        .push(1);
    *guard += 1;
    release.notified().await;
}

async fn rwlock_reader_order_child(
    rwlock: Arc<tokio::sync::RwLock<i32>>,
    order: Arc<Mutex<Vec<i32>>>,
    release: Arc<Notify>,
) {
    let _guard = rwlock.read_owned().await;
    order
        .lock()
        .expect("rwlock reader order mutex poisoned")
        .push(2);
    release.notified().await;
}

async fn rwlock_writer_priority_fifo_case() -> bool {
    let rwlock = Arc::new(tokio::sync::RwLock::with_max_readers(0, 3));
    let owner = rwlock.clone().read_owned().await;
    let order = Arc::new(Mutex::new(Vec::new()));
    let release_writer = Arc::new(Notify::new());
    let release_reader = Arc::new(Notify::new());

    let writer = tokio::spawn(rwlock_writer_order_child(
        Arc::clone(&rwlock),
        Arc::clone(&order),
        Arc::clone(&release_writer),
    ));
    tokio::task::yield_now().await;
    let reader = tokio::spawn(rwlock_reader_order_child(
        Arc::clone(&rwlock),
        Arc::clone(&order),
        Arc::clone(&release_reader),
    ));
    tokio::task::yield_now().await;
    let both_waited = order
        .lock()
        .expect("rwlock order mutex poisoned")
        .is_empty();

    drop(owner);
    while order
        .lock()
        .expect("rwlock order mutex poisoned")
        .is_empty()
    {
        tokio::task::yield_now().await;
    }
    let writer_first = *order.lock().expect("rwlock order mutex poisoned") == vec![1];
    release_writer.notify_waiters();
    while order.lock().expect("rwlock order mutex poisoned").len() < 2 {
        tokio::task::yield_now().await;
    }
    let reader_second = *order.lock().expect("rwlock order mutex poisoned") == vec![1, 2];
    release_reader.notify_waiters();

    let writer_ok = writer.await.is_ok();
    let reader_ok = reader.await.is_ok();
    let final_value = *rwlock.read().await;
    both_waited && writer_first && reader_second && writer_ok && reader_ok && final_value == 1
}

async fn rwlock_cancel_writer_child(
    rwlock: Arc<tokio::sync::RwLock<i32>>,
    acquired: Arc<AtomicBool>,
) {
    let mut guard = rwlock.write_owned().await;
    acquired.store(true, Ordering::Release);
    *guard += 1;
}

async fn rwlock_cancel_partial_writer_case() -> bool {
    let rwlock = Arc::new(tokio::sync::RwLock::with_max_readers(0, 3));
    let owner = rwlock.clone().read_owned().await;
    let writer_acquired = Arc::new(AtomicBool::new(false));
    let writer = tokio::spawn(rwlock_cancel_writer_child(
        Arc::clone(&rwlock),
        Arc::clone(&writer_acquired),
    ));
    tokio::task::yield_now().await;

    let release_reader = Arc::new(Notify::new());
    let reader_acquired = Arc::new(AtomicBool::new(false));
    let reader = tokio::spawn(rwlock_hold_reader_child(
        Arc::clone(&rwlock),
        Arc::clone(&reader_acquired),
        Arc::clone(&release_reader),
    ));
    tokio::task::yield_now().await;
    let partial_blocked =
        !writer_acquired.load(Ordering::Acquire) && !reader_acquired.load(Ordering::Acquire);

    writer.abort();
    let writer_cancelled = writer.await.is_err_and(|error| error.is_cancelled());
    while !reader_acquired.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    let transferred = writer_cancelled && !writer_acquired.load(Ordering::Acquire) && *owner == 0;

    release_reader.notify_waiters();
    let reader_ok = reader.await.is_ok();
    drop(owner);
    let final_guard = rwlock.try_write();
    partial_blocked && transferred && reader_ok && final_guard.is_ok_and(|guard| *guard == 0)
}

async fn rwlock_no_poison_case() -> bool {
    let rwlock = Arc::new(tokio::sync::RwLock::new(0));
    let child_rwlock = Arc::clone(&rwlock);
    let child = tokio::spawn(async move {
        let mut guard = child_rwlock.write_owned().await;
        *guard = 7;
        panic!("expected rwlock holder panic");
    });
    let panicked = child.await.is_err_and(|error| error.is_panic());
    let readable = rwlock.try_read().is_ok_and(|guard| *guard == 7);
    let writable = rwlock.try_write().is_ok_and(|guard| *guard == 7);
    panicked && readable && writable
}

struct RwLockProbeNested {
    value: i32,
}

struct RwLockProbeRecord {
    nested: RwLockProbeNested,
}

async fn rwlock_owned_mapping_case() -> bool {
    let rwlock = Arc::new(tokio::sync::RwLock::new(RwLockProbeRecord {
        nested: RwLockProbeNested { value: 2 },
    }));
    {
        let guard = rwlock.clone().write_owned().await;
        let nested = tokio::sync::OwnedRwLockWriteGuard::map(guard, |value| &mut value.nested);
        let mut value = tokio::sync::OwnedRwLockMappedWriteGuard::map(nested, |nested_value| {
            &mut nested_value.value
        });
        *value = 9;
    }
    let read = rwlock.clone().read_owned().await;
    let nested = tokio::sync::OwnedRwLockReadGuard::map(read, |value| &value.nested);
    let value = tokio::sync::OwnedRwLockReadGuard::map(nested, |nested_value| &nested_value.value);
    *value == 9
}

async fn rwlock_atomic_downgrade_case() -> bool {
    let rwlock = Arc::new(tokio::sync::RwLock::new(1));
    let write = rwlock.clone().write_owned().await;
    let writer_acquired = Arc::new(AtomicBool::new(false));
    let child_rwlock = Arc::clone(&rwlock);
    let child_acquired = Arc::clone(&writer_acquired);
    let writer = tokio::spawn(async move {
        let mut guard = child_rwlock.write_owned().await;
        child_acquired.store(true, Ordering::Release);
        *guard = 2;
    });
    tokio::task::yield_now().await;

    let read = write.downgrade();
    tokio::task::yield_now().await;
    let stayed_read_locked = *read == 1 && !writer_acquired.load(Ordering::Acquire);
    drop(read);

    let writer_ok = writer.await.is_ok();
    let final_value = *rwlock.read().await;
    stayed_read_locked && writer_ok && writer_acquired.load(Ordering::Acquire) && final_value == 2
}

async fn barrier_wait_probe_child(
    barrier: Arc<tokio::sync::Barrier>,
    completed: Arc<std::sync::atomic::AtomicUsize>,
    leaders: Arc<std::sync::atomic::AtomicUsize>,
) {
    let result = barrier.wait().await;
    if result.is_leader() {
        leaders.fetch_add(1, Ordering::Relaxed);
    }
    completed.fetch_add(1, Ordering::Release);
}

async fn barrier_zero_single_leader_case() -> bool {
    let zero = tokio::sync::Barrier::new(0);
    let single = tokio::sync::Barrier::new(1);
    zero.wait().await.is_leader() && single.wait().await.is_leader()
}

async fn barrier_lazy_unpolled_case() -> bool {
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let unpolled = barrier.wait();
    let completed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let leaders = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let first = tokio::spawn(barrier_wait_probe_child(
        Arc::clone(&barrier),
        Arc::clone(&completed),
        Arc::clone(&leaders),
    ));
    tokio::task::yield_now().await;
    let unpolled_not_counted = completed.load(Ordering::Acquire) == 0;

    let second = tokio::spawn(barrier_wait_probe_child(
        Arc::clone(&barrier),
        Arc::clone(&completed),
        Arc::clone(&leaders),
    ));
    let joined = first.await.is_ok() && second.await.is_ok();
    drop(unpolled);
    unpolled_not_counted
        && joined
        && completed.load(Ordering::Acquire) == 2
        && leaders.load(Ordering::Acquire) == 1
}

async fn barrier_reusable_unique_leader_case() -> bool {
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    for _generation in 0..4 {
        let completed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let leaders = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut waiters = Vec::new();
        for _participant in 0..3 {
            waiters.push(tokio::spawn(barrier_wait_probe_child(
                Arc::clone(&barrier),
                Arc::clone(&completed),
                Arc::clone(&leaders),
            )));
        }
        for waiter in waiters {
            if waiter.await.is_err() {
                return false;
            }
        }
        if completed.load(Ordering::Acquire) != 3 || leaders.load(Ordering::Acquire) != 1 {
            return false;
        }
    }
    true
}

async fn barrier_cancelled_arrival_retained_case() -> bool {
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let started = Arc::new(AtomicBool::new(false));
    let child_barrier = Arc::clone(&barrier);
    let child_started = Arc::clone(&started);
    let cancelled = tokio::spawn(async move {
        child_started.store(true, Ordering::Release);
        child_barrier.wait().await
    });
    while !started.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    cancelled.abort();
    let cancelled_result = cancelled.await;

    let completed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let leaders = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let second = tokio::spawn(barrier_wait_probe_child(
        Arc::clone(&barrier),
        Arc::clone(&completed),
        Arc::clone(&leaders),
    ));
    let third = tokio::spawn(barrier_wait_probe_child(
        Arc::clone(&barrier),
        Arc::clone(&completed),
        Arc::clone(&leaders),
    ));
    let joined = second.await.is_ok() && third.await.is_ok();

    cancelled_result.is_err_and(|error| error.is_cancelled())
        && joined
        && completed.load(Ordering::Acquire) == 2
        && leaders.load(Ordering::Acquire) == 1
}

async fn once_cell_single_initializer_case() -> bool {
    let cell = Arc::new(tokio::sync::OnceCell::<i32>::new());
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut handles = Vec::new();
    for _ in 0..16 {
        let child_cell = Arc::clone(&cell);
        let child_calls = Arc::clone(&calls);
        handles.push(tokio::spawn(async move {
            *child_cell
                .get_or_init(|| async move {
                    child_calls.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                    7
                })
                .await
                == 7
        }));
    }
    for handle in handles {
        if !matches!(handle.await, Ok(true)) {
            return false;
        }
    }
    calls.load(Ordering::Relaxed) == 1
}

async fn once_cell_cancel_retry_case() -> bool {
    let cell = Arc::new(tokio::sync::OnceCell::<i32>::new());
    let entered = Arc::new(AtomicBool::new(false));
    let task_cell = Arc::clone(&cell);
    let task_entered = Arc::clone(&entered);
    let owner = tokio::spawn(async move {
        task_cell
            .get_or_init(|| async move {
                task_entered.store(true, Ordering::Release);
                pending::<()>().await;
                1
            })
            .await;
    });
    while !entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }

    let initializing_error = matches!(
        cell.set(9),
        Err(tokio::sync::SetError::InitializingError(9))
    );
    owner.abort();
    let cancelled = owner.await.is_err_and(|error| error.is_cancelled());
    let retried = *cell.get_or_init(|| async { 2 }).await == 2;
    initializing_error && cancelled && retried
}

async fn once_cell_try_error_retry_case() -> bool {
    let cell = tokio::sync::OnceCell::<i32>::new();
    let failed = cell
        .get_or_try_init(|| async {
            tokio::task::yield_now().await;
            Err::<i32, i32>(5)
        })
        .await;
    if failed != Err(5) {
        return false;
    }
    cell.get_or_try_init(|| async {
        tokio::task::yield_now().await;
        Ok::<i32, i32>(8)
    })
    .await
        == Ok(&8)
}

async fn once_cell_clone_independent_case() -> bool {
    let mut cell = tokio::sync::OnceCell::<i32>::new();
    if cell.set(1).is_err() {
        return false;
    }
    let cloned = cell.clone();
    if cell.take() != Some(1) || cell.set(2).is_err() {
        return false;
    }
    cell.get() == Some(&2) && cloned.get() == Some(&1)
}

async fn once_cell_debug_format_case() -> bool {
    let cell = tokio::sync::OnceCell::<i32>::new();
    let empty = format!("{cell:?}") == "OnceCell { value: None }";
    if cell.set(7).is_err() {
        return false;
    }
    empty && format!("{cell:?}") == "OnceCell { value: Some(7) }"
}

async fn once_cell_set_error_format_case() -> bool {
    let initialized = tokio::sync::OnceCell::<i32>::new();
    if initialized.set(1).is_err() {
        return false;
    }
    let Err(already) = initialized.set(5) else {
        return false;
    };
    let already_value = matches!(&already, tokio::sync::SetError::AlreadyInitializedError(5));
    let already_ok = already_value
        && format!("{already:?}") == "AlreadyInitializedError(5)"
        && format!("{already}") == "AlreadyInitializedError";

    let initializing = Arc::new(tokio::sync::OnceCell::<i32>::new());
    let entered = Arc::new(AtomicBool::new(false));
    let child_cell = Arc::clone(&initializing);
    let child_entered = Arc::clone(&entered);
    let owner = tokio::spawn(async move {
        child_cell
            .get_or_init(|| async move {
                child_entered.store(true, Ordering::Release);
                pending::<()>().await;
                1
            })
            .await;
    });
    while !entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    let Err(during) = initializing.set(9) else {
        owner.abort();
        let _ = owner.await;
        return false;
    };
    let initializing_value = matches!(&during, tokio::sync::SetError::InitializingError(9));
    let initializing_ok = initializing_value
        && format!("{during:?}") == "InitializingError(9)"
        && format!("{during}") == "InitializingError";
    owner.abort();
    let cancelled = owner.await.is_err_and(|error| error.is_cancelled());
    already_ok && initializing_ok && cancelled
}

async fn set_once_wait_unblocks_case() -> bool {
    let set_once = Arc::new(tokio::sync::SetOnce::<i32>::new());
    let entered = Arc::new(AtomicBool::new(false));
    let child_set_once = Arc::clone(&set_once);
    let child_entered = Arc::clone(&entered);
    let waiter = tokio::spawn(async move {
        child_entered.store(true, Ordering::Release);
        *child_set_once.wait().await
    });
    while !entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    let set = set_once.set(11);
    set.is_ok() && matches!(waiter.await, Ok(11))
}

async fn set_once_single_winner_values_case() -> bool {
    let set_once = Arc::new(tokio::sync::SetOnce::<i32>::new());
    let mut contenders = Vec::new();
    for value in 1..=16 {
        let child_set_once = Arc::clone(&set_once);
        contenders.push(tokio::spawn(async move {
            tokio::task::yield_now().await;
            match child_set_once.set(value) {
                Ok(()) => -value,
                Err(error) => error.0,
            }
        }));
    }

    let mut winners = 0;
    let mut observed_sum = 0;
    for contender in contenders {
        let Ok(value) = contender.await else {
            return false;
        };
        if value < 0 {
            winners += 1;
            observed_sum += -value;
        } else {
            observed_sum += value;
        }
    }
    let winner = set_once.get().copied();
    winners == 1 && observed_sum == 136 && winner.is_some_and(|value| (1..=16).contains(&value))
}

async fn set_once_cancel_safe_case() -> bool {
    let set_once = Arc::new(tokio::sync::SetOnce::<i32>::new());
    let entered = Arc::new(AtomicBool::new(false));
    let child_set_once = Arc::clone(&set_once);
    let child_entered = Arc::clone(&entered);
    let cancelled = tokio::spawn(async move {
        child_entered.store(true, Ordering::Release);
        child_set_once.wait().await;
    });
    while !entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    cancelled.abort();
    let cancelled_result = cancelled.await;
    let set = set_once.set(13);
    let retry = *set_once.wait().await;
    cancelled_result.is_err_and(|error| error.is_cancelled()) && set.is_ok() && retry == 13
}

async fn set_once_clone_independent_case() -> bool {
    let original = tokio::sync::SetOnce::<i32>::new();
    let cloned = original.clone();
    let first = original.set(17);
    let second = cloned.set(23);
    first.is_ok() && second.is_ok() && original.get() == Some(&17) && cloned.get() == Some(&23)
}

async fn oneshot_send_receive_case() -> bool {
    let (sender, receiver) = oneshot::channel();
    sender.send(11).is_ok() && receiver.await == Ok(11)
}

async fn oneshot_sender_drop_recv_error_case() -> bool {
    let (sender, receiver) = oneshot::channel::<i32>();
    drop(sender);
    let Err(error) = receiver.await else {
        return false;
    };
    format!("{error}") == "channel closed" && format!("{error:?}") == "RecvError(())"
}

async fn oneshot_receiver_drop_returns_value_case() -> bool {
    let (sender, receiver) = oneshot::channel();
    drop(receiver);
    sender.send(17) == Err(17)
}

async fn oneshot_close_preserves_sent_case() -> bool {
    let (sender, mut receiver) = oneshot::channel();
    let sent = sender.send(23);
    receiver.close();
    let received = receiver.try_recv();
    sent.is_ok() && received == Ok(23) && receiver.is_empty() && receiver.is_terminated()
}

async fn oneshot_close_rejects_late_send_case() -> bool {
    let (sender, mut receiver) = oneshot::channel();
    receiver.close();
    let sent = sender.send(29);
    let received = receiver.await;
    sent == Err(29)
        && received
            .as_ref()
            .is_err_and(|error| format!("{error}") == "channel closed")
}

async fn oneshot_try_recv_empty_closed_case() -> bool {
    let (sender, mut receiver) = oneshot::channel::<i32>();
    let empty = receiver.try_recv();
    drop(sender);
    let closed = receiver.try_recv();
    matches!(empty, Err(oneshot::error::TryRecvError::Empty))
        && matches!(closed, Err(oneshot::error::TryRecvError::Closed))
        && empty.as_ref().is_err_and(|error| {
            format!("{error}") == "channel empty" && format!("{error:?}") == "Empty"
        })
        && closed.as_ref().is_err_and(|error| {
            format!("{error}") == "channel closed" && format!("{error:?}") == "Closed"
        })
}

async fn oneshot_receive_cancel_safe_case() -> bool {
    let (sender, mut receiver) = oneshot::channel();
    let polled = Arc::new(AtomicBool::new(false));
    let wait = poll_fn(|context| {
        polled.store(true, Ordering::Release);
        Future::poll(Pin::new(&mut receiver), context)
    });
    tokio::select! {
        biased;
        _ = async {
            while !polled.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        } => {}
        _ = wait => return false,
    }
    sender.send(31).is_ok() && receiver.await == Ok(31)
}

async fn oneshot_sender_closed_wakes_case() -> bool {
    let (mut sender, mut receiver) = oneshot::channel::<i32>();
    let wait_closed = async {
        sender.closed().await;
        sender.is_closed()
    };
    let close_receiver = async {
        tokio::task::yield_now().await;
        receiver.close();
    };
    let (closed, ()) = tokio::join!(wait_closed, close_receiver);
    closed
}

async fn oneshot_empty_terminated_transitions_case() -> bool {
    let (sender, mut receiver) = oneshot::channel();
    let initial = receiver.is_empty() && !receiver.is_terminated();
    let sent = sender.send(37);
    let published = !receiver.is_empty() && !receiver.is_terminated();
    let received = receiver.try_recv();
    let consumed = receiver.is_empty() && receiver.is_terminated();
    initial && sent.is_ok() && published && received == Ok(37) && consumed
}

struct OneshotDropProbe {
    drops: Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for OneshotDropProbe {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

async fn oneshot_value_drop_once_case() -> bool {
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    {
        let (sender, receiver) = oneshot::channel();
        if sender
            .send(OneshotDropProbe {
                drops: Arc::clone(&drops),
            })
            .is_err()
        {
            return false;
        }
        drop(receiver);
    }
    drops.load(Ordering::Relaxed) == 1
}

async fn oneshot_ready_budget_yields_case() -> bool {
    const BOUND: usize = 512;

    let receive_peer_ran = Arc::new(AtomicBool::new(false));
    let child_receive_peer_ran = Arc::clone(&receive_peer_ran);
    let receive_peer = tokio::spawn(async move {
        child_receive_peer_ran.store(true, Ordering::Release);
    });
    let mut receive_peer_iteration = 0;
    let mut checksum = 0_usize;
    for iteration in 1..=BOUND {
        let (sender, receiver) = oneshot::channel();
        let sent = sender.send(iteration);
        let received = receiver.await;
        if sent.is_err() || received != Ok(iteration) {
            return false;
        }
        checksum += received.expect("已验证 oneshot receive 成功");
        if receive_peer_iteration == 0 && receive_peer_ran.load(Ordering::Acquire) {
            receive_peer_iteration = iteration;
        }
    }
    let receive_peer_joined = receive_peer.await.is_ok();

    let closed_peer_ran = Arc::new(AtomicBool::new(false));
    let child_closed_peer_ran = Arc::clone(&closed_peer_ran);
    let closed_peer = tokio::spawn(async move {
        child_closed_peer_ran.store(true, Ordering::Release);
    });
    let mut closed_peer_iteration = 0;
    for iteration in 1..=BOUND {
        let (mut sender, mut receiver) = oneshot::channel::<usize>();
        receiver.close();
        sender.closed().await;
        if !sender.is_closed() {
            return false;
        }
        if closed_peer_iteration == 0 && closed_peer_ran.load(Ordering::Acquire) {
            closed_peer_iteration = iteration;
        }
    }
    let closed_peer_joined = closed_peer.await.is_ok();

    receive_peer_joined
        && closed_peer_joined
        && (1..=BOUND).contains(&receive_peer_iteration)
        && (1..=BOUND).contains(&closed_peer_iteration)
        && checksum == BOUND * (BOUND + 1) / 2
}

async fn mpsc_marked_send(
    sender: mpsc::Sender<i32>,
    value: i32,
    entered: Arc<AtomicBool>,
    completed: Option<Arc<AtomicBool>>,
) -> Result<(), mpsc::error::SendError<i32>> {
    entered.store(true, Ordering::Release);
    let result = sender.send(value).await;
    if let Some(completed) = completed {
        completed.store(true, Ordering::Release);
    }
    result
}

async fn mpsc_marked_reserve_send(
    sender: mpsc::Sender<i32>,
    value: i32,
    entered: Arc<AtomicBool>,
) -> bool {
    entered.store(true, Ordering::Release);
    let Ok(permit) = sender.reserve().await else {
        return false;
    };
    permit.send(value);
    true
}

async fn mpsc_fifo_backpressure_case() -> bool {
    let (sender, mut receiver) = mpsc::channel(2);
    if sender.try_send(1).is_err() || sender.try_send(2).is_err() {
        return false;
    }
    let full = sender.try_send(3);
    if !matches!(&full, Err(mpsc::error::TrySendError::Full(3))) {
        return false;
    }

    let entered = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicBool::new(false));
    let pending = tokio::spawn(mpsc_marked_send(
        sender.clone(),
        3,
        Arc::clone(&entered),
        Some(Arc::clone(&completed)),
    ));
    while !entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    let observed_backpressure = !completed.load(Ordering::Acquire);

    let first = receiver.recv().await;
    let joined = pending.await;
    let second = receiver.recv().await;
    let third = receiver.recv().await;
    observed_backpressure
        && first == Some(1)
        && second == Some(2)
        && third == Some(3)
        && joined.is_ok_and(|result| result.is_ok())
}

async fn mpsc_send_reserve_fairness_case() -> bool {
    let (sender, mut receiver) = mpsc::channel(1);
    if sender.try_send(0).is_err() {
        return false;
    }

    let first_entered = Arc::new(AtomicBool::new(false));
    let reserve_entered = Arc::new(AtomicBool::new(false));
    let third_entered = Arc::new(AtomicBool::new(false));

    let first = tokio::spawn(mpsc_marked_send(
        sender.clone(),
        1,
        Arc::clone(&first_entered),
        None,
    ));
    while !first_entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;

    let reserved = tokio::spawn(mpsc_marked_reserve_send(
        sender.clone(),
        2,
        Arc::clone(&reserve_entered),
    ));
    while !reserve_entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;

    let third = tokio::spawn(mpsc_marked_send(
        sender.clone(),
        3,
        Arc::clone(&third_entered),
        None,
    ));
    while !third_entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;

    let zero = receiver.recv().await;
    let first_joined = first.await;
    let one = receiver.recv().await;
    let reserve_joined = reserved.await;
    let two = receiver.recv().await;
    let third_joined = third.await;
    let three = receiver.recv().await;

    zero == Some(0)
        && one == Some(1)
        && two == Some(2)
        && three == Some(3)
        && first_joined.is_ok_and(|result| result.is_ok())
        && reserve_joined.is_ok_and(|value| value)
        && third_joined.is_ok_and(|result| result.is_ok())
}

async fn mpsc_cancel_send_case() -> bool {
    let (sender, mut receiver) = mpsc::channel(1);
    if sender.try_send(1).is_err() {
        return false;
    }

    let entered = Arc::new(AtomicBool::new(false));
    let pending = tokio::spawn(mpsc_marked_send(
        sender.clone(),
        2,
        Arc::clone(&entered),
        None,
    ));
    while !entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    pending.abort();
    let cancelled = pending.await;

    let first = receiver.recv().await;
    let retry = sender.try_send(3);
    let third = receiver.recv().await;
    cancelled.is_err_and(|error| error.is_cancelled())
        && first == Some(1)
        && retry.is_ok()
        && third == Some(3)
}

async fn mpsc_cancel_reserve_case() -> bool {
    let (sender, mut receiver) = mpsc::channel(1);
    if sender.try_send(1).is_err() {
        return false;
    }

    let entered = Arc::new(AtomicBool::new(false));
    let child_sender = sender.clone();
    let child_entered = Arc::clone(&entered);
    let pending = tokio::spawn(async move {
        child_entered.store(true, Ordering::Release);
        let _permit = child_sender.reserve().await;
    });
    while !entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    pending.abort();
    let cancelled = pending.await;

    let first = receiver.recv().await;
    let retry = sender.try_send(3);
    let third = receiver.recv().await;
    cancelled.is_err_and(|error| error.is_cancelled())
        && first == Some(1)
        && retry.is_ok()
        && third == Some(3)
}

async fn mpsc_permit_capacity_case() -> bool {
    let (sender, mut receiver) = mpsc::channel(2);
    let initial = sender.capacity() == 2 && sender.max_capacity() == 2;

    let Ok(permit) = sender.reserve().await else {
        return false;
    };
    let held_capacity = sender.capacity() == 1;
    drop(permit);
    let restored = sender.capacity() == 2;

    let Ok(permit) = sender.reserve().await else {
        return false;
    };
    permit.send(5);
    let sent_capacity = sender.capacity() == 1;
    let received = receiver.recv().await;
    initial
        && held_capacity
        && restored
        && sent_capacity
        && received == Some(5)
        && sender.capacity() == 2
}

async fn mpsc_close_drain_permit_case() -> bool {
    let (sender, mut receiver) = mpsc::channel(2);
    if sender.try_send(1).is_err() {
        return false;
    }
    let Ok(permit) = sender.reserve().await else {
        return false;
    };
    if sender.capacity() != 0 {
        return false;
    }

    receiver.close();
    let rejected = sender.try_send(3);
    if !matches!(&rejected, Err(mpsc::error::TrySendError::Closed(3))) {
        return false;
    }
    permit.send(2);

    let first = receiver.recv().await;
    let second = receiver.recv().await;
    let finished = receiver.recv().await;
    sender.is_closed() && first == Some(1) && second == Some(2) && finished.is_none()
}

async fn mpsc_try_errors_case() -> bool {
    let (sender, mut receiver) = mpsc::channel(1);
    let empty = receiver.try_recv();
    let first = sender.try_send(1);
    let full = sender.try_send(2);
    receiver.close();
    let closed = sender.try_send(3);
    let received = receiver.recv().await;
    let finished = receiver.recv().await;
    let disconnected = receiver.try_recv();

    matches!(empty, Err(mpsc::error::TryRecvError::Empty))
        && first.is_ok()
        && matches!(full, Err(mpsc::error::TrySendError::Full(2)))
        && matches!(closed, Err(mpsc::error::TrySendError::Closed(3)))
        && received == Some(1)
        && finished.is_none()
        && matches!(disconnected, Err(mpsc::error::TryRecvError::Disconnected))
}

async fn mpsc_receiver_drop_case() -> bool {
    let (sender, receiver) = mpsc::channel(1);
    if sender.try_send(7).is_err() {
        return false;
    }
    let entered = Arc::new(AtomicBool::new(false));
    let pending = tokio::spawn(mpsc_marked_send(
        sender.clone(),
        8,
        Arc::clone(&entered),
        None,
    ));
    while !entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;

    drop(receiver);
    let joined = pending.await;
    matches!(joined, Ok(Err(error)) if error.0 == 8) && sender.is_closed()
}

async fn mpsc_last_sender_weak_case() -> bool {
    let (sender, mut receiver) = mpsc::channel::<i32>(1);
    let weak = sender.downgrade();
    let clone = sender.clone();
    drop(clone);
    drop(sender);

    let cannot_revive =
        weak.upgrade().is_none() && weak.strong_count() == 0 && weak.weak_count() == 1;
    let finished = receiver.recv().await;
    cannot_revive && finished.is_none()
}

async fn mpsc_sender_counts_case() -> bool {
    let (sender, receiver) = mpsc::channel::<i32>(1);
    let initial = sender.strong_count() == 1
        && sender.weak_count() == 0
        && receiver.sender_strong_count() == 1
        && receiver.sender_weak_count() == 0;

    let expanded;
    let after_upgrade;
    {
        let clone = sender.clone();
        let weak = sender.downgrade();
        let weak_clone = weak.clone();
        let upgraded = weak.upgrade();
        expanded = upgraded.is_some()
            && sender.strong_count() == 3
            && sender.weak_count() == 2
            && receiver.sender_strong_count() == 3
            && receiver.sender_weak_count() == 2;
        drop(upgraded);
        after_upgrade =
            sender.strong_count() == 2 && weak.strong_count() == 2 && weak.weak_count() == 2;
        drop(weak_clone);
        drop(weak);
        drop(clone);
    }

    initial
        && expanded
        && after_upgrade
        && sender.strong_count() == 1
        && sender.weak_count() == 0
        && receiver.sender_strong_count() == 1
        && receiver.sender_weak_count() == 0
}

async fn mpsc_error_format_case() -> bool {
    let (send_sender, send_receiver) = mpsc::channel(1);
    drop(send_receiver);
    let send_error = send_sender.send(41).await.expect_err("Receiver 已析构");

    let (reserve_sender, mut reserve_receiver) = mpsc::channel::<i32>(1);
    reserve_receiver.close();
    let reserve_error = reserve_sender.reserve().await.expect_err("Receiver 已关闭");

    let (try_sender, mut try_receiver) = mpsc::channel(1);
    if try_sender.try_send(1).is_err() {
        return false;
    }
    let full = try_sender.try_send(2).expect_err("通道已满");
    try_receiver.close();
    let closed = try_sender.try_send(3).expect_err("Receiver 已关闭");

    let (recv_sender, mut recv_receiver) = mpsc::channel::<i32>(1);
    let empty = recv_receiver.try_recv().expect_err("通道为空");
    drop(recv_sender);
    let disconnected = recv_receiver.try_recv().expect_err("通道已断开");

    send_error.0 == 41
        && format!("{send_error:?}") == "SendError { .. }"
        && format!("{send_error}") == "channel closed"
        && format!("{reserve_error:?}") == "SendError { .. }"
        && format!("{reserve_error}") == "channel closed"
        && format!("{full:?}") == "\"Full(..)\""
        && format!("{full}") == "no available capacity"
        && format!("{closed:?}") == "\"Closed(..)\""
        && format!("{closed}") == "channel closed"
        && format!("{empty:?}") == "Empty"
        && format!("{empty}") == "receiving on an empty channel"
        && format!("{disconnected:?}") == "Disconnected"
        && format!("{disconnected}") == "receiving on a closed channel"
}

async fn mpsc_marked_closed(
    sender: mpsc::Sender<i32>,
    entered: Arc<AtomicBool>,
    completed: Option<Arc<AtomicBool>>,
) -> bool {
    entered.store(true, Ordering::Release);
    sender.closed().await;
    if let Some(completed) = completed {
        completed.store(true, Ordering::Release);
    }
    sender.is_closed()
}

async fn mpsc_marked_reserve_owned(sender: mpsc::Sender<i32>, entered: Arc<AtomicBool>) -> bool {
    entered.store(true, Ordering::Release);
    match sender.reserve_owned().await {
        Ok(permit) => {
            drop(permit);
            true
        }
        Err(_) => false,
    }
}

async fn mpsc_closed_wakes_case() -> bool {
    let (sender, mut receiver) = mpsc::channel(1);
    let first_entered = Arc::new(AtomicBool::new(false));
    let second_entered = Arc::new(AtomicBool::new(false));
    let first_completed = Arc::new(AtomicBool::new(false));
    let second_completed = Arc::new(AtomicBool::new(false));
    let first = tokio::spawn(mpsc_marked_closed(
        sender.clone(),
        Arc::clone(&first_entered),
        Some(Arc::clone(&first_completed)),
    ));
    let second = tokio::spawn(mpsc_marked_closed(
        sender.clone(),
        Arc::clone(&second_entered),
        Some(Arc::clone(&second_completed)),
    ));
    while !first_entered.load(Ordering::Acquire) || !second_entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    let pending = !first_completed.load(Ordering::Acquire)
        && !second_completed.load(Ordering::Acquire)
        && !sender.is_closed();
    receiver.close();
    let first_joined = first.await;
    let second_joined = second.await;
    sender.closed().await;
    pending
        && first_joined.is_ok_and(|value| value)
        && second_joined.is_ok_and(|value| value)
        && sender.is_closed()
}

async fn mpsc_closed_cancel_safe_case() -> bool {
    let (sender, mut receiver) = mpsc::channel(1);
    let entered = Arc::new(AtomicBool::new(false));
    let cancelled = tokio::spawn(mpsc_marked_closed(
        sender.clone(),
        Arc::clone(&entered),
        None,
    ));
    while !entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    cancelled.abort();
    let cancelled_result = cancelled.await;
    if !cancelled_result.is_err_and(|error| error.is_cancelled()) || sender.is_closed() {
        return false;
    }

    let retry = tokio::spawn(mpsc_marked_closed(
        sender.clone(),
        Arc::new(AtomicBool::new(false)),
        None,
    ));
    tokio::task::yield_now().await;
    receiver.close();
    retry.await.is_ok_and(|value| value) && sender.is_closed()
}

async fn mpsc_same_channel_case() -> bool {
    let (first, _first_receiver) = mpsc::channel::<i32>(1);
    let clone = first.clone();
    let (other, _other_receiver) = mpsc::channel::<i32>(1);
    first.same_channel(&clone)
        && clone.same_channel(&first)
        && !first.same_channel(&other)
        && !other.same_channel(&clone)
}

async fn mpsc_receiver_len_empty_case() -> bool {
    let (sender, mut receiver) = mpsc::channel(3);
    let initial = receiver.is_empty() && receiver.len() == 0;
    let Ok(unpublished) = sender.try_reserve() else {
        return false;
    };
    let reservation_is_not_message = receiver.is_empty() && receiver.len() == 0;
    drop(unpublished);
    if sender.try_send(1).is_err() || sender.try_send(2).is_err() {
        return false;
    }
    let buffered = !receiver.is_empty() && receiver.len() == 2;
    let first = receiver.recv().await;
    let one_left = !receiver.is_empty() && receiver.len() == 1;
    let second = receiver.recv().await;
    let drained = receiver.is_empty() && receiver.len() == 0;
    receiver.close();
    let finished = receiver.recv().await;
    initial
        && reservation_is_not_message
        && buffered
        && first == Some(1)
        && one_left
        && second == Some(2)
        && drained
        && finished.is_none()
        && receiver.is_empty()
        && receiver.len() == 0
}

async fn mpsc_try_reserve_errors_case() -> bool {
    let (sender, mut receiver) = mpsc::channel::<i32>(1);
    let Ok(permit) = sender.try_reserve() else {
        return false;
    };
    let full = sender.try_reserve();
    let held = sender.capacity() == 0 && matches!(full, Err(mpsc::error::TrySendError::Full(())));
    drop(permit);
    let restored = sender.capacity() == 1;
    receiver.close();
    let closed = sender.try_reserve();
    held && restored && matches!(closed, Err(mpsc::error::TrySendError::Closed(())))
}

async fn mpsc_owned_permit_send_release_case() -> bool {
    let (sender, mut receiver) = mpsc::channel(2);
    let Ok(first_permit) = sender.clone().reserve_owned().await else {
        return false;
    };
    let returned = first_permit.send(10);
    let send_returned_sender =
        returned.same_channel(&sender) && receiver.len() == 1 && sender.capacity() == 1;

    let Ok(second_permit) = returned.reserve_owned().await else {
        return false;
    };
    let returned = second_permit.release();
    let release_restored =
        returned.same_channel(&sender) && sender.capacity() == 1 && receiver.len() == 1;
    let second_sent = returned.try_send(11);
    let first = receiver.recv().await;
    let second = receiver.recv().await;
    send_returned_sender
        && release_restored
        && second_sent.is_ok()
        && first == Some(10)
        && second == Some(11)
        && receiver.is_empty()
}

async fn mpsc_owned_permit_same_channel_case() -> bool {
    let (first_sender, _first_receiver) = mpsc::channel::<i32>(2);
    let (other_sender, _other_receiver) = mpsc::channel::<i32>(1);
    let Ok(first) = first_sender.clone().reserve_owned().await else {
        return false;
    };
    let Ok(second) = first_sender.clone().reserve_owned().await else {
        return false;
    };
    let Ok(other) = other_sender.clone().reserve_owned().await else {
        return false;
    };

    first.same_channel(&second)
        && second.same_channel(&first)
        && !first.same_channel(&other)
        && first.same_channel_as_sender(&first_sender)
        && !first.same_channel_as_sender(&other_sender)
        && other.same_channel_as_sender(&other_sender)
}

async fn mpsc_owned_permit_lifetime_case() -> bool {
    let (sender, mut receiver) = mpsc::channel::<i32>(1);
    let weak = sender.downgrade();
    let Ok(permit) = sender.reserve_owned().await else {
        return false;
    };
    let held_open = weak.strong_count() == 1 && weak.weak_count() == 1 && !receiver.is_closed();
    drop(permit);
    let closed = weak.strong_count() == 0 && weak.upgrade().is_none();
    let finished = receiver.recv().await;
    held_open && closed && finished.is_none()
}

async fn mpsc_reserve_owned_closed_consumes_sender_case() -> bool {
    let (sender, mut receiver) = mpsc::channel::<i32>(1);
    let witness = sender.clone();
    receiver.close();
    let Err(error) = sender.reserve_owned().await else {
        return false;
    };
    witness.strong_count() == 1
        && witness.is_closed()
        && format!("{error:?}") == "SendError { .. }"
        && format!("{error}") == "channel closed"
}

async fn mpsc_try_reserve_owned_errors_case() -> bool {
    let (sender, mut receiver) = mpsc::channel(1);
    if sender.try_send(1).is_err() {
        return false;
    }
    let returned = match sender.clone().try_reserve_owned() {
        Err(mpsc::error::TrySendError::Full(returned)) => returned,
        _ => return false,
    };
    let full_returned_sender = returned.same_channel(&sender);
    if receiver.recv().await != Some(1) || returned.try_send(2).is_err() {
        return false;
    }
    let second = receiver.recv().await;

    let (closed_sender, mut closed_receiver) = mpsc::channel::<i32>(1);
    let witness = closed_sender.clone();
    closed_receiver.close();
    let returned_closed = match closed_sender.try_reserve_owned() {
        Err(mpsc::error::TrySendError::Closed(returned)) => returned,
        _ => return false,
    };
    full_returned_sender
        && second == Some(2)
        && returned_closed.same_channel(&witness)
        && returned_closed.is_closed()
}

async fn mpsc_reserve_owned_cancel_safe_case() -> bool {
    let (sender, mut receiver) = mpsc::channel(1);
    if sender.try_send(1).is_err() {
        return false;
    }
    let entered = Arc::new(AtomicBool::new(false));
    let pending = tokio::spawn(mpsc_marked_reserve_owned(
        sender.clone(),
        Arc::clone(&entered),
    ));
    while !entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    let pending_owns_one_sender = sender.strong_count() == 2;
    pending.abort();
    let cancelled = pending.await;

    let first = receiver.recv().await;
    let retry = sender.try_send(3);
    let third = receiver.recv().await;
    cancelled.is_err_and(|error| error.is_cancelled())
        && pending_owns_one_sender
        && sender.strong_count() == 1
        && first == Some(1)
        && retry.is_ok()
        && third == Some(3)
}

async fn mpsc_unbounded_fifo_multi_sender_case() -> bool {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let second = sender.clone();

    if sender.send(1).is_err()
        || second.send(2).is_err()
        || sender.send(3).is_err()
        || second.send(4).is_err()
    {
        return false;
    }
    drop(sender);
    drop(second);

    receiver.recv().await == Some(1)
        && receiver.recv().await == Some(2)
        && receiver.recv().await == Some(3)
        && receiver.recv().await == Some(4)
        && receiver.recv().await.is_none()
}

async fn mpsc_unbounded_send_try_errors_case() -> bool {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let empty = receiver.try_recv();
    if sender.send(7).is_err() {
        return false;
    }
    let received = receiver.try_recv();
    let empty_again = receiver.try_recv();
    drop(sender);
    let disconnected = receiver.try_recv();

    let (closed_sender, closed_receiver) = mpsc::unbounded_channel();
    drop(closed_receiver);
    let closed = closed_sender.send(41).expect_err("Receiver 已析构");

    matches!(empty, Err(mpsc::error::TryRecvError::Empty))
        && received == Ok(7)
        && matches!(empty_again, Err(mpsc::error::TryRecvError::Empty))
        && matches!(disconnected, Err(mpsc::error::TryRecvError::Disconnected))
        && closed.0 == 41
        && format!("{closed:?}") == "SendError { .. }"
        && format!("{closed}") == "channel closed"
}

async fn mpsc_unbounded_close_drain_case() -> bool {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    if sender.send(1).is_err() || sender.send(2).is_err() {
        return false;
    }
    receiver.close();
    let rejected = sender.send(3);
    let first = receiver.recv().await;
    let second = receiver.recv().await;
    let finished = receiver.recv().await;

    sender.is_closed()
        && matches!(rejected, Err(mpsc::error::SendError(3)))
        && first == Some(1)
        && second == Some(2)
        && finished.is_none()
        && receiver.is_closed()
}

async fn mpsc_unbounded_receiver_drop_case() -> bool {
    let (sender, receiver) = mpsc::unbounded_channel();
    if sender.send(7).is_err() {
        return false;
    }
    drop(receiver);
    let rejected = sender.send(8);
    matches!(rejected, Err(mpsc::error::SendError(8))) && sender.is_closed()
}

async fn mpsc_unbounded_marked_closed(
    sender: mpsc::UnboundedSender<i32>,
    entered: Arc<AtomicBool>,
    completed: Option<Arc<AtomicBool>>,
) -> bool {
    entered.store(true, Ordering::Release);
    sender.closed().await;
    if let Some(completed) = completed {
        completed.store(true, Ordering::Release);
    }
    sender.is_closed()
}

async fn mpsc_unbounded_closed_wakes_case() -> bool {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let first_entered = Arc::new(AtomicBool::new(false));
    let second_entered = Arc::new(AtomicBool::new(false));
    let first_completed = Arc::new(AtomicBool::new(false));
    let second_completed = Arc::new(AtomicBool::new(false));
    let first = tokio::spawn(mpsc_unbounded_marked_closed(
        sender.clone(),
        Arc::clone(&first_entered),
        Some(Arc::clone(&first_completed)),
    ));
    let second = tokio::spawn(mpsc_unbounded_marked_closed(
        sender.clone(),
        Arc::clone(&second_entered),
        Some(Arc::clone(&second_completed)),
    ));
    while !first_entered.load(Ordering::Acquire) || !second_entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    let pending = !first_completed.load(Ordering::Acquire)
        && !second_completed.load(Ordering::Acquire)
        && !sender.is_closed();

    receiver.close();
    let first_joined = first.await;
    let second_joined = second.await;
    sender.closed().await;
    pending
        && first_joined.is_ok_and(|value| value)
        && second_joined.is_ok_and(|value| value)
        && sender.is_closed()
}

async fn mpsc_unbounded_closed_cancel_safe_case() -> bool {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let entered = Arc::new(AtomicBool::new(false));
    let cancelled = tokio::spawn(mpsc_unbounded_marked_closed(
        sender.clone(),
        Arc::clone(&entered),
        None,
    ));
    while !entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    cancelled.abort();
    let cancelled_result = cancelled.await;
    if !cancelled_result.is_err_and(|error| error.is_cancelled()) || sender.is_closed() {
        return false;
    }

    let retry_entered = Arc::new(AtomicBool::new(false));
    let retry = tokio::spawn(mpsc_unbounded_marked_closed(
        sender.clone(),
        Arc::clone(&retry_entered),
        None,
    ));
    while !retry_entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    receiver.close();
    retry.await.is_ok_and(|value| value) && sender.is_closed()
}

async fn mpsc_unbounded_last_sender_weak_case() -> bool {
    let (sender, mut receiver) = mpsc::unbounded_channel::<i32>();
    let weak = sender.downgrade();
    let clone = sender.clone();
    drop(clone);
    drop(sender);

    let cannot_revive =
        weak.upgrade().is_none() && weak.strong_count() == 0 && weak.weak_count() == 1;
    let finished = receiver.recv().await;
    cannot_revive && finished.is_none()
}

async fn mpsc_unbounded_same_channel_counts_case() -> bool {
    let (sender, receiver) = mpsc::unbounded_channel::<i32>();
    let (other, _other_receiver) = mpsc::unbounded_channel::<i32>();
    let initial = sender.strong_count() == 1
        && sender.weak_count() == 0
        && receiver.sender_strong_count() == 1
        && receiver.sender_weak_count() == 0
        && !sender.same_channel(&other);

    let expanded;
    let after_upgrade;
    {
        let clone = sender.clone();
        let weak = sender.downgrade();
        let weak_clone = weak.clone();
        let upgraded = weak.upgrade();
        expanded = sender.same_channel(&clone)
            && upgraded
                .as_ref()
                .is_some_and(|upgraded| sender.same_channel(upgraded))
            && sender.strong_count() == 3
            && sender.weak_count() == 2
            && receiver.sender_strong_count() == 3
            && receiver.sender_weak_count() == 2;
        drop(upgraded);
        after_upgrade =
            sender.strong_count() == 2 && weak.strong_count() == 2 && weak.weak_count() == 2;
        drop(weak_clone);
        drop(weak);
        drop(clone);
    }

    initial
        && expanded
        && after_upgrade
        && sender.strong_count() == 1
        && sender.weak_count() == 0
        && receiver.sender_strong_count() == 1
        && receiver.sender_weak_count() == 0
}

async fn mpsc_unbounded_receiver_len_empty_case() -> bool {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let initial = receiver.is_empty() && receiver.len() == 0;
    if sender.send(1).is_err() || sender.send(2).is_err() {
        return false;
    }
    let buffered = !receiver.is_empty() && receiver.len() == 2;
    let first = receiver.recv().await;
    let one_left = !receiver.is_empty() && receiver.len() == 1;
    let second = receiver.recv().await;
    let drained = receiver.is_empty() && receiver.len() == 0;
    drop(sender);
    let finished = receiver.recv().await;

    initial
        && buffered
        && first == Some(1)
        && one_left
        && second == Some(2)
        && drained
        && finished.is_none()
        && receiver.is_empty()
        && receiver.len() == 0
}

async fn mpsc_unbounded_ready_recv_budget_case() -> bool {
    const BOUND: usize = 512;
    let (sender, mut receiver) = mpsc::unbounded_channel();
    for value in 1..=BOUND {
        if sender.send(value).is_err() {
            return false;
        }
    }
    drop(sender);

    let peer_ran = Arc::new(AtomicBool::new(false));
    let child_peer_ran = Arc::clone(&peer_ran);
    let peer = tokio::spawn(async move {
        child_peer_ran.store(true, Ordering::Release);
    });
    let mut peer_iteration = 0;
    let mut checksum = 0_usize;
    for iteration in 1..=BOUND {
        let Some(value) = receiver.recv().await else {
            return false;
        };
        checksum += value;
        if peer_iteration == 0 && peer_ran.load(Ordering::Acquire) {
            peer_iteration = iteration;
        }
    }
    let finished = receiver.recv().await;
    let peer_joined = peer.await.is_ok();

    peer_joined
        && (1..=BOUND).contains(&peer_iteration)
        && checksum == BOUND * (BOUND + 1) / 2
        && finished.is_none()
}

struct MpscUnboundedDropProbe {
    drops: Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for MpscUnboundedDropProbe {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

async fn mpsc_unbounded_value_drop_once_case() -> bool {
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (sender, mut receiver) = mpsc::unbounded_channel();
    for _ in 0..3 {
        if sender
            .send(MpscUnboundedDropProbe {
                drops: Arc::clone(&drops),
            })
            .is_err()
        {
            return false;
        }
    }

    let Some(first) = receiver.recv().await else {
        return false;
    };
    drop(first);
    let first_dropped = drops.load(Ordering::Relaxed) == 1;
    drop(receiver);
    let buffered_dropped = drops.load(Ordering::Relaxed) == 3;

    let rejected = sender.send(MpscUnboundedDropProbe {
        drops: Arc::clone(&drops),
    });
    let rejected_still_owned = drops.load(Ordering::Relaxed) == 3;
    drop(rejected);

    first_dropped && buffered_dropped && rejected_still_owned && drops.load(Ordering::Relaxed) == 4
}

async fn mpsc_unbounded_weak_upgrade_closed_case() -> bool {
    let (closed_sender, mut closed_receiver) = mpsc::unbounded_channel();
    let closed_weak = closed_sender.downgrade();
    closed_receiver.close();
    let Some(closed_upgraded) = closed_weak.upgrade() else {
        return false;
    };
    let close_preserves_strong = closed_sender.strong_count() == 2
        && closed_sender.same_channel(&closed_upgraded)
        && closed_upgraded.is_closed()
        && matches!(closed_upgraded.send(11), Err(mpsc::error::SendError(11)));

    let (dropped_sender, dropped_receiver) = mpsc::unbounded_channel();
    let dropped_weak = dropped_sender.downgrade();
    drop(dropped_receiver);
    let Some(dropped_upgraded) = dropped_weak.upgrade() else {
        return false;
    };
    let drop_preserves_strong = dropped_sender.strong_count() == 2
        && dropped_sender.same_channel(&dropped_upgraded)
        && dropped_upgraded.is_closed()
        && matches!(dropped_upgraded.send(12), Err(mpsc::error::SendError(12)));

    close_preserves_strong && drop_preserves_strong
}

async fn mpsc_unbounded_recv_cancel_safe_case() -> bool {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let mut pending_recv = Box::pin(receiver.recv());
    let observed_pending = poll_fn(|context| match pending_recv.as_mut().poll(context) {
        std::task::Poll::Pending => std::task::Poll::Ready(true),
        std::task::Poll::Ready(_) => std::task::Poll::Ready(false),
    })
    .await;
    drop(pending_recv);

    if !observed_pending || sender.send(29).is_err() {
        return false;
    }
    receiver.recv().await == Some(29)
}

async fn mpsc_unbounded_noncoop_send_closed_case() -> bool {
    const BOUND: usize = 512;

    let send_peer_ran = Arc::new(AtomicBool::new(false));
    let child_send_peer_ran = Arc::clone(&send_peer_ran);
    let send_peer = tokio::spawn(async move {
        child_send_peer_ran.store(true, Ordering::Release);
    });
    let (sender, receiver) = mpsc::unbounded_channel();
    for value in 0..BOUND {
        if sender.send(value).is_err() {
            return false;
        }
    }
    let send_did_not_yield = !send_peer_ran.load(Ordering::Acquire);
    let send_peer_joined = send_peer.await.is_ok();
    drop(receiver);

    let (closed_sender, mut closed_receiver) = mpsc::unbounded_channel::<usize>();
    closed_receiver.close();
    let closed_peer_ran = Arc::new(AtomicBool::new(false));
    let child_closed_peer_ran = Arc::clone(&closed_peer_ran);
    let closed_peer = tokio::spawn(async move {
        child_closed_peer_ran.store(true, Ordering::Release);
    });
    for _ in 0..BOUND {
        closed_sender.closed().await;
    }
    let closed_did_not_yield = !closed_peer_ran.load(Ordering::Acquire);
    let closed_peer_joined = closed_peer.await.is_ok();

    send_did_not_yield && send_peer_joined && closed_did_not_yield && closed_peer_joined
}

async fn watch_initial_borrow_case() -> bool {
    let (sender, receiver) = watch::channel(7);
    let receiver_ref = receiver.borrow();
    let receiver_ok = *receiver_ref == 7 && !receiver_ref.has_changed();
    drop(receiver_ref);
    let sender_ref = sender.borrow();
    receiver_ok
        && *sender_ref == 7
        && !sender_ref.has_changed()
        && matches!(receiver.has_changed(), Ok(false))
}

async fn watch_send_changed_borrow_update_case() -> bool {
    let (sender, mut receiver) = watch::channel(1);
    if sender.send(2).is_err() {
        return false;
    }

    let borrowed = receiver.borrow();
    let borrow_does_not_mark =
        *borrowed == 2 && borrowed.has_changed() && matches!(receiver.has_changed(), Ok(true));
    drop(borrowed);
    if receiver.changed().await.is_err() || !matches!(receiver.has_changed(), Ok(false)) {
        return false;
    }

    if sender.send(3).is_err() {
        return false;
    }
    let updated = receiver.borrow_and_update();
    let atomic_borrow_and_mark = *updated == 3 && updated.has_changed();
    drop(updated);

    borrow_does_not_mark && atomic_borrow_and_mark && matches!(receiver.has_changed(), Ok(false))
}

async fn watch_marks_and_has_changed_case() -> bool {
    let (sender, mut receiver) = watch::channel(5);
    receiver.mark_changed();
    if !matches!(receiver.has_changed(), Ok(true)) || receiver.changed().await.is_err() {
        return false;
    }
    receiver.mark_changed();
    receiver.mark_unchanged();
    let unchanged = matches!(receiver.has_changed(), Ok(false));

    if sender.send(6).is_err() {
        return false;
    }
    let unseen_before_close = matches!(receiver.has_changed(), Ok(true));
    drop(sender);

    unchanged && unseen_before_close && receiver.has_changed().is_err()
}

async fn watch_independent_receivers_subscribe_case() -> bool {
    let (sender, mut receiver_one) = watch::channel(10);
    let receiver_two = receiver_one.clone();
    if sender.send(11).is_err() || receiver_one.changed().await.is_err() {
        return false;
    }

    let independent_seen = matches!(receiver_one.has_changed(), Ok(false))
        && matches!(receiver_two.has_changed(), Ok(true))
        && receiver_one.same_channel(&receiver_two);
    let subscribed = sender.subscribe();

    independent_seen
        && *subscribed.borrow() == 11
        && matches!(subscribed.has_changed(), Ok(false))
        && subscribed.same_channel(&receiver_two)
}

async fn watch_last_sender_close_retains_value_case() -> bool {
    let (sender, mut receiver) = watch::channel(0);
    if sender.send(9).is_err() {
        return false;
    }
    drop(sender);

    let unseen_delivered_before_error = receiver.changed().await.is_ok();
    let retained = *receiver.borrow() == 9;
    let closed_after_seen = receiver.changed().await.is_err();
    unseen_delivered_before_error && retained && closed_after_seen
}

async fn watch_last_receiver_closes_sender_case() -> bool {
    let (sender, receiver) = watch::channel(3);
    drop(receiver);
    let closed_now = sender.is_closed();
    sender.closed().await;
    let send_error_preserves_value =
        matches!(sender.send(4), Err(watch::error::SendError(value)) if value == 4);

    let subscribed = sender.subscribe();
    let reopened = !sender.is_closed()
        && sender.send(5).is_ok()
        && *subscribed.borrow() == 5
        && matches!(subscribed.has_changed(), Ok(true));

    closed_now && send_error_preserves_value && reopened
}

async fn watch_changed_cancel_safe_case() -> bool {
    let (sender, mut receiver) = watch::channel(0);
    let observed_pending = {
        let mut pending_changed = Box::pin(receiver.changed());
        poll_fn(|context| match pending_changed.as_mut().poll(context) {
            std::task::Poll::Pending => std::task::Poll::Ready(true),
            std::task::Poll::Ready(_) => std::task::Poll::Ready(false),
        })
        .await
    };

    if !observed_pending || sender.send(1).is_err() {
        return false;
    }

    receiver.changed().await.is_ok()
        && *receiver.borrow() == 1
        && matches!(receiver.has_changed(), Ok(false))
}

async fn watch_same_channel_counts_case() -> bool {
    let (sender_one, receiver_one) = watch::channel(1);
    let sender_two = sender_one.clone();
    let receiver_two = receiver_one.clone();
    let subscribed = sender_one.subscribe();
    let (other_sender, other_receiver) = watch::channel(1);

    sender_one.sender_count() == 2
        && sender_two.sender_count() == 2
        && sender_one.receiver_count() == 3
        && sender_one.same_channel(&sender_two)
        && !sender_one.same_channel(&other_sender)
        && receiver_one.same_channel(&receiver_two)
        && receiver_one.same_channel(&subscribed)
        && !receiver_one.same_channel(&other_receiver)
}

async fn watch_send_replace_case() -> bool {
    let (sender, mut receiver) = watch::channel(1);
    let old = sender.send_replace(2);
    let first_replace = old == 1
        && matches!(receiver.has_changed(), Ok(true))
        && *receiver.borrow_and_update() == 2;
    let second_old = sender.send_replace(3);
    first_replace
        && second_old == 2
        && matches!(receiver.has_changed(), Ok(true))
        && *receiver.borrow() == 3
}

async fn watch_wait_for_case() -> bool {
    let (sender, mut receiver) = watch::channel(2);
    let immediate = match receiver.wait_for(|value| *value == 2).await {
        Ok(value) => *value == 2,
        Err(_) => false,
    };
    if !immediate || sender.send(3).is_err() || sender.send(4).is_err() {
        return false;
    }

    let latest = match receiver.wait_for(|value| *value >= 4).await {
        Ok(value) => *value == 4 && value.has_changed(),
        Err(_) => false,
    };
    drop(sender);
    let closed_true_predicate = match receiver.wait_for(|value| *value == 4).await {
        Ok(value) => *value == 4,
        Err(_) => false,
    };
    let closed_false_predicate = receiver.wait_for(|value| *value == 99).await.is_err();

    latest && closed_true_predicate && closed_false_predicate
}

async fn watch_value_drop_and_clone_case() -> bool {
    struct Tracked {
        drops: Arc<std::sync::atomic::AtomicUsize>,
        clones: Arc<std::sync::atomic::AtomicUsize>,
        value: usize,
    }

    impl Clone for Tracked {
        fn clone(&self) -> Self {
            self.clones.fetch_add(1, Ordering::AcqRel);
            Self {
                drops: Arc::clone(&self.drops),
                clones: Arc::clone(&self.clones),
                value: self.value,
            }
        }
    }

    impl Drop for Tracked {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::AcqRel);
        }
    }

    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let clones = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    {
        let make_value = |value| Tracked {
            drops: Arc::clone(&drops),
            clones: Arc::clone(&clones),
            value,
        };
        let (sender, receiver) = watch::channel(make_value(1));
        let sender_two = sender.clone();
        let receiver_two = receiver.clone();
        let subscribed = sender.subscribe();
        drop(sender.send_replace(make_value(2)));
        if sender.send(make_value(3)).is_err()
            || receiver.borrow().value != 3
            || receiver_two.borrow().value != 3
            || subscribed.borrow().value != 3
        {
            return false;
        }
        drop(sender_two);
        drop(receiver_two);
        drop(subscribed);
    }

    clones.load(Ordering::Acquire) == 0 && drops.load(Ordering::Acquire) == 3
}

async fn watch_error_format_case() -> bool {
    let (sender, receiver) = watch::channel(1);
    drop(receiver);
    let send_error = sender.send(2).expect_err("watch send must fail");
    let send_format = send_error.to_string() == "channel closed"
        && format!("{send_error:?}") == "SendError { .. }";

    let (closed_sender, mut closed_receiver) = watch::channel(3);
    drop(closed_sender);
    let recv_error = closed_receiver
        .changed()
        .await
        .expect_err("closed seen watch must fail");
    let recv_format =
        recv_error.to_string() == "channel closed" && format!("{recv_error:?}") == "RecvError(())";

    send_format && recv_format
}

async fn watch_cooperative_ready_paths_case() -> bool {
    const BOUND: usize = 512;

    let changed_peer_ran = Arc::new(AtomicBool::new(false));
    let changed_peer_flag = Arc::clone(&changed_peer_ran);
    let changed_peer = tokio::spawn(async move {
        changed_peer_flag.store(true, Ordering::Release);
    });
    let (sender, mut receiver) = watch::channel(0);
    for value in 1..=BOUND {
        if sender.send(value).is_err() || receiver.changed().await.is_err() {
            return false;
        }
    }
    let changed_yielded = changed_peer_ran.load(Ordering::Acquire);
    let changed_peer_joined = changed_peer.await.is_ok();

    let error_peer_ran = Arc::new(AtomicBool::new(false));
    let error_peer_flag = Arc::clone(&error_peer_ran);
    let error_peer = tokio::spawn(async move {
        error_peer_flag.store(true, Ordering::Release);
    });
    let (closed_sender, mut closed_receiver) = watch::channel(0);
    drop(closed_sender);
    for _ in 0..BOUND {
        if closed_receiver.changed().await.is_ok() {
            return false;
        }
    }
    let error_yielded = error_peer_ran.load(Ordering::Acquire);
    let error_peer_joined = error_peer.await.is_ok();

    let closed_peer_ran = Arc::new(AtomicBool::new(false));
    let closed_peer_flag = Arc::clone(&closed_peer_ran);
    let closed_peer = tokio::spawn(async move {
        closed_peer_flag.store(true, Ordering::Release);
    });
    let (closed_wait_sender, closed_wait_receiver) = watch::channel(0);
    drop(closed_wait_receiver);
    for _ in 0..BOUND {
        closed_wait_sender.closed().await;
    }
    let closed_yielded = closed_peer_ran.load(Ordering::Acquire);
    let closed_peer_joined = closed_peer.await.is_ok();

    let wait_peer_ran = Arc::new(AtomicBool::new(false));
    let wait_peer_flag = Arc::clone(&wait_peer_ran);
    let wait_peer = tokio::spawn(async move {
        wait_peer_flag.store(true, Ordering::Release);
    });
    let (_wait_sender, mut wait_receiver) = watch::channel(7);
    for _ in 0..BOUND {
        if wait_receiver.wait_for(|value| *value == 7).await.is_err() {
            return false;
        }
    }
    let wait_yielded = wait_peer_ran.load(Ordering::Acquire);
    let wait_peer_joined = wait_peer.await.is_ok();

    changed_yielded
        && changed_peer_joined
        && error_yielded
        && error_peer_joined
        && closed_yielded
        && closed_peer_joined
        && wait_yielded
        && wait_peer_joined
}

async fn watch_coop_changed_success_boundary_case() -> bool {
    tokio::spawn(async {
        let peer_ran = Arc::new(AtomicBool::new(false));
        let peer_flag = Arc::clone(&peer_ran);
        let peer = tokio::spawn(async move {
            peer_flag.store(true, Ordering::Release);
        });
        let (sender, mut receiver) = watch::channel(0_usize);
        let mut first_peer_iteration = 0_usize;
        for iteration in 1..=129 {
            if sender.send(iteration).is_err() || receiver.changed().await.is_err() {
                return false;
            }
            if first_peer_iteration == 0 && peer_ran.load(Ordering::Acquire) {
                first_peer_iteration = iteration;
            }
        }
        peer.await.is_ok() && first_peer_iteration == 129
    })
    .await
    .is_ok_and(|value| value)
}

async fn watch_coop_changed_error_boundary_case() -> bool {
    tokio::spawn(async {
        let peer_ran = Arc::new(AtomicBool::new(false));
        let peer_flag = Arc::clone(&peer_ran);
        let peer = tokio::spawn(async move {
            peer_flag.store(true, Ordering::Release);
        });
        let (sender, mut receiver) = watch::channel(0_usize);
        drop(sender);
        let mut first_peer_iteration = 0_usize;
        for iteration in 1..=129 {
            if receiver.changed().await.is_ok() {
                return false;
            }
            if first_peer_iteration == 0 && peer_ran.load(Ordering::Acquire) {
                first_peer_iteration = iteration;
            }
        }
        peer.await.is_ok() && first_peer_iteration == 129
    })
    .await
    .is_ok_and(|value| value)
}

async fn watch_coop_closed_boundary_case() -> bool {
    tokio::spawn(async {
        let peer_ran = Arc::new(AtomicBool::new(false));
        let peer_flag = Arc::clone(&peer_ran);
        let peer = tokio::spawn(async move {
            peer_flag.store(true, Ordering::Release);
        });
        let (sender, receiver) = watch::channel(0_usize);
        drop(receiver);
        let mut first_peer_iteration = 0_usize;
        for iteration in 1..=129 {
            sender.closed().await;
            if first_peer_iteration == 0 && peer_ran.load(Ordering::Acquire) {
                first_peer_iteration = iteration;
            }
        }
        peer.await.is_ok() && first_peer_iteration == 129
    })
    .await
    .is_ok_and(|value| value)
}

async fn watch_coop_wait_for_success_boundary_case() -> bool {
    tokio::spawn(async {
        let peer_ran = Arc::new(AtomicBool::new(false));
        let peer_flag = Arc::clone(&peer_ran);
        let peer = tokio::spawn(async move {
            peer_flag.store(true, Ordering::Release);
        });
        let (_sender, mut receiver) = watch::channel(7_usize);
        let mut first_peer_iteration = 0_usize;
        for iteration in 1..=129 {
            if receiver.wait_for(|value| *value == 7).await.is_err() {
                return false;
            }
            if first_peer_iteration == 0 && peer_ran.load(Ordering::Acquire) {
                first_peer_iteration = iteration;
            }
        }
        peer.await.is_ok() && first_peer_iteration == 129
    })
    .await
    .is_ok_and(|value| value)
}

async fn watch_coop_wait_for_error_boundary_case() -> bool {
    tokio::spawn(async {
        let peer_ran = Arc::new(AtomicBool::new(false));
        let peer_flag = Arc::clone(&peer_ran);
        let peer = tokio::spawn(async move {
            peer_flag.store(true, Ordering::Release);
        });
        let (sender, mut receiver) = watch::channel(7_usize);
        drop(sender);
        let mut first_peer_iteration = 0_usize;
        for iteration in 1..=129 {
            if receiver.wait_for(|value| *value == 99).await.is_ok() {
                return false;
            }
            if first_peer_iteration == 0 && peer_ran.load(Ordering::Acquire) {
                first_peer_iteration = iteration;
            }
        }
        peer.await.is_ok() && first_peer_iteration == 129
    })
    .await
    .is_ok_and(|value| value)
}

async fn watch_expect_fresh_poll_debit() -> bool {
    let peer_ran = Arc::new(AtomicBool::new(false));
    let peer_flag = Arc::clone(&peer_ran);
    let peer = tokio::spawn(async move {
        peer_flag.store(true, Ordering::Release);
    });
    let mut first_peer_iteration = 0_usize;
    for iteration in 1..=128 {
        tokio::task::consume_budget().await;
        if first_peer_iteration == 0 && peer_ran.load(Ordering::Acquire) {
            first_peer_iteration = iteration;
        }
    }
    peer.await.is_ok() && first_peer_iteration == 128
}

async fn watch_coop_changed_fresh_wake_budget_case() -> bool {
    let entered = Arc::new(AtomicBool::new(false));
    let child_entered = Arc::clone(&entered);
    let (sender, mut receiver) = watch::channel(0_usize);
    let watcher = tokio::spawn(async move {
        child_entered.store(true, Ordering::Release);
        if receiver.changed().await.is_err() {
            return false;
        }
        watch_expect_fresh_poll_debit().await
    });
    while !entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    if sender.send(1).is_err() {
        return false;
    }
    watcher.await.is_ok_and(|value| value)
}

async fn watch_coop_wait_for_fresh_wake_budget_case() -> bool {
    let entered = Arc::new(AtomicBool::new(false));
    let child_entered = Arc::clone(&entered);
    let (sender, mut receiver) = watch::channel(0_usize);
    let watcher = tokio::spawn(async move {
        child_entered.store(true, Ordering::Release);
        if receiver.wait_for(|value| *value == 1).await.is_err() {
            return false;
        }
        watch_expect_fresh_poll_debit().await
    });
    while !entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    if sender.send(1).is_err() {
        return false;
    }
    watcher.await.is_ok_and(|value| value)
}

async fn watch_coop_closed_fresh_wake_budget_case() -> bool {
    let entered = Arc::new(AtomicBool::new(false));
    let child_entered = Arc::clone(&entered);
    let (sender, receiver) = watch::channel(0_usize);
    let watcher = tokio::spawn(async move {
        child_entered.store(true, Ordering::Release);
        sender.closed().await;
        watch_expect_fresh_poll_debit().await
    });
    while !entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    drop(receiver);
    watcher.await.is_ok_and(|value| value)
}

async fn broadcast_capacity_rounding_lag_case() -> bool {
    let (sender, mut receiver) = broadcast::channel(3);
    for value in 1_usize..=5 {
        if sender.send(value).is_err() {
            return false;
        }
    }
    if !matches!(
        receiver.recv().await,
        Err(broadcast::error::RecvError::Lagged(1))
    ) {
        return false;
    }
    for expected in 2_usize..=5 {
        if receiver.recv().await != Ok(expected) {
            return false;
        }
    }
    true
}

async fn broadcast_failed_send_then_subscribe_case() -> bool {
    let (sender, receiver) = broadcast::channel(4);
    drop(receiver);
    let failed = sender.send(10);
    let mut subscribed = sender.subscribe();
    let sent = sender.send(20);
    matches!(failed, Err(broadcast::error::SendError(10)))
        && matches!(sent, Ok(1))
        && subscribed.recv().await == Ok(20)
}

async fn broadcast_independent_receivers_case() -> bool {
    let (sender, mut first) = broadcast::channel(4);
    let mut second = sender.subscribe();
    if !matches!(sender.send(10), Ok(2)) || !matches!(sender.send(20), Ok(2)) {
        return false;
    }
    first.recv().await == Ok(10)
        && first.recv().await == Ok(20)
        && second.recv().await == Ok(10)
        && second.recv().await == Ok(20)
}

async fn broadcast_resubscribe_skips_backlog_case() -> bool {
    let (sender, mut original) = broadcast::channel(4);
    if sender.send(1).is_err() {
        return false;
    }
    let mut resubscribed = original.resubscribe();
    if !matches!(sender.send(2), Ok(2)) {
        return false;
    }
    resubscribed.recv().await == Ok(2)
        && original.recv().await == Ok(1)
        && original.recv().await == Ok(2)
}

async fn broadcast_drain_then_closed_case() -> bool {
    let (sender, mut receiver) = broadcast::channel(4);
    if sender.send(1).is_err() || sender.send(2).is_err() {
        return false;
    }
    drop(sender);
    receiver.recv().await == Ok(1)
        && receiver.recv().await == Ok(2)
        && matches!(
            receiver.recv().await,
            Err(broadcast::error::RecvError::Closed)
        )
}

async fn broadcast_lagged_exact_case() -> bool {
    let (sender, mut receiver) = broadcast::channel(2);
    for value in 1_usize..=5 {
        if sender.send(value).is_err() {
            return false;
        }
    }
    matches!(
        receiver.recv().await,
        Err(broadcast::error::RecvError::Lagged(3))
    ) && receiver.recv().await == Ok(4)
        && receiver.recv().await == Ok(5)
}

async fn broadcast_try_recv_empty_closed_case() -> bool {
    let (sender, mut receiver) = broadcast::channel(2);
    let initial = receiver.try_recv();
    if sender.send(7).is_err() {
        return false;
    }
    let received = receiver.try_recv();
    let empty_again = receiver.try_recv();
    drop(sender);
    let closed = receiver.try_recv();
    matches!(initial, Err(broadcast::error::TryRecvError::Empty))
        && received == Ok(7)
        && matches!(empty_again, Err(broadcast::error::TryRecvError::Empty))
        && matches!(closed, Err(broadcast::error::TryRecvError::Closed))
}

async fn broadcast_send_receiver_count_case() -> bool {
    let (sender, first) = broadcast::channel(2);
    let second = sender.subscribe();
    let sent_two = sender.send(1);
    drop(second);
    let sent_one = sender.send(2);
    drop(first);
    let failed = sender.send(3);
    matches!(sent_two, Ok(2))
        && matches!(sent_one, Ok(1))
        && matches!(failed, Err(broadcast::error::SendError(3)))
}

async fn broadcast_counts_case() -> bool {
    let (sender, receiver) = broadcast::channel::<usize>(4);
    let initial = sender.strong_count() == 1
        && sender.weak_count() == 0
        && sender.receiver_count() == 1
        && receiver.sender_strong_count() == 1
        && receiver.sender_weak_count() == 0;

    let second_sender = sender.clone();
    let weak = sender.downgrade();
    let weak_clone = weak.clone();
    let second_receiver = sender.subscribe();
    let expanded = sender.strong_count() == 2
        && sender.weak_count() == 2
        && sender.receiver_count() == 2
        && receiver.sender_strong_count() == 2
        && receiver.sender_weak_count() == 2
        && weak.strong_count() == 2
        && weak.weak_count() == 2;

    drop(second_sender);
    drop(weak_clone);
    drop(weak);
    drop(second_receiver);
    let restored = sender.strong_count() == 1
        && sender.weak_count() == 0
        && sender.receiver_count() == 1
        && receiver.sender_strong_count() == 1
        && receiver.sender_weak_count() == 0;
    initial && expanded && restored
}

async fn broadcast_weak_upgrade_case() -> bool {
    let (sender, receiver) = broadcast::channel::<usize>(4);
    let weak = sender.downgrade();
    let Some(upgraded) = weak.upgrade() else {
        return false;
    };
    let live = sender.strong_count() == 2 && weak.strong_count() == 2 && weak.weak_count() == 1;
    drop(upgraded);
    drop(sender);
    live && weak.strong_count() == 0
        && weak.weak_count() == 1
        && weak.upgrade().is_none()
        && receiver.is_closed()
}

struct BroadcastCloneProbe {
    value: usize,
    panic_once: Arc<AtomicBool>,
}

impl Clone for BroadcastCloneProbe {
    fn clone(&self) -> Self {
        if self.panic_once.swap(false, Ordering::AcqRel) {
            panic!("broadcast 差分测试的预期 clone panic");
        }
        Self {
            value: self.value,
            panic_once: Arc::clone(&self.panic_once),
        }
    }
}

async fn broadcast_copy_panic_advances_cursor_case() -> bool {
    let panic_once = Arc::new(AtomicBool::new(true));
    let (sender, receiver) = broadcast::channel(4);
    if sender
        .send(BroadcastCloneProbe {
            value: 1,
            panic_once: Arc::clone(&panic_once),
        })
        .is_err()
        || sender
            .send(BroadcastCloneProbe {
                value: 2,
                panic_once: Arc::clone(&panic_once),
            })
            .is_err()
    {
        return false;
    }

    let receiver = Arc::new(tokio::sync::Mutex::new(receiver));
    let task_receiver = Arc::clone(&receiver);
    let panicking = tokio::spawn(async move {
        let mut locked = task_receiver.lock().await;
        let _ = locked.recv().await;
    });
    let panicked = panicking.await.is_err_and(|error| error.is_panic());
    let second = receiver.lock().await.recv().await;
    panicked && second.is_ok_and(|value| value.value == 2)
}

async fn broadcast_recv_cooperative_ready_budget_case() -> bool {
    tokio::spawn(async {
        let peer_ran = Arc::new(AtomicBool::new(false));
        let peer_flag = Arc::clone(&peer_ran);
        let peer = tokio::spawn(async move {
            peer_flag.store(true, Ordering::Release);
        });
        let (sender, mut receiver) = broadcast::channel(256);
        for value in 1_usize..=129 {
            if sender.send(value).is_err() {
                return false;
            }
        }
        let mut first_peer_iteration = 0_usize;
        for expected in 1_usize..=129 {
            if receiver.recv().await != Ok(expected) {
                return false;
            }
            if first_peer_iteration == 0 && peer_ran.load(Ordering::Acquire) {
                first_peer_iteration = expected;
            }
        }
        peer.await.is_ok() && first_peer_iteration == 129
    })
    .await
    .is_ok_and(|value| value)
}

async fn broadcast_expect_fresh_poll_debit() -> bool {
    let peer_ran = Arc::new(AtomicBool::new(false));
    let peer_flag = Arc::clone(&peer_ran);
    let peer = tokio::spawn(async move {
        peer_flag.store(true, Ordering::Release);
    });
    let mut first_peer_iteration = 0_usize;
    for iteration in 1_usize..=128 {
        tokio::task::consume_budget().await;
        if first_peer_iteration == 0 && peer_ran.load(Ordering::Acquire) {
            first_peer_iteration = iteration;
        }
    }
    peer.await.is_ok() && first_peer_iteration == 128
}

async fn broadcast_recv_cooperative_pending_budget_case() -> bool {
    let entered = Arc::new(AtomicBool::new(false));
    let child_entered = Arc::clone(&entered);
    let (sender, mut receiver) = broadcast::channel(1);
    let watcher = tokio::spawn(async move {
        child_entered.store(true, Ordering::Release);
        if receiver.recv().await != Ok(7) {
            return false;
        }
        broadcast_expect_fresh_poll_debit().await
    });
    while !entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    if sender.send(7).is_err() {
        return false;
    }
    watcher.await.is_ok_and(|value| value)
}

async fn broadcast_closed_noncooperative_case() -> bool {
    tokio::spawn(async {
        let peer_ran = Arc::new(AtomicBool::new(false));
        let peer_flag = Arc::clone(&peer_ran);
        let peer = tokio::spawn(async move {
            peer_flag.store(true, Ordering::Release);
        });
        let (sender, receiver) = broadcast::channel::<usize>(1);
        drop(receiver);
        for _ in 0..512 {
            sender.closed().await;
        }
        let did_not_yield = !peer_ran.load(Ordering::Acquire);
        did_not_yield && peer.await.is_ok()
    })
    .await
    .is_ok_and(|value| value)
}

fn io_readbuf_regions_clear_case() -> bool {
    let mut storage = [0_u8; 5];
    let mut buffer = ReadBuf::new(&mut storage);
    let initial =
        buffer.filled().is_empty() && buffer.initialized().len() == 5 && buffer.remaining() == 5;
    buffer.put_slice(b"ab");
    let filled =
        buffer.filled() == b"ab" && buffer.initialized().len() == 5 && buffer.remaining() == 3;
    buffer.clear();
    let cleared =
        buffer.filled().is_empty() && buffer.initialized().len() == 5 && buffer.remaining() == 5;
    buffer.set_filled(2);
    initial && filled && cleared && buffer.filled() == b"ab"
}

struct IoProbeReader {
    bytes: Vec<u8>,
    position: usize,
    max_read: usize,
    read_calls: usize,
    fail_on_call: Option<usize>,
    pending_on_call: Option<usize>,
    pending_waker: Option<std::task::Waker>,
}

impl IoProbeReader {
    fn partial(bytes: &[u8], max_read: usize) -> Self {
        Self {
            bytes: bytes.to_vec(),
            position: 0,
            max_read,
            read_calls: 0,
            fail_on_call: None,
            pending_on_call: None,
            pending_waker: None,
        }
    }

    fn partial_then_error(bytes: &[u8], max_read: usize, fail_on_call: usize) -> Self {
        Self {
            bytes: bytes.to_vec(),
            position: 0,
            max_read,
            read_calls: 0,
            fail_on_call: Some(fail_on_call),
            pending_on_call: None,
            pending_waker: None,
        }
    }

    fn partial_then_pending(bytes: &[u8], max_read: usize, pending_on_call: usize) -> Self {
        Self {
            bytes: bytes.to_vec(),
            position: 0,
            max_read,
            read_calls: 0,
            fail_on_call: None,
            pending_on_call: Some(pending_on_call),
            pending_waker: None,
        }
    }

    fn wake_late(&mut self) {
        if let Some(waker) = self.pending_waker.take() {
            waker.wake();
        }
    }
}

impl AsyncRead for IoProbeReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.read_calls += 1;
        if self.fail_on_call == Some(self.read_calls) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Other,
                "injected read error",
            )));
        }
        if self.pending_on_call == Some(self.read_calls) {
            self.pending_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let amount = self
            .max_read
            .min(buffer.remaining())
            .min(self.bytes.len() - self.position);
        let begin = self.position;
        let end = begin + amount;
        buffer.put_slice(&self.bytes[begin..end]);
        self.position = end;
        Poll::Ready(Ok(()))
    }
}

async fn io_partial_read_eof_zero_capacity_case() -> bool {
    let mut reader = &b"abc"[..];
    let mut first = [0_u8; 2];
    let first_len = reader.read(&mut first).await;
    let mut second = [0_u8; 2];
    let second_len = reader.read(&mut second).await;
    let eof_len = reader.read(&mut second).await;

    let mut zero_reader = &b"z"[..];
    let mut empty = [];
    let zero_len = zero_reader.read(&mut empty).await;
    let mut remaining = [0_u8; 1];
    let remaining_len = zero_reader.read(&mut remaining).await;

    first_len.is_ok_and(|value| value == 2)
        && first == *b"ab"
        && second_len.is_ok_and(|value| value == 1)
        && second[0] == b'c'
        && eof_len.is_ok_and(|value| value == 0)
        && zero_len.is_ok_and(|value| value == 0)
        && remaining_len.is_ok_and(|value| value == 1)
        && remaining == *b"z"
}

#[derive(Default)]
struct IoProbeWriter {
    bytes: Vec<u8>,
    max_write: usize,
    write_calls: usize,
    zero_write: bool,
    fail_on_call: Option<usize>,
    pending_on_call: Option<usize>,
    pending_waker: Option<std::task::Waker>,
}

impl IoProbeWriter {
    fn partial(max_write: usize) -> Self {
        Self {
            max_write,
            ..Self::default()
        }
    }

    fn zero() -> Self {
        Self {
            zero_write: true,
            ..Self::default()
        }
    }

    fn partial_then_error(max_write: usize, fail_on_call: usize) -> Self {
        Self {
            max_write,
            fail_on_call: Some(fail_on_call),
            ..Self::default()
        }
    }

    fn partial_then_pending(max_write: usize, pending_on_call: usize) -> Self {
        Self {
            max_write,
            pending_on_call: Some(pending_on_call),
            ..Self::default()
        }
    }

    fn wake_late(&mut self) {
        if let Some(waker) = self.pending_waker.take() {
            waker.wake();
        }
    }
}

impl AsyncWrite for IoProbeWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.write_calls += 1;
        if self.fail_on_call == Some(self.write_calls) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Other,
                "injected write error",
            )));
        }
        if self.pending_on_call == Some(self.write_calls) {
            self.pending_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        if self.zero_write {
            return Poll::Ready(Ok(0));
        }
        let written = self.max_write.min(buf.len());
        self.bytes.extend_from_slice(&buf[..written]);
        Poll::Ready(Ok(written))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

async fn io_read_exact_partial_success_case() -> bool {
    let mut reader = IoProbeReader::partial(b"abcde", 2);
    let mut output = [0_u8; 5];
    reader
        .read_exact(&mut output)
        .await
        .is_ok_and(|value| value == 5)
        && output == *b"abcde"
        && reader.position == 5
        && reader.read_calls == 3
}

async fn io_read_exact_early_eof_case() -> bool {
    let mut reader = IoProbeReader::partial(b"abc", 2);
    let mut output = [0_u8; 5];
    let result = reader.read_exact(&mut output).await;
    result.is_err_and(|error| error.kind() == io::ErrorKind::UnexpectedEof)
        && output[..3] == *b"abc"
        && reader.position == 3
        && reader.read_calls == 3
}

async fn io_read_exact_partial_error_case() -> bool {
    let mut reader = IoProbeReader::partial_then_error(b"abcdef", 2, 3);
    let mut output = [0_u8; 6];
    let result = reader.read_exact(&mut output).await;
    result.is_err_and(|error| error.kind() == io::ErrorKind::Other)
        && output[..4] == *b"abcd"
        && reader.position == 4
        && reader.read_calls == 3
}

async fn io_write_all_partial_zero_case() -> bool {
    let mut writer = IoProbeWriter::partial(2);
    let complete = writer.write_all(b"abcde").await;
    let partial_ok = complete.is_ok() && writer.bytes == b"abcde" && writer.write_calls == 3;

    let mut zero = IoProbeWriter::zero();
    let zero_result = zero.write_all(b"x").await;
    partial_ok
        && zero_result.is_err_and(|error| error.kind() == io::ErrorKind::WriteZero)
        && zero.write_calls == 1
        && zero.bytes.is_empty()
}

async fn io_write_all_partial_error_case() -> bool {
    let mut writer = IoProbeWriter::partial_then_error(2, 3);
    let result = writer.write_all(b"abcdef").await;
    result.is_err_and(|error| error.kind() == io::ErrorKind::Other)
        && writer.bytes == b"abcd"
        && writer.write_calls == 3
}

async fn io_exact_cancel_partial_late_wake_case() -> bool {
    let mut reader = IoProbeReader::partial_then_pending(b"abcd", 2, 2);
    let mut output = [0_u8; 4];
    let mut read_future = Box::pin(reader.read_exact(&mut output));
    let read_pending = poll_fn(|cx| Poll::Ready(read_future.as_mut().poll(cx).is_pending())).await;
    drop(read_future);
    let read_cancelled =
        read_pending && output[..2] == *b"ab" && reader.position == 2 && reader.read_calls == 2;
    reader.wake_late();
    tokio::task::yield_now().await;
    let read_late_safe = output[..2] == *b"ab" && reader.position == 2 && reader.read_calls == 2;

    let mut writer = IoProbeWriter::partial_then_pending(2, 2);
    let mut write_future = Box::pin(writer.write_all(b"abcd"));
    let write_pending =
        poll_fn(|cx| Poll::Ready(write_future.as_mut().poll(cx).is_pending())).await;
    drop(write_future);
    let write_cancelled = write_pending && writer.bytes == b"ab" && writer.write_calls == 2;
    writer.wake_late();
    tokio::task::yield_now().await;
    let write_late_safe = writer.bytes == b"ab" && writer.write_calls == 2;

    read_cancelled && read_late_safe && write_cancelled && write_late_safe
}

async fn io_exact_empty_no_poll_case() -> bool {
    let mut reader = IoProbeReader::partial(b"x", 1);
    let mut output = [];
    let read = reader.read_exact(&mut output).await;
    let mut writer = IoProbeWriter::partial(1);
    let write = writer.write_all(b"").await;
    read.is_ok_and(|value| value == 0)
        && write.is_ok()
        && reader.position == 0
        && reader.read_calls == 0
        && writer.write_calls == 0
}

async fn io_partial_write_single_attempt_case() -> bool {
    let mut writer = IoProbeWriter::partial(2);
    let result = writer.write(b"abcd").await;
    result.is_ok_and(|value| value == 2) && writer.write_calls == 1 && writer.bytes == b"ab"
}

async fn io_write_zero_success_case() -> bool {
    let mut writer = IoProbeWriter::zero();
    writer.write(b"x").await.is_ok_and(|value| value == 0)
        && writer.write_calls == 1
        && writer.bytes.is_empty()
}

async fn io_write_vectored_default_first_nonempty_case() -> bool {
    let mut writer = IoProbeWriter::partial(usize::MAX);
    let buffers = [IoSlice::new(b""), IoSlice::new(b"ab"), IoSlice::new(b"cd")];
    let result = writer.write_vectored(&buffers).await;
    result.is_ok_and(|value| value == 2)
        && writer.write_calls == 1
        && writer.bytes == b"ab"
        && !writer.is_write_vectored()
}

#[derive(Default)]
struct IoLifecycleWriter {
    events: Vec<&'static str>,
    shutdown: bool,
}

impl AsyncWrite for IoLifecycleWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.shutdown {
            self.events.push("write_after_shutdown");
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "writer is shut down",
            )));
        }
        self.events.push("write");
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.events.push("flush");
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.shutdown {
            self.events.push("flush_in_shutdown");
            self.events.push("shutdown");
            self.shutdown = true;
        }
        Poll::Ready(Ok(()))
    }
}

async fn io_flush_shutdown_order_case() -> bool {
    let mut writer = IoLifecycleWriter::default();
    let write = writer.write(b"x").await;
    let flush = writer.flush().await;
    let shutdown = writer.shutdown().await;
    write.is_ok_and(|value| value == 1)
        && flush.is_ok()
        && shutdown.is_ok()
        && writer.events == ["write", "flush", "flush_in_shutdown", "shutdown"]
}

async fn io_shutdown_terminal_case() -> bool {
    let mut writer = IoLifecycleWriter::default();
    let first = writer.shutdown().await;
    let second = writer.shutdown().await;
    let write = writer.write(b"x").await;
    first.is_ok()
        && second.is_ok()
        && write.is_err_and(|error| error.kind() == io::ErrorKind::BrokenPipe)
        && writer.events == ["flush_in_shutdown", "shutdown", "write_after_shutdown"]
}

struct IoReadyReader;

impl AsyncRead for IoReadyReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buffer.remaining() != 0 {
            buffer.put_slice(b"x");
        }
        Poll::Ready(Ok(()))
    }
}

async fn io_ready_ext_noncooperative_case() -> bool {
    tokio::spawn(async {
        let peer_ran = Arc::new(AtomicBool::new(false));
        let peer_flag = Arc::clone(&peer_ran);
        let peer = tokio::spawn(async move {
            peer_flag.store(true, Ordering::Release);
        });
        let mut reader = IoReadyReader;
        let mut writer = IoProbeWriter::partial(usize::MAX);
        let mut byte = [0_u8; 1];
        for _ in 0..512 {
            if !reader.read(&mut byte).await.is_ok_and(|value| value == 1)
                || !writer.write(b"x").await.is_ok_and(|value| value == 1)
                || writer.flush().await.is_err()
            {
                return false;
            }
        }
        let did_not_yield = !peer_ran.load(Ordering::Acquire);
        did_not_yield && peer.await.is_ok()
    })
    .await
    .is_ok_and(|value| value)
}

async fn multi_spawn_join_case() -> bool {
    let completed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut handles = Vec::new();
    for _ in 0..100 {
        let task_completed = Arc::clone(&completed);
        handles.push(tokio::spawn(async move {
            tokio::task::yield_now().await;
            task_completed.fetch_add(1, Ordering::Relaxed);
        }));
    }
    for handle in handles {
        if handle.await.is_err() {
            return false;
        }
    }
    completed.load(Ordering::Acquire) == 100
}

fn main() {
    let current = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .max_blocking_threads(1)
        .build()
        .expect("current-thread runtime");
    current.block_on(async {
        emit("spawn_deferred", spawn_deferred_case().await);
        emit("abort_before_poll", abort_before_poll_case().await);
        emit("join_drop_detaches", join_drop_detaches_case().await);
        emit("panic_join_error", panic_join_error_case().await);
        emit(
            "abort_destroys_before_join",
            abort_destroys_before_join_case().await,
        );
        emit(
            "nested_future_current_poll",
            nested_future_current_poll_case().await,
        );
        emit("paused_sleep_rounding", paused_sleep_rounding_case().await);
        emit(
            "timeout_immediate_zero",
            timeout_immediate_zero_case().await,
        );
        emit("timeout_same_deadline", timeout_same_deadline_case().await);
        emit("timeout_drops_loser", timeout_drops_loser_case().await);
        emit(
            "sleep_reset_after_elapsed",
            sleep_reset_after_elapsed_case().await,
        );
        emit("interval_basic", interval_basic_case().await);
        emit("interval_missed_ticks", interval_missed_ticks_case().await);
        emit("consume_budget_yields", consume_budget_yields_case().await);
        emit(
            "blocking_running_abort_noop",
            blocking_running_abort_noop_case().await,
        );
        emit("blocking_queued_abort", blocking_queued_abort_case().await);
        emit(
            "blocking_paused_inhibits_time",
            blocking_paused_inhibits_time_case().await,
        );
        emit(
            "notify_permit_coalesces",
            notify_permit_coalesces_case().await,
        );
        emit("notify_fifo_lifo", notify_fifo_lifo_case().await);
        emit(
            "notify_waiters_snapshot",
            notify_waiters_snapshot_case().await,
        );
        emit(
            "notify_cancel_transfers",
            notify_cancel_transfers_case().await,
        );
        emit(
            "semaphore_fifo_head_blocking",
            semaphore_fifo_head_blocking_case().await,
        );
        emit(
            "semaphore_cancel_partial",
            semaphore_cancel_partial_case().await,
        );
        emit("semaphore_close", semaphore_close_case().await);
        emit("semaphore_permit_ops", semaphore_permit_ops_case().await);
        emit("mutex_fifo", mutex_fifo_case().await);
        emit(
            "mutex_cancel_transfers",
            mutex_cancel_transfers_case().await,
        );
        emit("mutex_no_poison", mutex_no_poison_case().await);
        emit("mutex_owned_map", mutex_owned_map_case().await);
        emit("mutex_blocking_bridge", mutex_blocking_bridge_case().await);
        emit(
            "rwlock_shared_max_readers",
            rwlock_shared_max_readers_case().await,
        );
        emit(
            "rwlock_writer_priority_fifo",
            rwlock_writer_priority_fifo_case().await,
        );
        emit(
            "rwlock_cancel_partial_writer",
            rwlock_cancel_partial_writer_case().await,
        );
        emit("rwlock_no_poison", rwlock_no_poison_case().await);
        emit("rwlock_owned_mapping", rwlock_owned_mapping_case().await);
        emit(
            "rwlock_atomic_downgrade",
            rwlock_atomic_downgrade_case().await,
        );
        emit(
            "barrier_zero_single_leader",
            barrier_zero_single_leader_case().await,
        );
        emit("barrier_lazy_unpolled", barrier_lazy_unpolled_case().await);
        emit(
            "barrier_reusable_unique_leader",
            barrier_reusable_unique_leader_case().await,
        );
        emit(
            "barrier_cancelled_arrival_retained",
            barrier_cancelled_arrival_retained_case().await,
        );
        emit(
            "once_cell_single_initializer",
            once_cell_single_initializer_case().await,
        );
        emit(
            "once_cell_cancel_retry",
            once_cell_cancel_retry_case().await,
        );
        emit(
            "once_cell_try_error_retry",
            once_cell_try_error_retry_case().await,
        );
        emit(
            "once_cell_clone_independent",
            once_cell_clone_independent_case().await,
        );
        emit(
            "once_cell_debug_format",
            once_cell_debug_format_case().await,
        );
        emit(
            "once_cell_set_error_format",
            once_cell_set_error_format_case().await,
        );
        emit(
            "set_once_wait_unblocks",
            set_once_wait_unblocks_case().await,
        );
        emit(
            "set_once_single_winner_values",
            set_once_single_winner_values_case().await,
        );
        emit("set_once_cancel_safe", set_once_cancel_safe_case().await);
        emit(
            "set_once_clone_independent",
            set_once_clone_independent_case().await,
        );
        emit("oneshot_send_receive", oneshot_send_receive_case().await);
        emit(
            "oneshot_sender_drop_recv_error",
            oneshot_sender_drop_recv_error_case().await,
        );
        emit(
            "oneshot_receiver_drop_returns_value",
            oneshot_receiver_drop_returns_value_case().await,
        );
        emit(
            "oneshot_close_preserves_sent",
            oneshot_close_preserves_sent_case().await,
        );
        emit(
            "oneshot_close_rejects_late_send",
            oneshot_close_rejects_late_send_case().await,
        );
        emit(
            "oneshot_try_recv_empty_closed",
            oneshot_try_recv_empty_closed_case().await,
        );
        emit(
            "oneshot_receive_cancel_safe",
            oneshot_receive_cancel_safe_case().await,
        );
        emit(
            "oneshot_sender_closed_wakes",
            oneshot_sender_closed_wakes_case().await,
        );
        emit(
            "oneshot_empty_terminated_transitions",
            oneshot_empty_terminated_transitions_case().await,
        );
        emit(
            "oneshot_value_drop_once",
            oneshot_value_drop_once_case().await,
        );
        emit(
            "oneshot_ready_budget_yields",
            oneshot_ready_budget_yields_case().await,
        );
        emit(
            "mpsc_fifo_backpressure",
            mpsc_fifo_backpressure_case().await,
        );
        emit(
            "mpsc_send_reserve_fairness",
            mpsc_send_reserve_fairness_case().await,
        );
        emit("mpsc_cancel_send", mpsc_cancel_send_case().await);
        emit("mpsc_cancel_reserve", mpsc_cancel_reserve_case().await);
        emit("mpsc_permit_capacity", mpsc_permit_capacity_case().await);
        emit(
            "mpsc_close_drain_permit",
            mpsc_close_drain_permit_case().await,
        );
        emit("mpsc_try_errors", mpsc_try_errors_case().await);
        emit("mpsc_receiver_drop", mpsc_receiver_drop_case().await);
        emit("mpsc_last_sender_weak", mpsc_last_sender_weak_case().await);
        emit("mpsc_sender_counts", mpsc_sender_counts_case().await);
        emit("mpsc_error_format", mpsc_error_format_case().await);
        emit("mpsc_closed_wakes", mpsc_closed_wakes_case().await);
        emit(
            "mpsc_closed_cancel_safe",
            mpsc_closed_cancel_safe_case().await,
        );
        emit("mpsc_same_channel", mpsc_same_channel_case().await);
        emit(
            "mpsc_receiver_len_empty",
            mpsc_receiver_len_empty_case().await,
        );
        emit(
            "mpsc_try_reserve_errors",
            mpsc_try_reserve_errors_case().await,
        );
        emit(
            "mpsc_owned_permit_send_release",
            mpsc_owned_permit_send_release_case().await,
        );
        emit(
            "mpsc_owned_permit_same_channel",
            mpsc_owned_permit_same_channel_case().await,
        );
        emit(
            "mpsc_owned_permit_lifetime",
            mpsc_owned_permit_lifetime_case().await,
        );
        emit(
            "mpsc_reserve_owned_closed_consumes_sender",
            mpsc_reserve_owned_closed_consumes_sender_case().await,
        );
        emit(
            "mpsc_try_reserve_owned_errors",
            mpsc_try_reserve_owned_errors_case().await,
        );
        emit(
            "mpsc_reserve_owned_cancel_safe",
            mpsc_reserve_owned_cancel_safe_case().await,
        );
        emit(
            "mpsc_unbounded_fifo_multi_sender",
            mpsc_unbounded_fifo_multi_sender_case().await,
        );
        emit(
            "mpsc_unbounded_send_try_errors",
            mpsc_unbounded_send_try_errors_case().await,
        );
        emit(
            "mpsc_unbounded_close_drain",
            mpsc_unbounded_close_drain_case().await,
        );
        emit(
            "mpsc_unbounded_receiver_drop",
            mpsc_unbounded_receiver_drop_case().await,
        );
        emit(
            "mpsc_unbounded_closed_wakes",
            mpsc_unbounded_closed_wakes_case().await,
        );
        emit(
            "mpsc_unbounded_closed_cancel_safe",
            mpsc_unbounded_closed_cancel_safe_case().await,
        );
        emit(
            "mpsc_unbounded_last_sender_weak",
            mpsc_unbounded_last_sender_weak_case().await,
        );
        emit(
            "mpsc_unbounded_same_channel_counts",
            mpsc_unbounded_same_channel_counts_case().await,
        );
        emit(
            "mpsc_unbounded_receiver_len_empty",
            mpsc_unbounded_receiver_len_empty_case().await,
        );
        emit(
            "mpsc_unbounded_ready_recv_budget",
            mpsc_unbounded_ready_recv_budget_case().await,
        );
        emit(
            "mpsc_unbounded_value_drop_once",
            mpsc_unbounded_value_drop_once_case().await,
        );
        emit(
            "mpsc_unbounded_weak_upgrade_closed",
            mpsc_unbounded_weak_upgrade_closed_case().await,
        );
        emit(
            "mpsc_unbounded_recv_cancel_safe",
            mpsc_unbounded_recv_cancel_safe_case().await,
        );
        emit(
            "mpsc_unbounded_noncoop_send_closed",
            mpsc_unbounded_noncoop_send_closed_case().await,
        );
        emit("watch_initial_borrow", watch_initial_borrow_case().await);
        emit(
            "watch_send_changed_borrow_update",
            watch_send_changed_borrow_update_case().await,
        );
        emit(
            "watch_marks_and_has_changed",
            watch_marks_and_has_changed_case().await,
        );
        emit(
            "watch_independent_receivers_subscribe",
            watch_independent_receivers_subscribe_case().await,
        );
        emit(
            "watch_last_sender_close_retains_value",
            watch_last_sender_close_retains_value_case().await,
        );
        emit(
            "watch_last_receiver_closes_sender",
            watch_last_receiver_closes_sender_case().await,
        );
        emit(
            "watch_changed_cancel_safe",
            watch_changed_cancel_safe_case().await,
        );
        emit(
            "watch_same_channel_counts",
            watch_same_channel_counts_case().await,
        );
        emit("watch_send_replace", watch_send_replace_case().await);
        emit("watch_wait_for", watch_wait_for_case().await);
        emit(
            "watch_value_drop_and_clone",
            watch_value_drop_and_clone_case().await,
        );
        emit("watch_error_format", watch_error_format_case().await);
        emit(
            "watch_cooperative_ready_paths",
            watch_cooperative_ready_paths_case().await,
        );
        emit(
            "watch_coop_changed_success_boundary",
            watch_coop_changed_success_boundary_case().await,
        );
        emit(
            "watch_coop_changed_error_boundary",
            watch_coop_changed_error_boundary_case().await,
        );
        emit(
            "watch_coop_closed_boundary",
            watch_coop_closed_boundary_case().await,
        );
        emit(
            "watch_coop_wait_for_success_boundary",
            watch_coop_wait_for_success_boundary_case().await,
        );
        emit(
            "watch_coop_wait_for_error_boundary",
            watch_coop_wait_for_error_boundary_case().await,
        );
        emit(
            "watch_coop_changed_fresh_wake_budget",
            watch_coop_changed_fresh_wake_budget_case().await,
        );
        emit(
            "watch_coop_wait_for_fresh_wake_budget",
            watch_coop_wait_for_fresh_wake_budget_case().await,
        );
        emit(
            "watch_coop_closed_fresh_wake_budget",
            watch_coop_closed_fresh_wake_budget_case().await,
        );
        emit(
            "broadcast_capacity_rounding_lag",
            broadcast_capacity_rounding_lag_case().await,
        );
        emit(
            "broadcast_failed_send_then_subscribe",
            broadcast_failed_send_then_subscribe_case().await,
        );
        emit(
            "broadcast_independent_receivers",
            broadcast_independent_receivers_case().await,
        );
        emit(
            "broadcast_resubscribe_skips_backlog",
            broadcast_resubscribe_skips_backlog_case().await,
        );
        emit(
            "broadcast_drain_then_closed",
            broadcast_drain_then_closed_case().await,
        );
        emit(
            "broadcast_lagged_exact",
            broadcast_lagged_exact_case().await,
        );
        emit(
            "broadcast_try_recv_empty_closed",
            broadcast_try_recv_empty_closed_case().await,
        );
        emit(
            "broadcast_send_receiver_count",
            broadcast_send_receiver_count_case().await,
        );
        emit("broadcast_counts", broadcast_counts_case().await);
        emit(
            "broadcast_weak_upgrade",
            broadcast_weak_upgrade_case().await,
        );
        emit(
            "broadcast_copy_panic_advances_cursor",
            broadcast_copy_panic_advances_cursor_case().await,
        );
        emit(
            "broadcast_recv_cooperative_ready_budget",
            broadcast_recv_cooperative_ready_budget_case().await,
        );
        emit(
            "broadcast_recv_cooperative_pending_budget",
            broadcast_recv_cooperative_pending_budget_case().await,
        );
        emit(
            "broadcast_closed_noncooperative",
            broadcast_closed_noncooperative_case().await,
        );
        emit("io_readbuf_regions_clear", io_readbuf_regions_clear_case());
        emit(
            "io_partial_read_eof_zero_capacity",
            io_partial_read_eof_zero_capacity_case().await,
        );
        emit(
            "io_read_exact_partial_success",
            io_read_exact_partial_success_case().await,
        );
        emit(
            "io_read_exact_early_eof",
            io_read_exact_early_eof_case().await,
        );
        emit(
            "io_read_exact_partial_error",
            io_read_exact_partial_error_case().await,
        );
        emit(
            "io_write_all_partial_zero",
            io_write_all_partial_zero_case().await,
        );
        emit(
            "io_write_all_partial_error",
            io_write_all_partial_error_case().await,
        );
        emit(
            "io_exact_cancel_partial_late_wake",
            io_exact_cancel_partial_late_wake_case().await,
        );
        emit(
            "io_exact_empty_no_poll",
            io_exact_empty_no_poll_case().await,
        );
        emit(
            "io_partial_write_single_attempt",
            io_partial_write_single_attempt_case().await,
        );
        emit("io_write_zero_success", io_write_zero_success_case().await);
        emit(
            "io_write_vectored_default_first_nonempty",
            io_write_vectored_default_first_nonempty_case().await,
        );
        emit(
            "io_flush_shutdown_order",
            io_flush_shutdown_order_case().await,
        );
        emit("io_shutdown_terminal", io_shutdown_terminal_case().await);
        emit(
            "io_ready_ext_noncooperative",
            io_ready_ext_noncooperative_case().await,
        );
    });

    let multi = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .expect("multi-thread runtime");
    emit("multi_spawn_join", multi.block_on(multi_spawn_join_case()));
}
