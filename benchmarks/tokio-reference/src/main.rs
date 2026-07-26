use std::future::{poll_fn, Future};
use std::io::{self, Cursor, Read};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

use tokio::io::AsyncReadExt;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{broadcast, mpsc, oneshot, watch, Mutex, Notify, OnceCell, RwLock, SetOnce};

const BOUNDED_MPSC_CAPACITY: usize = 64;
const BOUNDED_MPSC_CONSUMERS: usize = 1;
const BROADCAST_CAPACITY: usize = 64;
const IO_MEMORY_PAYLOAD_BYTES: usize = 64;
const IO_MEMORY_PAYLOAD_CHECKSUM: u64 = 2016;

struct Config {
    workload: String,
    workers: usize,
    operations: usize,
    warmups: usize,
    samples: usize,
}

fn read_config() -> Result<Config, String> {
    let mut input = String::new();
    io::stdin()
        .read_to_string(&mut input)
        .map_err(|error| format!("读取标准输入失败：{error}"))?;
    let mut fields = input.split_whitespace();
    let workload = fields
        .next()
        .ok_or_else(|| "缺少 workload".to_owned())?
        .to_owned();
    let parse = |field: Option<&str>, name: &str| -> Result<usize, String> {
        field
            .ok_or_else(|| format!("缺少 {name}"))?
            .parse::<usize>()
            .map_err(|error| format!("{name} 不是有效整数：{error}"))
    };
    let workers = parse(fields.next(), "workers")?;
    let operations = parse(fields.next(), "operations")?;
    let warmups = parse(fields.next(), "warmups")?;
    let samples = parse(fields.next(), "samples")?;

    if workers == 0 || operations == 0 || samples == 0 {
        return Err("workers、operations 和 samples 必须大于零".to_owned());
    }
    if !matches!(
        workload.as_str(),
        "schedule"
            | "yield"
            | "mutex"
            | "rwlock_read"
            | "rwlock_write"
            | "rwlock_mixed"
            | "once_cell_ready"
            | "once_cell_init"
            | "set_once_fanout"
            | "oneshot_wake"
            | "mpsc_bounded"
            | "watch_fanout"
            | "broadcast_fanout"
            | "io_memory_ready"
    ) {
        return Err("未知 benchmark workload".to_owned());
    }

    Ok(Config {
        workload,
        workers,
        operations,
        warmups,
        samples,
    })
}

fn build_runtime(workers: usize) -> Result<Runtime, String> {
    if workers == 1 {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("创建 current-thread runtime 失败：{error}"))
    } else {
        Builder::new_multi_thread()
            .worker_threads(workers)
            .enable_all()
            .build()
            .map_err(|error| format!("创建 multi-thread runtime 失败：{error}"))
    }
}

fn partitioned_task_count(operations: usize, workers: usize) -> usize {
    usize::min(operations, usize::max(2, workers * 4))
}

fn bounded_mpsc_producer_count(operations: usize, workers: usize) -> usize {
    partitioned_task_count(operations, workers)
}

fn watch_subscriber_count(operations: usize, workers: usize) -> usize {
    usize::max(1, usize::min(operations, workers * 4))
}

fn broadcast_subscriber_count(operations: usize, workers: usize) -> usize {
    partitioned_task_count(operations, workers)
}

async fn run_schedule(operations: usize) {
    let completed = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(Notify::new());
    for _ in 0..operations {
        let child_completed = Arc::clone(&completed);
        let child_notify = Arc::clone(&notify);
        tokio::spawn(async move {
            let observed = child_completed.fetch_add(1, Ordering::AcqRel) + 1;
            if observed == operations {
                child_notify.notify_one();
            }
        });
    }
    notify.notified().await;
    assert_eq!(completed.load(Ordering::Acquire), operations);
}

async fn run_yield(operations: usize) {
    for _ in 0..operations {
        tokio::task::yield_now().await;
    }
}

async fn run_io_memory_ready(operations: usize, workers: usize) {
    let expected_bytes_usize = operations
        .checked_mul(IO_MEMORY_PAYLOAD_BYTES)
        .expect("io_memory_ready bytes 溢出 usize");
    let expected_bytes =
        u64::try_from(expected_bytes_usize).expect("io_memory_ready bytes 超出 u64");
    let expected_checksum = u64::try_from(operations)
        .expect("io_memory_ready operations 超出 u64")
        .checked_mul(IO_MEMORY_PAYLOAD_CHECKSUM)
        .expect("io_memory_ready checksum 溢出 u64");
    let task_count = partitioned_task_count(operations, workers);
    let base_operations = operations / task_count;
    let remainder = operations % task_count;
    let total_calls = Arc::new(AtomicUsize::new(0));
    let total_bytes = Arc::new(AtomicU64::new(0));
    let total_checksum = Arc::new(AtomicU64::new(0));
    let failures = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(Notify::new());

    for index in 0..task_count {
        let child_operations = base_operations + usize::from(index < remainder);
        let child_total_calls = Arc::clone(&total_calls);
        let child_total_bytes = Arc::clone(&total_bytes);
        let child_total_checksum = Arc::clone(&total_checksum);
        let child_failures = Arc::clone(&failures);
        let child_completed = Arc::clone(&completed);
        let child_notify = Arc::clone(&notify);
        tokio::spawn(async move {
            let mut source = Vec::with_capacity(child_operations * IO_MEMORY_PAYLOAD_BYTES);
            for _ in 0..child_operations {
                source.extend(0_u8..IO_MEMORY_PAYLOAD_BYTES as u8);
            }
            let mut reader = Cursor::new(source);
            let mut buffer = [0_u8; IO_MEMORY_PAYLOAD_BYTES];
            let mut local_calls = 0_usize;
            let mut local_bytes = 0_u64;
            let mut local_checksum = 0_u64;

            for _ in 0..child_operations {
                let read = AsyncReadExt::read(&mut reader, &mut buffer).await;
                if !read.is_ok_and(|value| value == IO_MEMORY_PAYLOAD_BYTES) {
                    child_failures.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                let snapshot = buffer.to_vec();
                local_calls += 1;
                local_bytes += snapshot.len() as u64;
                local_checksum += snapshot.iter().map(|value| u64::from(*value)).sum::<u64>();
            }

            child_total_calls.fetch_add(local_calls, Ordering::Relaxed);
            child_total_bytes.fetch_add(local_bytes, Ordering::Relaxed);
            child_total_checksum.fetch_add(local_checksum, Ordering::Relaxed);
            let observed = child_completed.fetch_add(1, Ordering::AcqRel) + 1;
            if observed == task_count {
                child_notify.notify_one();
            }
        });
    }

    notify.notified().await;
    assert_eq!(completed.load(Ordering::Acquire), task_count);
    assert_eq!(failures.load(Ordering::Relaxed), 0);
    assert_eq!(total_calls.load(Ordering::Relaxed), operations);
    assert_eq!(total_bytes.load(Ordering::Relaxed), expected_bytes);
    assert_eq!(total_checksum.load(Ordering::Relaxed), expected_checksum);
}

async fn run_once_cell_ready(operations: usize, workers: usize) {
    let task_count = partitioned_task_count(operations, workers);
    let base_operations = operations / task_count;
    let remainder = operations % task_count;

    let cell = Arc::new(OnceCell::new_with(Some(1_usize)));
    let checksum = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(Notify::new());

    for index in 0..task_count {
        let child_cell = Arc::clone(&cell);
        let child_checksum = Arc::clone(&checksum);
        let child_completed = Arc::clone(&completed);
        let child_notify = Arc::clone(&notify);
        let child_operations = base_operations + usize::from(index < remainder);
        tokio::spawn(async move {
            for _ in 0..child_operations {
                let value = child_cell.get().expect("once_cell_ready 观察到空 cell");
                child_checksum.fetch_add(*value, Ordering::Relaxed);
            }
            let observed = child_completed.fetch_add(1, Ordering::AcqRel) + 1;
            if observed == task_count {
                child_notify.notify_one();
            }
        });
    }

    notify.notified().await;
    assert_eq!(completed.load(Ordering::Acquire), task_count);
    assert_eq!(checksum.load(Ordering::Relaxed), operations);
}

async fn run_once_cell_init(operations: usize) {
    let cell = Arc::new(OnceCell::new());
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let failures = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(Notify::new());

    for _ in 0..operations {
        let child_cell = Arc::clone(&cell);
        let child_factory_calls = Arc::clone(&factory_calls);
        let child_failures = Arc::clone(&failures);
        let child_completed = Arc::clone(&completed);
        let child_notify = Arc::clone(&notify);
        tokio::spawn(async move {
            let value = child_cell
                .get_or_init(|| async move {
                    child_factory_calls.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                    1_usize
                })
                .await;
            if *value != 1 {
                child_failures.fetch_add(1, Ordering::Relaxed);
            }
            let observed = child_completed.fetch_add(1, Ordering::AcqRel) + 1;
            if observed == operations {
                child_notify.notify_one();
            }
        });
    }

    notify.notified().await;
    assert_eq!(cell.get(), Some(&1));
    assert_eq!(factory_calls.load(Ordering::Relaxed), 1);
    assert_eq!(failures.load(Ordering::Relaxed), 0);
    assert_eq!(completed.load(Ordering::Acquire), operations);
}

async fn run_set_once_fanout(operations: usize) {
    let set_once = Arc::new(SetOnce::new());
    let entered = Arc::new(AtomicUsize::new(0));
    let failures = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(Notify::new());

    for _ in 0..operations {
        let child_set_once = Arc::clone(&set_once);
        let child_entered = Arc::clone(&entered);
        let child_failures = Arc::clone(&failures);
        let child_completed = Arc::clone(&completed);
        let child_notify = Arc::clone(&notify);
        tokio::spawn(async move {
            let mut wait = std::pin::pin!(child_set_once.wait());
            let mut registered = false;
            let value = poll_fn(|context| {
                let result = wait.as_mut().poll(context);
                if result == Poll::Pending && !registered {
                    child_entered.fetch_add(1, Ordering::Release);
                    registered = true;
                }
                result
            })
            .await;
            if *value != 1 {
                child_failures.fetch_add(1, Ordering::Relaxed);
            }
            let observed = child_completed.fetch_add(1, Ordering::AcqRel) + 1;
            if observed == operations {
                child_notify.notify_one();
            }
        });
    }

    while entered.load(Ordering::Acquire) != operations {
        tokio::task::yield_now().await;
    }
    assert!(set_once.set(1).is_ok());

    notify.notified().await;
    assert_eq!(failures.load(Ordering::Relaxed), 0);
    assert_eq!(completed.load(Ordering::Acquire), operations);
}

async fn run_oneshot_wake(operations: usize) {
    let entered = Arc::new(AtomicUsize::new(0));
    let failures = Arc::new(AtomicUsize::new(0));
    let checksum = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(Notify::new());
    let mut senders = Vec::with_capacity(operations);

    for index in 0..operations {
        let (sender, mut receiver) = oneshot::channel();
        senders.push(sender);
        let child_entered = Arc::clone(&entered);
        let child_failures = Arc::clone(&failures);
        let child_checksum = Arc::clone(&checksum);
        let child_completed = Arc::clone(&completed);
        let child_notify = Arc::clone(&notify);
        tokio::spawn(async move {
            let mut registered = false;
            let received = poll_fn(|context| {
                let result = std::pin::Pin::new(&mut receiver).poll(context);
                if result == Poll::Pending && !registered {
                    child_entered.fetch_add(1, Ordering::Release);
                    registered = true;
                }
                result
            })
            .await;

            match received {
                Ok(value) if value == index + 1 => {
                    child_checksum.fetch_add(value, Ordering::Relaxed);
                }
                _ => {
                    child_failures.fetch_add(1, Ordering::Relaxed);
                }
            }
            let observed = child_completed.fetch_add(1, Ordering::AcqRel) + 1;
            if observed == operations {
                child_notify.notify_one();
            }
        });
    }

    while entered.load(Ordering::Acquire) != operations {
        tokio::task::yield_now().await;
    }

    for (index, sender) in senders.into_iter().enumerate() {
        assert_eq!(sender.send(index + 1), Ok(()));
    }

    notify.notified().await;
    let expected_checksum = if operations % 2 == 0 {
        (operations / 2) * (operations + 1)
    } else {
        operations * ((operations + 1) / 2)
    };
    assert_eq!(entered.load(Ordering::Acquire), operations);
    assert_eq!(completed.load(Ordering::Acquire), operations);
    assert_eq!(failures.load(Ordering::Relaxed), 0);
    assert_eq!(checksum.load(Ordering::Relaxed), expected_checksum);
}

async fn run_mpsc_bounded(operations: usize, workers: usize) {
    let producer_count = bounded_mpsc_producer_count(operations, workers);
    let base_operations = operations / producer_count;
    let remainder = operations % producer_count;
    let task_count = producer_count + BOUNDED_MPSC_CONSUMERS;

    let failures = Arc::new(AtomicUsize::new(0));
    let received_count = Arc::new(AtomicUsize::new(0));
    let checksum = Arc::new(AtomicU64::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(Notify::new());

    {
        let (sender, mut receiver) = mpsc::channel::<u64>(BOUNDED_MPSC_CAPACITY);
        let child_received_count = Arc::clone(&received_count);
        let child_checksum = Arc::clone(&checksum);
        let child_completed = Arc::clone(&completed);
        let child_notify = Arc::clone(&notify);
        tokio::spawn(async move {
            let mut local_count = 0_usize;
            let mut local_checksum = 0_u64;
            while let Some(value) = receiver.recv().await {
                local_count += 1;
                local_checksum = local_checksum.wrapping_add(value);
            }
            child_received_count.store(local_count, Ordering::Relaxed);
            child_checksum.store(local_checksum, Ordering::Relaxed);
            let observed = child_completed.fetch_add(1, Ordering::AcqRel) + 1;
            if observed == task_count {
                child_notify.notify_one();
            }
        });

        let mut first_operation = 0_usize;
        for index in 0..producer_count {
            let child_sender = sender.clone();
            let child_failures = Arc::clone(&failures);
            let child_completed = Arc::clone(&completed);
            let child_notify = Arc::clone(&notify);
            let child_operations = base_operations + usize::from(index < remainder);
            let child_first_operation = first_operation;
            tokio::spawn(async move {
                for child_index in 0..child_operations {
                    let value = u64::try_from(child_first_operation + child_index + 1)
                        .expect("mpsc_bounded 消息编号超出 u64");
                    if child_sender.send(value).await.is_err() {
                        child_failures.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
                let observed = child_completed.fetch_add(1, Ordering::AcqRel) + 1;
                if observed == task_count {
                    child_notify.notify_one();
                }
            });
            first_operation += child_operations;
        }
    }

    notify.notified().await;
    let count = u64::try_from(operations).expect("mpsc_bounded 操作数超出 u64");
    let expected_checksum = if count % 2 == 0 {
        (count / 2).wrapping_mul(count + 1)
    } else {
        count.wrapping_mul((count + 1) / 2)
    };
    assert_eq!(failures.load(Ordering::Relaxed), 0);
    assert_eq!(received_count.load(Ordering::Relaxed), operations);
    assert_eq!(checksum.load(Ordering::Relaxed), expected_checksum);
    assert_eq!(completed.load(Ordering::Acquire), task_count);
}

async fn run_watch_fanout(operations: usize, workers: usize) {
    let subscriber_count = watch_subscriber_count(operations, workers);
    let expected_deliveries = operations
        .checked_mul(subscriber_count)
        .expect("watch_fanout deliveries 溢出 usize");

    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let failures = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let acknowledgement_notify = Arc::new(Notify::new());
    let completion_notify = Arc::new(Notify::new());
    let (sender, receiver) = watch::channel(0_usize);

    for _ in 0..subscriber_count {
        let mut child_receiver = receiver.clone();
        let child_acknowledgements = Arc::clone(&acknowledgements);
        let child_failures = Arc::clone(&failures);
        let child_completed = Arc::clone(&completed);
        let child_acknowledgement_notify = Arc::clone(&acknowledgement_notify);
        let child_completion_notify = Arc::clone(&completion_notify);
        tokio::spawn(async move {
            for expected in 1..=operations {
                if child_receiver.changed().await.is_err() || *child_receiver.borrow() != expected {
                    child_failures.fetch_add(1, Ordering::Release);
                    child_acknowledgement_notify.notify_one();
                    break;
                }

                let observed = child_acknowledgements.fetch_add(1, Ordering::AcqRel) + 1;
                if observed % subscriber_count == 0 {
                    child_acknowledgement_notify.notify_one();
                }
            }

            let observed = child_completed.fetch_add(1, Ordering::AcqRel) + 1;
            if observed == subscriber_count {
                child_completion_notify.notify_one();
            }
        });
    }
    drop(receiver);

    for version in 1..=operations {
        sender
            .send(version)
            .expect("watch_fanout 发布时 Receiver 意外关闭");
        let target = version
            .checked_mul(subscriber_count)
            .expect("watch_fanout ack 目标溢出 usize");
        loop {
            let notified = acknowledgement_notify.notified();
            assert_eq!(failures.load(Ordering::Acquire), 0);
            if acknowledgements.load(Ordering::Acquire) == target {
                break;
            }
            notified.await;
        }
    }

    loop {
        let notified = completion_notify.notified();
        if completed.load(Ordering::Acquire) == subscriber_count {
            break;
        }
        notified.await;
    }

    assert_eq!(failures.load(Ordering::Acquire), 0);
    assert_eq!(
        acknowledgements.load(Ordering::Acquire),
        expected_deliveries
    );
    assert_eq!(completed.load(Ordering::Acquire), subscriber_count);
}

async fn run_broadcast_fanout(operations: usize, workers: usize) {
    let subscriber_count = broadcast_subscriber_count(operations, workers);
    let expected_deliveries = operations
        .checked_mul(subscriber_count)
        .expect("broadcast_fanout deliveries 溢出 usize");

    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let failures = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let acknowledgement_notify = Arc::new(Notify::new());
    let completion_notify = Arc::new(Notify::new());
    let (sender, receiver) = broadcast::channel::<u64>(BROADCAST_CAPACITY);

    let mut receivers = Vec::with_capacity(subscriber_count);
    receivers.push(receiver);
    for _ in 1..subscriber_count {
        receivers.push(sender.subscribe());
    }
    for mut child_receiver in receivers {
        let child_acknowledgements = Arc::clone(&acknowledgements);
        let child_failures = Arc::clone(&failures);
        let child_completed = Arc::clone(&completed);
        let child_acknowledgement_notify = Arc::clone(&acknowledgement_notify);
        let child_completion_notify = Arc::clone(&completion_notify);
        tokio::spawn(async move {
            for expected in 1..=operations {
                match child_receiver.recv().await {
                    Ok(value) if value == expected as u64 => {}
                    _ => {
                        child_failures.fetch_add(1, Ordering::Release);
                        child_acknowledgement_notify.notify_one();
                        break;
                    }
                }

                let observed = child_acknowledgements.fetch_add(1, Ordering::AcqRel) + 1;
                if observed % subscriber_count == 0 {
                    child_acknowledgement_notify.notify_one();
                }
            }

            let observed = child_completed.fetch_add(1, Ordering::AcqRel) + 1;
            if observed == subscriber_count {
                child_completion_notify.notify_one();
            }
        });
    }

    for value in 1..=operations {
        assert_eq!(
            sender
                .send(value as u64)
                .expect("broadcast_fanout 发布时 Receiver 意外关闭"),
            subscriber_count
        );
        let target = value
            .checked_mul(subscriber_count)
            .expect("broadcast_fanout ack 目标溢出 usize");
        loop {
            let notified = acknowledgement_notify.notified();
            assert_eq!(failures.load(Ordering::Acquire), 0);
            if acknowledgements.load(Ordering::Acquire) == target {
                break;
            }
            notified.await;
        }
    }

    loop {
        let notified = completion_notify.notified();
        if completed.load(Ordering::Acquire) == subscriber_count {
            break;
        }
        notified.await;
    }

    assert_eq!(failures.load(Ordering::Acquire), 0);
    assert_eq!(
        acknowledgements.load(Ordering::Acquire),
        expected_deliveries
    );
    assert_eq!(completed.load(Ordering::Acquire), subscriber_count);
}

async fn run_mutex(operations: usize, workers: usize) {
    let task_count = partitioned_task_count(operations, workers);
    let base_operations = operations / task_count;
    let remainder = operations % task_count;

    let mutex = Arc::new(Mutex::new(0_u64));
    let completed = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(Notify::new());

    for index in 0..task_count {
        let child_mutex = Arc::clone(&mutex);
        let child_completed = Arc::clone(&completed);
        let child_notify = Arc::clone(&notify);
        let child_operations = base_operations + usize::from(index < remainder);
        tokio::spawn(async move {
            for _ in 0..child_operations {
                {
                    let mut guard = child_mutex.lock().await;
                    tokio::task::yield_now().await;
                    *guard += 1;
                }
                tokio::task::yield_now().await;
            }
            let observed = child_completed.fetch_add(1, Ordering::AcqRel) + 1;
            if observed == task_count {
                child_notify.notify_one();
            }
        });
    }

    notify.notified().await;
    assert_eq!(*mutex.lock().await, operations as u64);
}

async fn run_rwlock(workload: &str, operations: usize, workers: usize) {
    let task_count = partitioned_task_count(operations, workers);
    let base_operations = operations / task_count;
    let remainder = operations % task_count;

    let rwlock = Arc::new(RwLock::new(if workload == "rwlock_read" {
        1_u64
    } else {
        0_u64
    }));
    let checksum = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(Notify::new());
    let mut first_operation = 0_usize;

    for index in 0..task_count {
        let child_rwlock = Arc::clone(&rwlock);
        let child_checksum = Arc::clone(&checksum);
        let child_completed = Arc::clone(&completed);
        let child_notify = Arc::clone(&notify);
        let child_operations = base_operations + usize::from(index < remainder);
        let child_first_operation = first_operation;
        let child_workload = workload.to_owned();
        tokio::spawn(async move {
            for child_index in 0..child_operations {
                let global_index = child_first_operation + child_index;
                match child_workload.as_str() {
                    "rwlock_read" => {
                        let guard = child_rwlock.read().await;
                        tokio::task::yield_now().await;
                        child_checksum.fetch_add(*guard as usize, Ordering::Relaxed);
                    }
                    "rwlock_write" => {
                        let mut guard = child_rwlock.write().await;
                        tokio::task::yield_now().await;
                        *guard += 1;
                    }
                    "rwlock_mixed" if global_index % 5 == 0 => {
                        let mut guard = child_rwlock.write().await;
                        tokio::task::yield_now().await;
                        *guard += 1;
                    }
                    "rwlock_mixed" => {
                        let guard = child_rwlock.read().await;
                        tokio::task::yield_now().await;
                        child_checksum.fetch_add(*guard as usize, Ordering::Relaxed);
                    }
                    _ => unreachable!(),
                }
                tokio::task::yield_now().await;
            }
            let observed = child_completed.fetch_add(1, Ordering::AcqRel) + 1;
            if observed == task_count {
                child_notify.notify_one();
            }
        });
        first_operation += child_operations;
    }

    notify.notified().await;
    let final_value = *rwlock.read().await;
    let expected_writes = match workload {
        "rwlock_write" => operations,
        "rwlock_mixed" => (operations + 4) / 5,
        _ => 0,
    };
    assert_eq!(
        final_value,
        if workload == "rwlock_read" {
            1
        } else {
            expected_writes as u64
        }
    );
    if workload == "rwlock_read" {
        assert_eq!(checksum.load(Ordering::Relaxed), operations);
    }
}

fn run_once(runtime: &Runtime, config: &Config) {
    match config.workload.as_str() {
        "schedule" => {
            let operations = config.operations;
            runtime.block_on(async move {
                tokio::spawn(run_schedule(operations))
                    .await
                    .expect("schedule 控制 task 失败");
            });
        }
        "yield" => {
            let operations = config.operations;
            runtime.block_on(async move {
                tokio::spawn(run_yield(operations))
                    .await
                    .expect("yield 控制 task 失败");
            });
        }
        "io_memory_ready" => {
            let operations = config.operations;
            let workers = config.workers;
            runtime.block_on(async move {
                tokio::spawn(run_io_memory_ready(operations, workers))
                    .await
                    .expect("io_memory_ready 控制 task 失败");
            });
        }
        "once_cell_ready" => {
            let operations = config.operations;
            let workers = config.workers;
            runtime.block_on(async move {
                tokio::spawn(run_once_cell_ready(operations, workers))
                    .await
                    .expect("once_cell_ready 控制 task 失败");
            });
        }
        "once_cell_init" => {
            let operations = config.operations;
            runtime.block_on(async move {
                tokio::spawn(run_once_cell_init(operations))
                    .await
                    .expect("once_cell_init 控制 task 失败");
            });
        }
        "set_once_fanout" => {
            let operations = config.operations;
            runtime.block_on(async move {
                tokio::spawn(run_set_once_fanout(operations))
                    .await
                    .expect("set_once_fanout 控制 task 失败");
            });
        }
        "oneshot_wake" => {
            let operations = config.operations;
            runtime.block_on(async move {
                tokio::spawn(run_oneshot_wake(operations))
                    .await
                    .expect("oneshot_wake 控制 task 失败");
            });
        }
        "mpsc_bounded" => {
            let operations = config.operations;
            let workers = config.workers;
            runtime.block_on(async move {
                tokio::spawn(run_mpsc_bounded(operations, workers))
                    .await
                    .expect("mpsc_bounded 控制 task 失败");
            });
        }
        "watch_fanout" => {
            let operations = config.operations;
            let workers = config.workers;
            runtime.block_on(async move {
                tokio::spawn(run_watch_fanout(operations, workers))
                    .await
                    .expect("watch_fanout 控制 task 失败");
            });
        }
        "broadcast_fanout" => {
            let operations = config.operations;
            let workers = config.workers;
            runtime.block_on(async move {
                tokio::spawn(run_broadcast_fanout(operations, workers))
                    .await
                    .expect("broadcast_fanout 控制 task 失败");
            });
        }
        "mutex" => {
            let operations = config.operations;
            let workers = config.workers;
            runtime.block_on(async move {
                tokio::spawn(run_mutex(operations, workers))
                    .await
                    .expect("mutex 控制 task 失败");
            });
        }
        "rwlock_read" | "rwlock_write" | "rwlock_mixed" => {
            let workload = config.workload.clone();
            let operations = config.operations;
            let workers = config.workers;
            runtime.block_on(async move {
                tokio::spawn(async move {
                    run_rwlock(&workload, operations, workers).await;
                })
                .await
                .expect("rwlock 控制 task 失败");
            });
        }
        _ => unreachable!(),
    }
}

fn measure(runtime: &Runtime, config: &Config) -> Vec<u64> {
    let total_runs = config.warmups + config.samples;
    let mut samples = Vec::with_capacity(config.samples);
    for index in 0..total_runs {
        let started = Instant::now();
        run_once(runtime, config);
        let elapsed = started.elapsed().as_nanos();
        if index >= config.warmups {
            samples.push(u64::try_from(elapsed).expect("单次 benchmark 时间超出 u64"));
        }
    }
    samples
}

fn write_result(config: &Config, samples: &[u64]) {
    let task_count = match config.workload.as_str() {
        "schedule" | "once_cell_init" | "set_once_fanout" | "oneshot_wake" => config.operations,
        "yield" => 1,
        "mpsc_bounded" => {
            bounded_mpsc_producer_count(config.operations, config.workers) + BOUNDED_MPSC_CONSUMERS
        }
        "watch_fanout" => watch_subscriber_count(config.operations, config.workers),
        "broadcast_fanout" => broadcast_subscriber_count(config.operations, config.workers),
        _ => partitioned_task_count(config.operations, config.workers),
    };
    print!(
        "{{\"runtime\":\"tokio\",\"runtime_version\":\"1.53.1\",\
         \"build_mode\":\"release\",\
         \"runtime_type\":\"{}\",\
         \"workload\":\"{}\",\"workers\":{},\"tasks\":{},\"operations\":{},\
         \"warmups\":{}",
        if config.workers == 1 {
            "current_thread"
        } else {
            "multi_thread"
        },
        config.workload,
        config.workers,
        task_count,
        config.operations,
        config.warmups
    );
    if config.workload == "mpsc_bounded" {
        print!(
            ",\"channel_capacity\":{},\"producers\":{},\"consumers\":{}",
            BOUNDED_MPSC_CAPACITY,
            bounded_mpsc_producer_count(config.operations, config.workers),
            BOUNDED_MPSC_CONSUMERS
        );
    }
    if config.workload == "watch_fanout" {
        let subscribers = watch_subscriber_count(config.operations, config.workers);
        let deliveries = config
            .operations
            .checked_mul(subscribers)
            .expect("watch_fanout deliveries 溢出 usize");
        print!(",\"subscribers\":{subscribers},\"deliveries\":{deliveries}");
    }
    if config.workload == "broadcast_fanout" {
        let subscribers = broadcast_subscriber_count(config.operations, config.workers);
        let deliveries = config
            .operations
            .checked_mul(subscribers)
            .expect("broadcast_fanout deliveries 溢出 usize");
        print!(
            ",\"channel_capacity\":{BROADCAST_CAPACITY},\
             \"subscribers\":{subscribers},\"deliveries\":{deliveries}"
        );
    }
    if config.workload == "io_memory_ready" {
        let bytes = config
            .operations
            .checked_mul(IO_MEMORY_PAYLOAD_BYTES)
            .expect("io_memory_ready bytes 溢出 usize");
        print!(",\"bytes\":{bytes},\"payload_bytes\":{IO_MEMORY_PAYLOAD_BYTES}");
    }
    print!(",\"samples_ns\":[");
    for (index, sample) in samples.iter().enumerate() {
        if index != 0 {
            print!(",");
        }
        print!("{sample}");
    }
    println!("]}}");
}

fn main() {
    let result = (|| -> Result<(), String> {
        let config = read_config()?;
        let runtime = build_runtime(config.workers)?;
        let samples = measure(&runtime, &config);
        write_result(&config, &samples);
        Ok(())
    })();

    if let Err(error) = result {
        eprintln!("Tokio benchmark 失败：{error}");
        std::process::exit(1);
    }
}
