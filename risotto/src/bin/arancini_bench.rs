#[path = "../serializer.rs"]
mod serializer;
#[path = "../update_capnp.rs"]
mod update_capnp;

use bytes::BytesMut;
use chrono::Utc;
use core_affinity::CoreId;
use risotto_lib::state::State;
use risotto_lib::state_store::memory::MemoryStore;
use risotto_lib::update::{map_to_ipv6, Update, UpdateAttributes};
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
struct AllocSnapshot {
    alloc_calls: u64,
    dealloc_calls: u64,
    realloc_calls: u64,
    alloc_bytes: u64,
    dealloc_bytes: u64,
}

impl AllocSnapshot {
    fn delta(self, earlier: Self) -> Self {
        Self {
            alloc_calls: self.alloc_calls.saturating_sub(earlier.alloc_calls),
            dealloc_calls: self.dealloc_calls.saturating_sub(earlier.dealloc_calls),
            realloc_calls: self.realloc_calls.saturating_sub(earlier.realloc_calls),
            alloc_bytes: self.alloc_bytes.saturating_sub(earlier.alloc_bytes),
            dealloc_bytes: self.dealloc_bytes.saturating_sub(earlier.dealloc_bytes),
        }
    }
}

struct CountingAllocator;

static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static DEALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static REALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static ALLOC_TRACKING_ENABLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ALLOC_TRACKING_ENABLED.load(Ordering::Relaxed) {
            ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ALLOC_TRACKING_ENABLED.load(Ordering::Relaxed) {
            DEALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
            DEALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if ALLOC_TRACKING_ENABLED.load(Ordering::Relaxed) {
            REALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
            DEALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

fn alloc_snapshot() -> AllocSnapshot {
    AllocSnapshot {
        alloc_calls: ALLOC_CALLS.load(Ordering::Relaxed),
        dealloc_calls: DEALLOC_CALLS.load(Ordering::Relaxed),
        realloc_calls: REALLOC_CALLS.load(Ordering::Relaxed),
        alloc_bytes: ALLOC_BYTES.load(Ordering::Relaxed),
        dealloc_bytes: DEALLOC_BYTES.load(Ordering::Relaxed),
    }
}

#[derive(Debug)]
struct BenchRun {
    label: &'static str,
    elapsed: Duration,
    alloc: AllocSnapshot,
    updates: u64,
}

#[derive(Debug)]
struct ThroughputPoint {
    workers: usize,
    elapsed: Duration,
    total_updates: u64,
    updates_per_sec: f64,
    efficiency_vs_single: f64,
}

fn sample_update(
    worker: usize,
    seq: u32,
    announced: bool,
    attrs: Arc<UpdateAttributes>,
    timestamp: chrono::DateTime<Utc>,
) -> Update {
    let router = IpAddr::V4(Ipv4Addr::new(10, 0, (worker % 250) as u8, 1));
    let peer = IpAddr::V4(Ipv4Addr::new(198, 51, 100, (worker % 250) as u8 + 1));
    let prefix = IpAddr::V4(Ipv4Addr::from(0x0a00_0000u32.wrapping_add(seq)));
    Update {
        time_received_ns: timestamp,
        time_bmp_header_ns: timestamp,
        router_addr: map_to_ipv6(router),
        router_port: 4000,
        peer_addr: map_to_ipv6(peer),
        peer_bgp_id: Ipv4Addr::new(203, 0, 113, 1),
        peer_asn: 64512 + worker as u32,
        prefix_addr: map_to_ipv6(prefix),
        prefix_len: 24,
        is_post_policy: false,
        is_adj_rib_out: false,
        announced,
        synthetic: false,
        attrs,
    }
}

fn run_serialization_alloc_bench(iterations: u32) -> Vec<BenchRun> {
    let mut cases = Vec::new();
    ALLOC_TRACKING_ENABLED.store(true, Ordering::Relaxed);
    let shared_attrs = Arc::new(UpdateAttributes::default());
    let ts = Utc::now();
    let updates: Vec<Update> = (0..iterations)
        .map(|i| sample_update(0, i, true, Arc::clone(&shared_attrs), ts))
        .collect();

    let start_alloc = alloc_snapshot();
    let start = Instant::now();
    let mut count = 0_u64;
    for update in &updates {
        let payload = serializer::serialize_update(update);
        black_box(payload.len());
        count += 1;
    }
    cases.push(BenchRun {
        label: "serialize_update_alloc_per_call",
        elapsed: start.elapsed(),
        alloc: alloc_snapshot().delta(start_alloc),
        updates: count,
    });

    let start_alloc = alloc_snapshot();
    let start = Instant::now();
    let mut count = 0_u64;
    let mut output = Vec::with_capacity(1024);
    for update in &updates {
        serializer::serialize_update_into(update, &mut output);
        black_box(output.len());
        count += 1;
    }
    cases.push(BenchRun {
        label: "serialize_update_into_reused_buffer",
        elapsed: start.elapsed(),
        alloc: alloc_snapshot().delta(start_alloc),
        updates: count,
    });
    ALLOC_TRACKING_ENABLED.store(false, Ordering::Relaxed);

    cases
}

fn build_bmp_packet(payload_len: usize) -> Vec<u8> {
    let len = 6 + payload_len;
    let mut packet = Vec::with_capacity(len);
    packet.push(3);
    packet.extend_from_slice(&(len as u32).to_be_bytes());
    packet.push(0);
    packet.extend(std::iter::repeat_n(0_u8, payload_len));
    packet
}

fn build_framing_input(frame_count: usize) -> Vec<u8> {
    let mut all = Vec::with_capacity(frame_count * 320);
    for i in 0..frame_count {
        let payload_len = 128 + (i % 256);
        let packet = build_bmp_packet(payload_len);
        all.extend_from_slice(&packet);
    }
    all
}

fn run_framing_alloc_bench(frame_count: usize) -> Vec<BenchRun> {
    let input = build_framing_input(frame_count);
    let mut cases = Vec::new();
    ALLOC_TRACKING_ENABLED.store(true, Ordering::Relaxed);

    let start_alloc = alloc_snapshot();
    let start = Instant::now();
    let mut cursor = 0usize;
    let mut parsed = 0_u64;
    while cursor + 6 <= input.len() {
        let packet_len = u32::from_be_bytes([
            input[cursor + 1],
            input[cursor + 2],
            input[cursor + 3],
            input[cursor + 4],
        ]) as usize;
        if cursor + packet_len > input.len() {
            break;
        }
        let mut packet = vec![0u8; packet_len];
        packet.copy_from_slice(&input[cursor..cursor + packet_len]);
        black_box(packet[5]);
        cursor += packet_len;
        parsed += 1;
    }
    cases.push(BenchRun {
        label: "legacy_peek_and_alloc_per_frame",
        elapsed: start.elapsed(),
        alloc: alloc_snapshot().delta(start_alloc),
        updates: parsed,
    });

    let start_alloc = alloc_snapshot();
    let start = Instant::now();
    let mut parsed = 0_u64;
    let mut frame_buffer = BytesMut::with_capacity(64 * 1024);
    let mut offset = 0usize;
    let chunk_size = 1500usize;
    while offset < input.len() {
        let end = (offset + chunk_size).min(input.len());
        frame_buffer.extend_from_slice(&input[offset..end]);
        offset = end;

        loop {
            if frame_buffer.len() < 6 {
                break;
            }
            let packet_len = u32::from_be_bytes([
                frame_buffer[1],
                frame_buffer[2],
                frame_buffer[3],
                frame_buffer[4],
            ]) as usize;
            if frame_buffer.len() < packet_len {
                break;
            }
            let packet = frame_buffer.split_to(packet_len).freeze();
            black_box(packet[5]);
            parsed += 1;
        }
    }
    cases.push(BenchRun {
        label: "framed_reusable_buffer_split_to",
        elapsed: start.elapsed(),
        alloc: alloc_snapshot().delta(start_alloc),
        updates: parsed,
    });
    ALLOC_TRACKING_ENABLED.store(false, Ordering::Relaxed);

    cases
}

fn worker_hot_path(worker_id: usize, updates_per_worker: u32) -> u64 {
    let mut state = State::new(MemoryStore::new());
    let mut out = Vec::with_capacity(1024);
    let mut emitted = 0_u64;
    let shared_attrs = Arc::new(UpdateAttributes::default());
    let ts = Utc::now();

    for i in 0..updates_per_worker {
        // Toggle announce/withdraw on the same prefix key sequence so state size stays bounded.
        let announced = i % 2 == 0;
        let key = i / 2 + (worker_id as u32 * 1_000_000);
        let update = sample_update(worker_id, key, announced, Arc::clone(&shared_attrs), ts);
        if state
            .update(&update.router_addr, &update.peer_addr, &update)
            .unwrap_or(false)
        {
            serializer::serialize_update_into(&update, &mut out);
            black_box(out.len());
            emitted += 1;
        }
    }

    emitted
}

fn run_scaling(
    worker_counts: &[usize],
    updates_per_worker: u32,
    core_ids: Option<Vec<CoreId>>,
) -> Vec<ThroughputPoint> {
    let mut points = Vec::new();
    let mut baseline = None::<f64>;

    for &workers in worker_counts {
        let barrier = Arc::new(Barrier::new(workers + 1));
        let mut handles = Vec::with_capacity(workers);
        for worker_id in 0..workers {
            let barrier = Arc::clone(&barrier);
            let pin = core_ids
                .as_ref()
                .and_then(|ids| ids.get(worker_id % ids.len()))
                .copied();
            handles.push(thread::spawn(move || {
                if let Some(core_id) = pin {
                    let _ = core_affinity::set_for_current(core_id);
                }
                barrier.wait();
                worker_hot_path(worker_id, updates_per_worker)
            }));
        }

        let start = Instant::now();
        barrier.wait();
        let mut total_updates = 0_u64;
        for handle in handles {
            total_updates += handle
                .join()
                .expect("benchmark worker thread should complete");
        }
        let elapsed = start.elapsed();
        let updates_per_sec = total_updates as f64 / elapsed.as_secs_f64();

        let base = baseline.get_or_insert(updates_per_sec);
        let efficiency_vs_single = updates_per_sec / (*base * workers as f64);

        points.push(ThroughputPoint {
            workers,
            elapsed,
            total_updates,
            updates_per_sec,
            efficiency_vs_single,
        });
    }

    points
}

fn print_alloc_runs(title: &str, runs: &[BenchRun]) {
    println!("# {}", title);
    println!("label,updates,elapsed_ms,updates_per_sec,alloc_calls,realloc_calls,alloc_bytes");
    for run in runs {
        let ups = run.updates as f64 / run.elapsed.as_secs_f64();
        println!(
            "{},{},{:.3},{:.2},{},{},{}",
            run.label,
            run.updates,
            run.elapsed.as_secs_f64() * 1000.0,
            ups,
            run.alloc.alloc_calls,
            run.alloc.realloc_calls,
            run.alloc.alloc_bytes
        );
    }
    println!();
}

fn print_scaling(points: &[ThroughputPoint]) {
    println!("# Throughput Scaling");
    println!("workers,total_updates,elapsed_ms,updates_per_sec,efficiency_vs_single");
    for p in points {
        println!(
            "{},{},{:.3},{:.2},{:.3}",
            p.workers,
            p.total_updates,
            p.elapsed.as_secs_f64() * 1000.0,
            p.updates_per_sec,
            p.efficiency_vs_single
        );
    }
    println!();
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "all".to_string());

    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let mut workers = vec![1usize, 2, 4];
    workers.retain(|w| *w <= cpus);
    if workers.is_empty() {
        workers.push(1);
    }
    let core_ids = core_affinity::get_core_ids();

    match mode.as_str() {
        "throughput" => {
            let points = run_scaling(&workers, 600_000, core_ids.clone());
            print_scaling(&points);
        }
        "alloc" => {
            let ser = run_serialization_alloc_bench(200_000);
            let framing = run_framing_alloc_bench(120_000);
            print_alloc_runs("Serialization Allocation Profile", &ser);
            print_alloc_runs("Framing Allocation Profile", &framing);
        }
        "all" => {
            let points = run_scaling(&workers, 600_000, core_ids);
            let ser = run_serialization_alloc_bench(200_000);
            let framing = run_framing_alloc_bench(120_000);
            print_scaling(&points);
            print_alloc_runs("Serialization Allocation Profile", &ser);
            print_alloc_runs("Framing Allocation Profile", &framing);
        }
        _ => {
            eprintln!(
                "usage: cargo run --release -p risotto --bin arancini_bench -- [throughput|alloc|all]"
            );
            std::process::exit(2);
        }
    }
}
