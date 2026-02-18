use anyhow::Result;
use async_nats::jetstream;
use bgpkit_parser::bmp::messages::{PeerDownNotification, RouteMonitoring};
use bgpkit_parser::parser::bmp::messages::BmpMessageBody;
use bytes::Bytes;
use bytes::BytesMut;
use crossfire::mpsc;
use crossfire::{AsyncRxTrait, AsyncTxTrait};
use metrics::counter;
use metrics::gauge;
use metrics::histogram;
use monoio::fs::File;
use monoio::io::AsyncReadRent;
use monoio::net::{ListenerOpts, TcpListener, TcpStream};
use monoio::time::sleep;
use monoio::{FusionDriver, RuntimeBuilder};
use arancini_lib::processor::decode_bmp_message;
use arancini_lib::sender::UpdateSender;
use arancini_lib::state::{synthesize_withdraw_update, State};
use arancini_lib::state_store::memory::MemoryStore;
use arancini_lib::state_store::store::StateStore;
use arancini_lib::update::{decode_updates, new_metadata, Update, UpdateMetadata};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tokio::sync::oneshot;
use tracing::{debug, error, info, trace, warn};

use crate::config::{AppConfig, BMPConfig, CurationConfig, NatsConfig, SnapshotBackend};

const BMP_COMMON_HEADER_LEN: usize = 6;
const BMP_MAX_MESSAGE_TYPE: u8 = 6;
type WorkerShardState = Arc<Mutex<State<MemoryStore>>>;
type SnapshotFlavor = mpsc::Array<SnapshotCommand>;
type SnapshotTx = crossfire::MAsyncTx<SnapshotFlavor>;
type SnapshotRx = crossfire::AsyncRx<SnapshotFlavor>;

#[derive(Debug)]
enum SnapshotCommand {
    Persist {
        worker_id: usize,
        payload: Vec<u8>,
    },
    Restore {
        worker_id: usize,
        reply: oneshot::Sender<std::result::Result<Option<Vec<u8>>, String>>,
    },
}

#[derive(Debug, Clone)]
struct SnapshotBridge {
    tx: SnapshotTx,
}

impl SnapshotBridge {
    fn bounded(size: usize) -> (Self, SnapshotRx) {
        let (tx, rx) = mpsc::bounded_async(size);
        (Self { tx }, rx)
    }

    async fn enqueue_persist(&self, worker_id: usize, payload: Vec<u8>) -> Result<()> {
        self.tx
            .send(SnapshotCommand::Persist { worker_id, payload })
            .await
            .map_err(|err| anyhow::anyhow!("failed to enqueue shard snapshot persist: {}", err))?;
        if let Some(capacity) = self.tx.capacity() {
            gauge!("risotto_arancini_snapshot_queue_fill_ratio")
                .set(self.tx.len() as f64 / capacity as f64);
        }
        gauge!("risotto_arancini_snapshot_queue_depth").set(self.tx.len() as f64);
        Ok(())
    }

    async fn restore(&self, worker_id: usize) -> Result<Option<Vec<u8>>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(SnapshotCommand::Restore {
                worker_id,
                reply: reply_tx,
            })
            .await
            .map_err(|err| anyhow::anyhow!("failed to enqueue shard snapshot restore: {}", err))?;
        if let Some(capacity) = self.tx.capacity() {
            gauge!("risotto_arancini_snapshot_queue_fill_ratio")
                .set(self.tx.len() as f64 / capacity as f64);
        }
        gauge!("risotto_arancini_snapshot_queue_depth").set(self.tx.len() as f64);
        match reply_rx.await {
            Ok(Ok(payload)) => Ok(payload),
            Ok(Err(err)) => Err(anyhow::anyhow!("{}", err)),
            Err(err) => Err(anyhow::anyhow!(
                "failed waiting for snapshot restore response: {}",
                err
            )),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SnapshotManifest {
    object_name: String,
    updated_unix_ms: i64,
    payload_size_bytes: usize,
    format_version: u8,
}

struct BytesAsyncReader {
    data: Bytes,
    pos: usize,
}

impl BytesAsyncReader {
    fn new(data: Bytes) -> Self {
        Self { data, pos: 0 }
    }
}

impl AsyncRead for BytesAsyncReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.pos >= self.data.len() {
            return std::task::Poll::Ready(Ok(()));
        }
        let remaining = &self.data[self.pos..];
        let to_copy = remaining.len().min(buf.remaining());
        if to_copy == 0 {
            return std::task::Poll::Ready(Ok(()));
        }

        buf.put_slice(&remaining[..to_copy]);
        self.pos += to_copy;
        std::task::Poll::Ready(Ok(()))
    }
}

#[derive(Debug, Clone, Copy)]
struct SessionOwnerEntry {
    worker_id: usize,
    ref_count: usize,
}

#[derive(Debug, Clone, Default)]
struct SessionOwnerRegistry {
    owners: Arc<std::sync::Mutex<HashMap<IpAddr, SessionOwnerEntry>>>,
}

#[derive(Debug)]
struct SessionOwnerGuard {
    registry: SessionOwnerRegistry,
    router_ip: IpAddr,
    worker_id: usize,
}

impl SessionOwnerRegistry {
    fn claim(&self, router_ip: IpAddr, worker_id: usize) -> Result<SessionOwnerGuard> {
        let mut owners = self
            .owners
            .lock()
            .map_err(|_| anyhow::anyhow!("session owner registry lock poisoned"))?;
        match owners.entry(router_ip) {
            Entry::Vacant(slot) => {
                slot.insert(SessionOwnerEntry {
                    worker_id,
                    ref_count: 1,
                });
            }
            Entry::Occupied(mut slot) => {
                let owner = slot.get_mut();
                if owner.worker_id != worker_id {
                    counter!("risotto_arancini_session_ownership_conflicts_total").increment(1);
                    anyhow::bail!(
                        "router {} already owned by worker {}, worker {} cannot claim",
                        router_ip,
                        owner.worker_id,
                        worker_id
                    );
                }
                owner.ref_count += 1;
                counter!("risotto_arancini_session_duplicate_claims_total").increment(1);
            }
        }

        counter!("risotto_arancini_session_claim_total").increment(1);
        Ok(SessionOwnerGuard {
            registry: self.clone(),
            router_ip,
            worker_id,
        })
    }

    fn assert_owner(&self, router_ip: IpAddr, worker_id: usize) -> Result<()> {
        let owners = self
            .owners
            .lock()
            .map_err(|_| anyhow::anyhow!("session owner registry lock poisoned"))?;
        match owners.get(&router_ip) {
            Some(owner) if owner.worker_id == worker_id => Ok(()),
            Some(owner) => {
                counter!("risotto_arancini_session_ownership_assert_failures_total").increment(1);
                anyhow::bail!(
                    "router {} ownership mismatch: expected worker {}, found worker {}",
                    router_ip,
                    worker_id,
                    owner.worker_id
                )
            }
            None => {
                counter!("risotto_arancini_session_ownership_assert_failures_total").increment(1);
                anyhow::bail!(
                    "router {} has no active owner while asserting worker {}",
                    router_ip,
                    worker_id
                )
            }
        }
    }

    fn release(&self, router_ip: IpAddr, worker_id: usize) {
        let mut owners = match self.owners.lock() {
            Ok(owners) => owners,
            Err(_) => {
                error!("session owner registry lock poisoned during release");
                return;
            }
        };

        let mut remove_entry = false;
        if let Some(owner) = owners.get_mut(&router_ip) {
            if owner.worker_id != worker_id {
                counter!("risotto_arancini_session_ownership_assert_failures_total").increment(1);
                warn!(
                    "router {} release by worker {} rejected; owned by worker {}",
                    router_ip, worker_id, owner.worker_id
                );
                return;
            }

            if owner.ref_count > 1 {
                owner.ref_count -= 1;
            } else {
                remove_entry = true;
            }
            counter!("risotto_arancini_session_release_total").increment(1);
        }

        if remove_entry {
            owners.remove(&router_ip);
        }
    }
}

impl SessionOwnerGuard {
    fn assert_owner(&self) -> Result<()> {
        self.registry.assert_owner(self.router_ip, self.worker_id)
    }
}

impl Drop for SessionOwnerGuard {
    fn drop(&mut self) {
        self.registry.release(self.router_ip, self.worker_id);
    }
}

#[derive(Debug, Default)]
struct IngestAllocationStats {
    slot_growth_events: u64,
    frame_buffer_growth_events: u64,
}

impl IngestAllocationStats {
    fn record_slot_growth(&mut self) {
        self.slot_growth_events += 1;
        counter!("risotto_arancini_ingest_slot_growth_total").increment(1);
    }

    fn record_frame_buffer_growth(&mut self) {
        self.frame_buffer_growth_events += 1;
        counter!("risotto_arancini_ingest_frame_buffer_growth_total").increment(1);
    }
}

struct FixedSlotRing {
    slots: Vec<Vec<u8>>,
    slot_size: usize,
    next: usize,
}

impl FixedSlotRing {
    fn new(slot_count: usize, slot_size: usize) -> Result<Self> {
        if slot_count == 0 {
            anyhow::bail!("arancini fixed slot count must be greater than 0");
        }
        if slot_size < BMP_COMMON_HEADER_LEN {
            anyhow::bail!(
                "arancini fixed slot size {} must be at least BMP common header length {}",
                slot_size,
                BMP_COMMON_HEADER_LEN
            );
        }

        let mut slots = Vec::with_capacity(slot_count);
        for _ in 0..slot_count {
            slots.push(Vec::with_capacity(slot_size));
        }

        Ok(Self {
            slots,
            slot_size,
            next: 0,
        })
    }

    fn checkout(&mut self, stats: &mut IngestAllocationStats) -> (usize, Vec<u8>) {
        let idx = self.next;
        self.next = (self.next + 1) % self.slots.len();
        let mut slot = std::mem::take(&mut self.slots[idx]);
        if slot.capacity() < self.slot_size {
            let before = slot.capacity();
            slot.reserve(self.slot_size - slot.capacity());
            if slot.capacity() > before {
                stats.record_slot_growth();
            }
        }
        slot.clear();
        (idx, slot)
    }

    fn checkin(&mut self, idx: usize, mut slot: Vec<u8>, stats: &mut IngestAllocationStats) {
        if slot.capacity() < self.slot_size {
            let before = slot.capacity();
            slot.reserve(self.slot_size - slot.capacity());
            if slot.capacity() > before {
                stats.record_slot_growth();
            }
        }
        slot.clear();
        self.slots[idx] = slot;
    }
}

fn append_frame_chunk(
    frame_buffer: &mut BytesMut,
    chunk: &[u8],
    stats: &mut IngestAllocationStats,
) {
    let remaining = frame_buffer.capacity().saturating_sub(frame_buffer.len());
    if remaining < chunk.len() {
        let before = frame_buffer.capacity();
        frame_buffer.reserve(chunk.len() - remaining);
        if frame_buffer.capacity() > before {
            stats.record_frame_buffer_growth();
        }
    }
    frame_buffer.extend_from_slice(chunk);
}

fn next_packet_length(
    frame_buffer: &BytesMut,
    socket: SocketAddr,
    max_frame_size: usize,
) -> Result<Option<usize>> {
    if frame_buffer.len() < BMP_COMMON_HEADER_LEN {
        return Ok(None);
    }

    let message_version = frame_buffer[0];
    if message_version != 3 {
        anyhow::bail!("{}: unsupported BMP version {}", socket, message_version);
    }

    let packet_length = u32::from_be_bytes(
        frame_buffer[1..5]
            .try_into()
            .expect("BMP header length slice should be 4 bytes"),
    ) as usize;
    if packet_length < BMP_COMMON_HEADER_LEN {
        anyhow::bail!("{}: invalid BMP packet length {}", socket, packet_length);
    }
    if packet_length > max_frame_size {
        anyhow::bail!(
            "{}: BMP packet length {} exceeds configured max {}",
            socket,
            packet_length,
            max_frame_size
        );
    }

    let message_type = frame_buffer[5];
    if message_type > BMP_MAX_MESSAGE_TYPE {
        anyhow::bail!("{}: unsupported BMP message type {}", socket, message_type);
    }

    if frame_buffer.len() < packet_length {
        return Ok(None);
    }

    Ok(Some(packet_length))
}

pub fn spawn_workers<S: UpdateSender>(cfg: Arc<AppConfig>, tx: S) -> Result<()> {
    let workers = cfg.runtime.arancini_workers;
    if workers == 0 {
        anyhow::bail!("arancini runtime requires at least one worker thread");
    }
    let bmp_config = cfg.bmp.clone();
    let curation_cfg = cfg.curation.clone();
    let nats_cfg = cfg.nats.clone();
    let ownership = SessionOwnerRegistry::default();
    let snapshot_bridge =
        if curation_cfg.enabled && curation_cfg.snapshot_backend == SnapshotBackend::NatsKv {
            let (bridge, snapshot_rx) =
                SnapshotBridge::bounded(curation_cfg.snapshot_sidecar_buffer_size);
            spawn_snapshot_sidecar_thread(snapshot_rx, curation_cfg.clone(), nats_cfg)?;
            Some(bridge)
        } else {
            None
        };

    let core_ids = core_affinity::get_core_ids();
    info!(
        "starting arancini runtime with {} monoio workers on {}",
        workers, bmp_config.host
    );

    for worker_id in 0..workers {
        let tx = tx.clone();
        let bmp_config = bmp_config.clone();
        let curation_cfg = curation_cfg.clone();
        let ownership = ownership.clone();
        let snapshot_bridge = snapshot_bridge.clone();
        let pinned_core = core_ids
            .as_ref()
            .and_then(|ids| ids.get(worker_id % ids.len()).cloned());

        std::thread::Builder::new()
            .name(format!("arancini-worker-{}", worker_id))
            .spawn(move || {
                if let Some(core_id) = pinned_core {
                    let _ = core_affinity::set_for_current(core_id);
                }

                let mut runtime = match RuntimeBuilder::<FusionDriver>::new()
                    .enable_timer()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(err) => {
                        error!("failed to build arancini monoio runtime: {}", err);
                        return;
                    }
                };

                runtime.block_on(async move {
                    if let Err(err) = worker_loop(
                        worker_id,
                        bmp_config,
                        tx,
                        ownership,
                        curation_cfg,
                        snapshot_bridge,
                    )
                    .await
                    {
                        error!("arancini worker {} failed: {}", worker_id, err);
                    }
                });
            })?;
    }

    Ok(())
}

async fn worker_loop<S: UpdateSender>(
    worker_id: usize,
    cfg: BMPConfig,
    tx: S,
    ownership: SessionOwnerRegistry,
    curation_cfg: CurationConfig,
    snapshot_bridge: Option<SnapshotBridge>,
) -> Result<()> {
    // SO_REUSEPORT is enabled to let the kernel spread accepted sessions across workers.
    let mut opts = ListenerOpts::new()
        .reuse_port(true)
        .reuse_addr(true)
        .backlog(cfg.listener_backlog);
    if let Some(recv_buf_size) = cfg.socket_recv_buffer_bytes {
        opts = opts.recv_buf_size(recv_buf_size);
    }
    let listener = TcpListener::bind_with_config(cfg.host, &opts)?;
    info!("arancini worker {} listening on {}", worker_id, cfg.host);
    let curation_enabled = curation_cfg.enabled;
    let shard = if curation_enabled {
        Some(Arc::new(Mutex::new(State::new(MemoryStore::new()))))
    } else {
        None
    };
    if curation_enabled {
        if let Some(shard_ref) = &shard {
            match curation_cfg.snapshot_backend {
                SnapshotBackend::Local => {
                    load_worker_shard_snapshot_local(shard_ref, worker_id, &curation_cfg).await;
                }
                SnapshotBackend::NatsKv => {
                    if let Some(bridge) = &snapshot_bridge {
                        load_worker_shard_snapshot_nats(shard_ref, worker_id, bridge).await;
                    } else {
                        error!(
                            "arancini worker {} missing snapshot bridge for NATS restore backend",
                            worker_id
                        );
                    }
                }
            }
        }

        if let Some(snapshot_shard) = shard.clone() {
            let snapshot_cfg = curation_cfg.clone();
            let snapshot_bridge = snapshot_bridge.clone();
            monoio::spawn(async move {
                match snapshot_cfg.snapshot_backend {
                    SnapshotBackend::Local => {
                        worker_snapshot_loop_local(worker_id, snapshot_shard, snapshot_cfg).await
                    }
                    SnapshotBackend::NatsKv => {
                        if let Some(bridge) = snapshot_bridge {
                            worker_snapshot_loop_nats(
                                worker_id,
                                snapshot_shard,
                                snapshot_cfg,
                                bridge,
                            )
                            .await
                        } else {
                            error!(
                                "arancini worker {} missing snapshot bridge for NATS persistence backend",
                                worker_id
                            );
                        }
                    }
                }
            });
        }

        debug!(
            "arancini worker {} enabled local shard curation state",
            worker_id
        );
    }

    loop {
        let (stream, socket) = listener.accept().await?;
        let owner_guard = match ownership.claim(socket.ip(), worker_id) {
            Ok(owner_guard) => owner_guard,
            Err(err) => {
                warn!(
                    "arancini worker {} rejected {} due to ownership conflict: {}",
                    worker_id, socket, err
                );
                continue;
            }
        };
        debug!(
            "arancini worker {} accepted {} (router owner claim active)",
            worker_id, socket
        );

        if let Err(err) = stream.set_nodelay(true) {
            warn!(
                "arancini worker {} failed to set TCP_NODELAY on {}: {}",
                worker_id, socket, err
            );
        }

        let tx = tx.clone();
        let conn_cfg = cfg.clone();
        let shard = shard.clone();
        monoio::spawn(async move {
            if let Err(err) =
                handle_connection(stream, socket, conn_cfg, tx, worker_id, owner_guard, shard).await
            {
                error!("arancini connection {} failed: {}", socket, err);
            }
        });
    }
}

async fn handle_connection<S: UpdateSender>(
    mut stream: TcpStream,
    socket: SocketAddr,
    cfg: BMPConfig,
    tx: S,
    worker_id: usize,
    owner_guard: SessionOwnerGuard,
    shard: Option<WorkerShardState>,
) -> Result<()> {
    owner_guard.assert_owner()?;
    let mut alloc_stats = IngestAllocationStats::default();
    let mut slots = FixedSlotRing::new(
        cfg.arancini_fixed_slot_count,
        cfg.arancini_fixed_slot_size_bytes,
    )?;
    let mut frame_buffer = BytesMut::with_capacity(cfg.arancini_fixed_slot_size_bytes);

    loop {
        let (slot_idx, slot) = slots.checkout(&mut alloc_stats);
        let (res, slot) = stream.read(slot).await;
        let n = match res {
            Ok(0) => {
                slots.checkin(slot_idx, slot, &mut alloc_stats);
                debug!("{}: arancini session closed (EOF)", socket);
                break;
            }
            Ok(n) => n,
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::BrokenPipe
                ) =>
            {
                slots.checkin(slot_idx, slot, &mut alloc_stats);
                debug!("{}: arancini session closed ({})", socket, err);
                break;
            }
            Err(err) => {
                slots.checkin(slot_idx, slot, &mut alloc_stats);
                anyhow::bail!("{}: failed to read BMP socket data: {}", socket, err)
            }
        };

        append_frame_chunk(&mut frame_buffer, &slot[..n], &mut alloc_stats);
        slots.checkin(slot_idx, slot, &mut alloc_stats);

        loop {
            let packet_length =
                match next_packet_length(&frame_buffer, socket, cfg.arancini_max_frame_size_bytes)?
                {
                    Some(packet_length) => packet_length,
                    None => {
                        break;
                    }
                };

            if frame_buffer.len() < packet_length {
                break;
            }

            trace!(
                "{}: arancini read BMP packet ({} bytes)",
                socket,
                packet_length
            );
            let mut bytes = frame_buffer.split_to(packet_length).freeze();
            process_bmp_message_shard(shard.clone(), tx.clone(), worker_id, socket, &mut bytes)
                .await?;
        }

        if frame_buffer.len() > cfg.arancini_max_frame_size_bytes {
            anyhow::bail!(
                "{}: buffered BMP data {} exceeds max frame size {}",
                socket,
                frame_buffer.len(),
                cfg.arancini_max_frame_size_bytes
            );
        }
    }

    debug!(
        "{}: arancini ingest allocation stats: slot_growth_events={}, frame_buffer_growth_events={}",
        socket, alloc_stats.slot_growth_events, alloc_stats.frame_buffer_growth_events
    );
    debug!(
        "{}: arancini session ownership released for worker {}",
        socket, worker_id
    );

    Ok(())
}

fn worker_snapshot_path(base_path: &str, worker_id: usize) -> String {
    format!("{}.worker-{}.bin", base_path, worker_id)
}

fn worker_snapshot_manifest_key(worker_id: usize) -> String {
    format!("worker-{}", worker_id)
}

fn worker_snapshot_object_name(worker_id: usize) -> String {
    format!("worker-{}-latest", worker_id)
}

fn install_restored_store(
    shard: &WorkerShardState,
    worker_id: usize,
    source: &str,
    store: MemoryStore,
) -> Result<()> {
    let peers = store.get_peers();
    for (router_addr, peer_addr) in &peers {
        let updates = store.get_updates_by_peer(router_addr, peer_addr);
        gauge!(
            "risotto_state_updates",
            "router" => router_addr.to_string(),
            "peer" => peer_addr.to_string()
        )
        .set(updates.len() as f64);
    }

    let mut state = lock_worker_shard(shard)?;
    state.store = store;
    info!(
        "arancini worker {} restored shard snapshot from {} ({} peers)",
        worker_id,
        source,
        peers.len()
    );
    Ok(())
}

async fn load_worker_shard_snapshot_local(
    shard: &WorkerShardState,
    worker_id: usize,
    cfg: &CurationConfig,
) {
    let state_path = worker_snapshot_path(&cfg.state_path, worker_id);
    match monoio::fs::read(&state_path).await {
        Ok(encoded) => match postcard::from_bytes::<MemoryStore>(&encoded) {
            Ok(store) => {
                if let Err(err) = install_restored_store(shard, worker_id, &state_path, store) {
                    error!(
                        "arancini worker {} failed to apply local snapshot {}: {}",
                        worker_id, state_path, err
                    );
                }
            }
            Err(err) => {
                error!(
                    "arancini worker {} failed to deserialize snapshot {}: {}",
                    worker_id, state_path, err
                );
                let backup_path = format!(
                    "{}.corrupted-{}",
                    state_path,
                    chrono::Utc::now().timestamp()
                );
                warn!(
                    "arancini worker {} renaming corrupted snapshot to {}",
                    worker_id, backup_path
                );
                if let Err(rename_err) = std::fs::rename(&state_path, backup_path) {
                    warn!(
                        "arancini worker {} failed to rename corrupted snapshot {}: {}",
                        worker_id, state_path, rename_err
                    );
                }
            }
        },
        Err(err) => {
            if err.kind() == std::io::ErrorKind::NotFound {
                info!(
                    "arancini worker {} snapshot {} not found; starting empty",
                    worker_id, state_path
                );
            } else {
                warn!(
                    "arancini worker {} failed to read snapshot {}: {}",
                    worker_id, state_path, err
                );
            }
        }
    }
}

async fn load_worker_shard_snapshot_nats(
    shard: &WorkerShardState,
    worker_id: usize,
    bridge: &SnapshotBridge,
) {
    match bridge.restore(worker_id).await {
        Ok(Some(encoded)) => match postcard::from_bytes::<MemoryStore>(&encoded) {
            Ok(store) => {
                let source = format!(
                    "nats:{}:{}",
                    "manifest",
                    worker_snapshot_manifest_key(worker_id)
                );
                if let Err(err) = install_restored_store(shard, worker_id, &source, store) {
                    error!(
                        "arancini worker {} failed to apply NATS snapshot: {}",
                        worker_id, err
                    );
                }
            }
            Err(err) => {
                error!(
                    "arancini worker {} failed to deserialize NATS snapshot payload: {}",
                    worker_id, err
                );
            }
        },
        Ok(None) => {
            info!(
                "arancini worker {} has no NATS snapshot manifest; starting empty",
                worker_id
            );
        }
        Err(err) => {
            warn!(
                "arancini worker {} failed to restore from NATS snapshot backend: {}",
                worker_id, err
            );
        }
    }
}

async fn dump_worker_shard_snapshot_local(
    shard: &WorkerShardState,
    worker_id: usize,
    cfg: &CurationConfig,
) -> Result<()> {
    let state_path = worker_snapshot_path(&cfg.state_path, worker_id);
    let temp_path = format!("{}.tmp", state_path);
    let parent_dir = std::path::Path::new(&state_path).parent();
    if let Some(parent_dir) = parent_dir {
        if !parent_dir.as_os_str().is_empty() {
            std::fs::create_dir_all(parent_dir).map_err(|err| {
                anyhow::anyhow!(
                    "arancini worker {} failed to create snapshot dir {}: {}",
                    worker_id,
                    parent_dir.display(),
                    err
                )
            })?;
        }
    }

    let snapshot_store = {
        let state = lock_worker_shard(shard)?;
        state.store.clone()
    };
    let encoded = postcard::to_allocvec(&snapshot_store).map_err(|err| {
        anyhow::anyhow!(
            "arancini worker {} failed to serialize shard snapshot {}: {}",
            worker_id,
            state_path,
            err
        )
    })?;

    let file = File::create(&temp_path).await.map_err(|err| {
        anyhow::anyhow!(
            "arancini worker {} failed to create snapshot temp file {}: {}",
            worker_id,
            temp_path,
            err
        )
    })?;
    let (write_res, _) = file.write_all_at(encoded, 0).await;
    if let Err(err) = write_res {
        let _ = std::fs::remove_file(&temp_path);
        anyhow::bail!(
            "arancini worker {} failed to write snapshot {}: {}",
            worker_id,
            temp_path,
            err
        );
    }
    if let Err(err) = file.sync_all().await {
        let _ = std::fs::remove_file(&temp_path);
        anyhow::bail!(
            "arancini worker {} failed to sync snapshot {}: {}",
            worker_id,
            temp_path,
            err
        );
    }

    std::fs::rename(&temp_path, &state_path).map_err(|err| {
        let _ = std::fs::remove_file(&temp_path);
        anyhow::anyhow!(
            "arancini worker {} failed to rename snapshot {} to {}: {}",
            worker_id,
            temp_path,
            state_path,
            err
        )
    })?;

    Ok(())
}

async fn dump_worker_shard_snapshot_nats(
    shard: &WorkerShardState,
    worker_id: usize,
    bridge: &SnapshotBridge,
) -> Result<()> {
    let snapshot_store = {
        let state = lock_worker_shard(shard)?;
        state.store.clone()
    };
    let encoded = postcard::to_allocvec(&snapshot_store).map_err(|err| {
        anyhow::anyhow!(
            "arancini worker {} failed to serialize shard snapshot for NATS backend: {}",
            worker_id,
            err
        )
    })?;
    bridge.enqueue_persist(worker_id, encoded).await?;
    Ok(())
}

async fn worker_snapshot_loop_local(
    worker_id: usize,
    shard: WorkerShardState,
    cfg: CurationConfig,
) {
    let worker_label = worker_id.to_string();
    let interval_secs = cfg.state_save_interval.max(1);
    loop {
        sleep(Duration::from_secs(interval_secs)).await;
        let start = Instant::now();
        match dump_worker_shard_snapshot_local(&shard, worker_id, &cfg).await {
            Ok(()) => {
                counter!("risotto_state_dump_total").increment(1);
                let elapsed = start.elapsed().as_secs_f64();
                histogram!("risotto_state_dump_duration_seconds").record(elapsed);
                histogram!("risotto_arancini_snapshot_duration_seconds", "worker" => worker_label.clone())
                    .record(elapsed);
                trace!(
                    "arancini worker {} persisted shard snapshot to {}",
                    worker_id,
                    worker_snapshot_path(&cfg.state_path, worker_id)
                );
            }
            Err(err) => {
                error!(
                    "arancini worker {} failed to persist shard snapshot: {}",
                    worker_id, err
                );
            }
        }
    }
}

async fn worker_snapshot_loop_nats(
    worker_id: usize,
    shard: WorkerShardState,
    cfg: CurationConfig,
    bridge: SnapshotBridge,
) {
    let worker_label = worker_id.to_string();
    let interval_secs = cfg.state_save_interval.max(1);
    loop {
        sleep(Duration::from_secs(interval_secs)).await;
        let start = Instant::now();
        match dump_worker_shard_snapshot_nats(&shard, worker_id, &bridge).await {
            Ok(()) => {
                counter!("risotto_state_dump_total").increment(1);
                let elapsed = start.elapsed().as_secs_f64();
                histogram!("risotto_state_dump_duration_seconds").record(elapsed);
                histogram!("risotto_arancini_snapshot_duration_seconds", "worker" => worker_label.clone())
                    .record(elapsed);
                trace!(
                    "arancini worker {} enqueued shard snapshot to NATS backend",
                    worker_id
                );
            }
            Err(err) => {
                error!(
                    "arancini worker {} failed to enqueue NATS shard snapshot: {}",
                    worker_id, err
                );
            }
        }
    }
}

fn spawn_snapshot_sidecar_thread(
    rx: SnapshotRx,
    curation_cfg: CurationConfig,
    nats_cfg: NatsConfig,
) -> Result<()> {
    std::thread::Builder::new()
        .name("arancini-snapshot-sidecar".to_string())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(err) => {
                    error!("failed to build arancini snapshot sidecar runtime: {}", err);
                    return;
                }
            };

            runtime.block_on(async move {
                if let Err(err) = snapshot_sidecar_loop(rx, curation_cfg, nats_cfg).await {
                    error!("arancini snapshot sidecar failed: {}", err);
                }
            });
        })
        .map(|_| ())
        .map_err(|err| anyhow::anyhow!("failed to spawn snapshot sidecar thread: {}", err))
}

async fn get_or_create_kv_store(
    js: &jetstream::Context,
    bucket: &str,
) -> Result<jetstream::kv::Store> {
    match js.get_key_value(bucket).await {
        Ok(kv) => Ok(kv),
        Err(_) => {
            js.create_key_value(jetstream::kv::Config {
                bucket: bucket.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|err| anyhow::anyhow!("failed to create key-value bucket {}: {}", bucket, err))
        }
    }
}

async fn get_or_create_object_store(
    js: &jetstream::Context,
    bucket: &str,
) -> Result<jetstream::object_store::ObjectStore> {
    match js.get_object_store(bucket).await {
        Ok(store) => Ok(store),
        Err(_) => js
            .create_object_store(jetstream::object_store::Config {
                bucket: bucket.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|err| {
                anyhow::anyhow!("failed to create object store bucket {}: {}", bucket, err)
            }),
    }
}

async fn snapshot_sidecar_loop(
    rx: SnapshotRx,
    curation_cfg: CurationConfig,
    nats_cfg: NatsConfig,
) -> Result<()> {
    let client = crate::nats::connect(&nats_cfg).await.map_err(|err| {
        anyhow::anyhow!(
            "failed to connect snapshot sidecar to NATS {}: {}",
            nats_cfg.server,
            err
        )
    })?;
    let js = jetstream::new(client);
    let kv = get_or_create_kv_store(&js, &curation_cfg.snapshot_nats_kv_bucket).await?;
    let object_store =
        get_or_create_object_store(&js, &curation_cfg.snapshot_nats_object_store_bucket).await?;

    info!(
        "arancini snapshot sidecar connected to {} (kv={}, object_store={})",
        nats_cfg.server,
        curation_cfg.snapshot_nats_kv_bucket,
        curation_cfg.snapshot_nats_object_store_bucket
    );

    loop {
        let command = match rx.recv().await {
            Ok(command) => command,
            Err(_) => break,
        };
        if let Some(capacity) = rx.capacity() {
            gauge!("risotto_arancini_snapshot_queue_fill_ratio")
                .set(rx.len() as f64 / capacity as f64);
        }
        gauge!("risotto_arancini_snapshot_queue_depth").set(rx.len() as f64);

        match command {
            SnapshotCommand::Persist { worker_id, payload } => {
                let object_name = worker_snapshot_object_name(worker_id);
                let payload_size_bytes = payload.len();
                let mut reader = BytesAsyncReader::new(Bytes::from(payload));
                if let Err(err) = object_store.put(object_name.as_str(), &mut reader).await {
                    counter!("risotto_arancini_snapshot_errors_total").increment(1);
                    error!(
                        "snapshot sidecar failed object-store put for {}: {}",
                        object_name, err
                    );
                    continue;
                }

                let manifest = SnapshotManifest {
                    object_name,
                    updated_unix_ms: chrono::Utc::now().timestamp_millis(),
                    payload_size_bytes,
                    format_version: 1,
                };
                let manifest_key = worker_snapshot_manifest_key(worker_id);
                let manifest_payload = match serde_json::to_vec(&manifest) {
                    Ok(payload) => payload,
                    Err(err) => {
                        counter!("risotto_arancini_snapshot_errors_total").increment(1);
                        error!(
                            "snapshot sidecar failed to encode snapshot manifest: {}",
                            err
                        );
                        continue;
                    }
                };
                if let Err(err) = kv.put(manifest_key.clone(), manifest_payload.into()).await {
                    counter!("risotto_arancini_snapshot_errors_total").increment(1);
                    error!(
                        "snapshot sidecar failed kv put for {}: {}",
                        manifest_key, err
                    );
                    continue;
                }
                counter!("risotto_arancini_snapshot_persist_total").increment(1);
            }
            SnapshotCommand::Restore { worker_id, reply } => {
                let response = async {
                    let manifest_key = worker_snapshot_manifest_key(worker_id);
                    let manifest_bytes = match kv.get(manifest_key.clone()).await {
                        Ok(Some(bytes)) => bytes,
                        Ok(None) => return Ok(None),
                        Err(err) => {
                            return Err(anyhow::anyhow!(
                                "failed kv get for {}: {}",
                                manifest_key,
                                err
                            ))
                        }
                    };
                    let manifest: SnapshotManifest = serde_json::from_slice(&manifest_bytes)
                        .map_err(|err| {
                            anyhow::anyhow!("failed to decode manifest json: {}", err)
                        })?;

                    let mut object = object_store
                        .get(manifest.object_name.clone())
                        .await
                        .map_err(|err| {
                            anyhow::anyhow!(
                                "failed object-store get for {}: {}",
                                manifest.object_name,
                                err
                            )
                        })?;
                    let mut payload = Vec::with_capacity(manifest.payload_size_bytes);
                    object
                        .read_to_end(&mut payload)
                        .await
                        .map_err(|err| anyhow::anyhow!("failed reading object payload: {}", err))?;
                    Ok(Some(payload))
                }
                .await
                .map_err(|err| err.to_string());
                if response.is_err() {
                    counter!("risotto_arancini_snapshot_errors_total").increment(1);
                } else {
                    counter!("risotto_arancini_snapshot_restore_total").increment(1);
                }
                let _ = reply.send(response);
            }
        }
    }

    Ok(())
}

fn lock_worker_shard(
    shard: &WorkerShardState,
) -> Result<std::sync::MutexGuard<'_, State<MemoryStore>>> {
    shard
        .lock()
        .map_err(|_| anyhow::anyhow!("worker shard state lock poisoned"))
}

fn curate_updates_in_shard(shard: &WorkerShardState, updates: Vec<Update>) -> Result<Vec<Update>> {
    let mut to_emit = Vec::new();
    let mut state = lock_worker_shard(shard)?;
    for update in updates {
        if state.update(&update.router_addr, &update.peer_addr, &update)? {
            to_emit.push(update);
        }
    }
    Ok(to_emit)
}

async fn process_route_monitoring_shard<S: UpdateSender>(
    shard: Option<WorkerShardState>,
    tx: S,
    worker_id: usize,
    metadata: UpdateMetadata,
    body: RouteMonitoring,
) -> Result<()> {
    let updates = decode_updates(body, metadata).unwrap_or_default();
    let update_count = updates.len() as u64;
    counter!("risotto_rx_updates_total").increment(update_count);
    counter!("risotto_arancini_worker_rx_updates_total", "worker" => worker_id.to_string())
        .increment(update_count);

    if updates.is_empty() {
        return Ok(());
    }

    let to_emit = match shard {
        Some(shard) => curate_updates_in_shard(&shard, updates)?,
        None => updates,
    };

    let mut sent_updates = 0_u64;
    for update in to_emit {
        tx.send(update).await?;
        sent_updates += 1;
    }
    counter!("risotto_tx_updates_total").increment(sent_updates);
    counter!("risotto_arancini_worker_tx_updates_total", "worker" => worker_id.to_string())
        .increment(sent_updates);

    Ok(())
}

async fn process_peer_up_shard<S: UpdateSender>(
    shard: Option<WorkerShardState>,
    tx: S,
    worker_id: usize,
    metadata: UpdateMetadata,
) -> Result<()> {
    if let Some(shard) = shard {
        {
            let mut state = lock_worker_shard(&shard)?;
            state.add_peer(&metadata.router_socket.ip(), &metadata.peer_addr)?;
        }

        gauge!(
            "risotto_peer_established",
            "router" => metadata.router_socket.ip().to_string(),
            "peer" => metadata.peer_addr.to_string()
        )
        .set(1);

        let spawn_shard = shard.clone();
        let spawn_tx = tx.clone();
        monoio::spawn(async move {
            if let Err(err) =
                peer_up_withdraws_handler_shard(spawn_shard, spawn_tx, worker_id, metadata, 300)
                    .await
            {
                error!("arancini peer_up stale-withdraw handler failed: {}", err);
            }
        });
    }

    Ok(())
}

async fn peer_up_withdraws_handler_shard<S: UpdateSender>(
    shard: WorkerShardState,
    tx: S,
    worker_id: usize,
    metadata: UpdateMetadata,
    sleep_time: u64,
) -> Result<()> {
    let startup = chrono::Utc::now();
    sleep(Duration::from_secs(sleep_time)).await;

    let stale_prefixes = {
        let state = lock_worker_shard(&shard)?;
        state
            .store
            .get_updates_by_peer(&metadata.router_socket.ip(), &metadata.peer_addr)
            .into_iter()
            .filter(|timed_prefix| timed_prefix.timestamp < startup.timestamp_millis())
            .collect::<Vec<_>>()
    };

    let stale_count = stale_prefixes.len() as u64;
    counter!("risotto_tx_updates_total").increment(stale_count);
    counter!("risotto_arancini_worker_tx_updates_total", "worker" => worker_id.to_string())
        .increment(stale_count);

    for prefix in &stale_prefixes {
        let update = synthesize_withdraw_update(prefix, metadata.clone());
        tx.send(update).await?;
    }

    {
        let mut state = lock_worker_shard(&shard)?;
        for prefix in &stale_prefixes {
            let update = synthesize_withdraw_update(prefix, metadata.clone());
            state
                .store
                .update(&update.router_addr, &metadata.peer_addr, &update);
        }
    }

    Ok(())
}

async fn process_peer_down_shard<S: UpdateSender>(
    shard: Option<WorkerShardState>,
    tx: S,
    worker_id: usize,
    metadata: UpdateMetadata,
    _: PeerDownNotification,
) -> Result<()> {
    if let Some(shard) = shard {
        let synthetic_updates = {
            let mut state = lock_worker_shard(&shard)?;
            let prefixes =
                state.get_updates_by_peer(&metadata.router_socket.ip(), &metadata.peer_addr)?;
            let synthetic_updates = prefixes
                .into_iter()
                .map(|prefix| synthesize_withdraw_update(&prefix, metadata.clone()))
                .collect::<Vec<_>>();
            state.remove_peer(&metadata.router_socket.ip(), &metadata.peer_addr)?;
            synthetic_updates
        };

        gauge!(
            "risotto_peer_established",
            "router" => metadata.router_socket.ip().to_string(),
            "peer" => metadata.peer_addr.to_string()
        )
        .set(0);

        let synthetic_count = synthetic_updates.len() as u64;
        counter!("risotto_tx_updates_total").increment(synthetic_count);
        counter!("risotto_arancini_worker_tx_updates_total", "worker" => worker_id.to_string())
            .increment(synthetic_count);
        for update in synthetic_updates {
            tx.send(update).await?;
        }
    }

    Ok(())
}

async fn process_bmp_message_shard<S: UpdateSender>(
    shard: Option<WorkerShardState>,
    tx: S,
    worker_id: usize,
    socket: SocketAddr,
    bytes: &mut bytes::Bytes,
) -> Result<()> {
    let message = decode_bmp_message(bytes).map_err(|err| anyhow::anyhow!("{}", err))?;
    trace!("{} - {:?}", socket, message);
    let metadata = new_metadata(socket, &message);

    let metric_name = "risotto_bmp_messages_total";
    match message.message_body {
        BmpMessageBody::InitiationMessage(body) => {
            trace!("{}: {:?}", socket, body);
            counter!(metric_name, "router" =>  socket.ip().to_string(), "type" => "initiation")
                .increment(1);
        }
        BmpMessageBody::PeerUpNotification(body) => {
            trace!("{}: {:?}", socket, body);
            let metadata = metadata.ok_or_else(|| {
                anyhow::anyhow!("{}: PeerUpNotification: no per-peer header", socket)
            })?;
            counter!(metric_name, "router" =>  socket.ip().to_string(), "type" => "peer_up_notification")
                .increment(1);
            process_peer_up_shard(shard, tx, worker_id, metadata).await?;
        }
        BmpMessageBody::RouteMonitoring(body) => {
            trace!("{}: {:?}", socket, body);
            let metadata = metadata.ok_or_else(|| {
                anyhow::anyhow!("{}: RouteMonitoring: no per-peer header", socket)
            })?;
            if !metadata.peer_addr.is_unspecified() {
                counter!(metric_name, "router" =>  socket.ip().to_string(), "type" => "route_monitoring")
                    .increment(1);
                process_route_monitoring_shard(shard, tx, worker_id, metadata, body).await?;
            }
        }
        BmpMessageBody::RouteMirroring(body) => {
            trace!("{}: {:?}", socket, body);
            counter!(metric_name, "router" =>  socket.ip().to_string(), "type" => "route_mirroring")
                .increment(1);
        }
        BmpMessageBody::PeerDownNotification(body) => {
            trace!("{}: {:?}", socket, body);
            let metadata = metadata.ok_or_else(|| {
                anyhow::anyhow!("{}: PeerDownNotification: no per-peer header", socket)
            })?;
            counter!(metric_name, "router" =>  socket.ip().to_string(), "type" => "peer_down_notification")
                .increment(1);
            process_peer_down_shard(shard, tx, worker_id, metadata, body).await?;
        }
        BmpMessageBody::TerminationMessage(body) => {
            trace!("{}: {:?}", socket, body);
            counter!(metric_name, "router" =>  socket.ip().to_string(), "type" => "termination")
                .increment(1);
        }
        BmpMessageBody::StatsReport(body) => {
            trace!("{}: {:?}", socket, body);
            counter!(metric_name, "router" =>  socket.ip().to_string(), "type" => "stats_report")
                .increment(1);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bgpkit_parser::bmp::messages::PeerDownReason;
    use chrono::Utc;
    use arancini_lib::update::{UpdateAttributes, UpdateMetadata};
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::{Notify, Semaphore};
    use tokio::time::{timeout, Duration};

    #[derive(Clone)]
    struct BlockingSender {
        started: Arc<Notify>,
        release: Arc<Semaphore>,
        sent: Arc<AtomicUsize>,
    }

    impl Default for BlockingSender {
        fn default() -> Self {
            Self {
                started: Arc::new(Notify::new()),
                release: Arc::new(Semaphore::new(0)),
                sent: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl BlockingSender {
        async fn wait_for_send_start(&self) {
            self.started.notified().await;
        }

        fn release_one_send(&self) {
            self.release.add_permits(1);
        }

        fn sent_count(&self) -> usize {
            self.sent.load(Ordering::Relaxed)
        }
    }

    impl UpdateSender for BlockingSender {
        fn send<'a>(
            &'a self,
            _update: Update,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move {
                self.sent.fetch_add(1, Ordering::Relaxed);
                self.started.notify_waiters();
                let _permit = self
                    .release
                    .acquire()
                    .await
                    .map_err(|_| anyhow::anyhow!("blocking sender gate closed"))?;
                Ok(())
            })
        }
    }

    fn test_metadata(router: IpAddr, peer: IpAddr) -> UpdateMetadata {
        UpdateMetadata {
            time_bmp_header_ns: Utc::now().timestamp_millis(),
            router_socket: SocketAddr::new(router, 4000),
            peer_addr: peer,
            peer_bgp_id: Ipv4Addr::new(198, 51, 100, 1),
            peer_asn: 64512,
            is_post_policy: false,
            is_adj_rib_out: false,
        }
    }

    fn insert_announced_prefix(shard: &WorkerShardState, metadata: &UpdateMetadata) {
        let mut state = lock_worker_shard(shard).expect("worker shard lock must be available");
        let update = Update {
            time_received_ns: Utc::now(),
            time_bmp_header_ns: Utc::now(),
            router_addr: arancini_lib::update::map_to_ipv6(metadata.router_socket.ip()),
            router_port: metadata.router_socket.port(),
            peer_addr: arancini_lib::update::map_to_ipv6(metadata.peer_addr),
            peer_bgp_id: metadata.peer_bgp_id,
            peer_asn: metadata.peer_asn,
            prefix_addr: arancini_lib::update::map_to_ipv6(IpAddr::V4(Ipv4Addr::new(
                203, 0, 113, 0,
            ))),
            prefix_len: 24,
            is_post_policy: false,
            is_adj_rib_out: false,
            announced: true,
            synthetic: false,
            attrs: Arc::new(UpdateAttributes::default()),
        };
        state
            .store
            .update(&metadata.router_socket.ip(), &metadata.peer_addr, &update);
    }

    fn build_test_bmp_packet(payload_len: usize) -> Vec<u8> {
        let len = BMP_COMMON_HEADER_LEN + payload_len;
        let mut packet = Vec::with_capacity(len);
        packet.push(3);
        packet.extend_from_slice(&(len as u32).to_be_bytes());
        packet.push(0);
        packet.extend(std::iter::repeat_n(0_u8, payload_len));
        packet
    }

    #[test]
    fn steady_state_ingest_reuses_slots_and_frame_buffer() {
        let socket: SocketAddr = "127.0.0.1:4000".parse().unwrap();
        let packet = build_test_bmp_packet(64);
        let mut stats = IngestAllocationStats::default();
        let mut ring = FixedSlotRing::new(8, 1024).unwrap();
        let mut frame_buffer = BytesMut::with_capacity(1024);

        for _ in 0..2_000 {
            let (idx, mut slot) = ring.checkout(&mut stats);
            slot.extend_from_slice(&packet);
            append_frame_chunk(&mut frame_buffer, &slot, &mut stats);
            ring.checkin(idx, slot, &mut stats);

            let packet_length = next_packet_length(&frame_buffer, socket, 1024)
                .unwrap()
                .expect("expected complete frame");
            let _ = frame_buffer.split_to(packet_length);
        }

        assert_eq!(
            stats.slot_growth_events, 0,
            "steady-state slot ring should not grow"
        );
        assert_eq!(
            stats.frame_buffer_growth_events, 0,
            "steady-state frame buffer should not grow per message"
        );
    }

    #[test]
    fn partial_frame_returns_none_until_complete() {
        let socket: SocketAddr = "127.0.0.1:4000".parse().unwrap();
        let packet = build_test_bmp_packet(32);
        let mut buffer = BytesMut::with_capacity(128);
        let mut stats = IngestAllocationStats::default();

        append_frame_chunk(&mut buffer, &packet[..4], &mut stats);
        assert!(
            next_packet_length(&buffer, socket, 128).unwrap().is_none(),
            "header is incomplete"
        );

        append_frame_chunk(&mut buffer, &packet[4..], &mut stats);
        assert_eq!(
            next_packet_length(&buffer, socket, 128).unwrap(),
            Some(packet.len())
        );
    }

    #[test]
    fn session_owner_registry_rejects_cross_worker_claims() {
        let registry = SessionOwnerRegistry::default();
        let router_ip: IpAddr = "192.0.2.10".parse().unwrap();

        let first = registry.claim(router_ip, 1).unwrap();
        assert!(registry.claim(router_ip, 2).is_err());
        drop(first);
        assert!(registry.claim(router_ip, 2).is_ok());
    }

    #[test]
    fn session_owner_registry_refcounts_same_worker_claims() {
        let registry = SessionOwnerRegistry::default();
        let router_ip: IpAddr = "198.51.100.77".parse().unwrap();

        let first = registry.claim(router_ip, 3).unwrap();
        let second = registry.claim(router_ip, 3).unwrap();
        assert!(registry.assert_owner(router_ip, 3).is_ok());

        drop(first);
        assert!(
            registry.assert_owner(router_ip, 3).is_ok(),
            "ownership should stay active while one claim remains"
        );

        drop(second);
        assert!(
            registry.assert_owner(router_ip, 3).is_err(),
            "ownership should be removed after final claim drops"
        );
    }

    #[test]
    fn session_owner_registry_wrong_worker_release_does_not_drop_ownership() {
        let registry = SessionOwnerRegistry::default();
        let router_ip: IpAddr = "203.0.113.90".parse().unwrap();

        let guard = registry.claim(router_ip, 7).unwrap();
        registry.release(router_ip, 8);
        assert!(
            registry.assert_owner(router_ip, 7).is_ok(),
            "release from non-owner worker must not clear ownership"
        );

        drop(guard);
        assert!(
            registry.assert_owner(router_ip, 7).is_err(),
            "ownership should clear only after valid owner release"
        );
    }

    #[test]
    fn session_owner_registry_multi_router_ownership_is_sticky_until_release() {
        let registry = SessionOwnerRegistry::default();
        let router_a: IpAddr = "192.0.2.101".parse().unwrap();
        let router_b: IpAddr = "192.0.2.102".parse().unwrap();
        let router_c: IpAddr = "192.0.2.103".parse().unwrap();

        let a_owner = registry.claim(router_a, 0).unwrap();
        let b_owner = registry.claim(router_b, 1).unwrap();
        let c_owner = registry.claim(router_c, 0).unwrap();

        assert!(
            registry.claim(router_a, 1).is_err(),
            "router A ownership must remain pinned to worker 0"
        );
        assert!(
            registry.claim(router_b, 0).is_err(),
            "router B ownership must remain pinned to worker 1"
        );
        assert!(
            registry.assert_owner(router_a, 0).is_ok()
                && registry.assert_owner(router_b, 1).is_ok()
                && registry.assert_owner(router_c, 0).is_ok(),
            "all routers should report their claimed workers"
        );

        drop(a_owner);
        assert!(
            registry.claim(router_a, 1).is_ok(),
            "router A can move workers only after full release"
        );

        drop(b_owner);
        drop(c_owner);
    }

    #[tokio::test]
    async fn process_peer_down_shard_releases_lock_before_send_await() {
        let metadata = test_metadata(
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 40)),
            IpAddr::V4(Ipv4Addr::new(198, 51, 100, 40)),
        );
        let shard = Arc::new(Mutex::new(State::new(MemoryStore::new())));
        insert_announced_prefix(&shard, &metadata);
        let sender = BlockingSender::default();

        let worker_shard = shard.clone();
        let worker_sender = sender.clone();
        let worker_metadata = metadata.clone();
        let task = tokio::spawn(async move {
            process_peer_down_shard(
                Some(worker_shard),
                worker_sender,
                0,
                worker_metadata,
                PeerDownNotification {
                    reason: PeerDownReason::RemoteSystemsClosedNoData,
                    data: None,
                },
            )
            .await
        });

        timeout(Duration::from_secs(1), sender.wait_for_send_start())
            .await
            .expect("synthetic withdraw send should begin");
        let lock_shard = shard.clone();
        let lock_check =
            tokio::task::spawn_blocking(move || lock_worker_shard(&lock_shard).map(|_| ()));
        let lock_result = timeout(Duration::from_secs(1), lock_check)
            .await
            .expect("lock check should not block");
        lock_result
            .expect("lock check task should not panic")
            .expect("worker shard lock should be acquirable while send awaits");

        sender.release_one_send();
        task.await
            .expect("process_peer_down_shard task should complete")
            .expect("process_peer_down_shard should succeed");
        assert_eq!(sender.sent_count(), 1);
    }
}
