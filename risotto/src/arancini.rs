use anyhow::Result;
use bytes::BytesMut;
use metrics::counter;
use monoio::io::AsyncReadRent;
use monoio::net::{ListenerOpts, TcpListener, TcpStream};
use monoio::{FusionDriver, RuntimeBuilder};
use risotto_lib::process_bmp_message;
use risotto_lib::state_store::memory::MemoryStore;
use risotto_lib::update::Update;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, info, trace, warn};

use crate::config::{AppConfig, BMPConfig};

const BMP_COMMON_HEADER_LEN: usize = 6;
const BMP_MAX_MESSAGE_TYPE: u8 = 6;

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

pub fn spawn_workers(cfg: Arc<AppConfig>, tx: Sender<Update>) -> Result<()> {
    let workers = cfg.runtime.arancini_workers;
    if workers == 0 {
        anyhow::bail!("arancini runtime requires at least one worker thread");
    }
    let bmp_config = cfg.bmp.clone();

    let core_ids = core_affinity::get_core_ids();
    info!(
        "starting arancini runtime with {} monoio workers on {}",
        workers, bmp_config.host
    );

    for worker_id in 0..workers {
        let tx = tx.clone();
        let bmp_config = bmp_config.clone();
        let pinned_core = core_ids
            .as_ref()
            .and_then(|ids| ids.get(worker_id % ids.len()).cloned());

        std::thread::Builder::new()
            .name(format!("arancini-worker-{}", worker_id))
            .spawn(move || {
                if let Some(core_id) = pinned_core {
                    let _ = core_affinity::set_for_current(core_id);
                }

                let mut runtime = match RuntimeBuilder::<FusionDriver>::new().build() {
                    Ok(runtime) => runtime,
                    Err(err) => {
                        error!("failed to build arancini monoio runtime: {}", err);
                        return;
                    }
                };

                runtime.block_on(async move {
                    if let Err(err) = worker_loop(worker_id, bmp_config, tx).await {
                        error!("arancini worker {} failed: {}", worker_id, err);
                    }
                });
            })?;
    }

    Ok(())
}

async fn worker_loop(worker_id: usize, cfg: BMPConfig, tx: Sender<Update>) -> Result<()> {
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

    loop {
        let (stream, socket) = listener.accept().await?;
        debug!("arancini worker {} accepted {}", worker_id, socket);

        if let Err(err) = stream.set_nodelay(true) {
            warn!(
                "arancini worker {} failed to set TCP_NODELAY on {}: {}",
                worker_id, socket, err
            );
        }

        let tx = tx.clone();
        let conn_cfg = cfg.clone();
        monoio::spawn(async move {
            if let Err(err) = handle_connection(stream, socket, conn_cfg, tx).await {
                error!("arancini connection {} failed: {}", socket, err);
            }
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    socket: SocketAddr,
    cfg: BMPConfig,
    tx: Sender<Update>,
) -> Result<()> {
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

            trace!("{}: arancini read BMP packet ({} bytes)", socket, packet_length);
            let mut bytes = frame_buffer.split_to(packet_length).freeze();
            process_bmp_message::<MemoryStore>(None, tx.clone(), socket, &mut bytes).await?;
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

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
