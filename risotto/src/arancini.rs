use anyhow::Result;
use bytes::Bytes;
use monoio::io::AsyncReadRentExt;
use monoio::net::{ListenerOpts, TcpListener, TcpStream};
use risotto_lib::process_bmp_message;
use risotto_lib::state_store::memory::MemoryStore;
use risotto_lib::update::Update;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, info, trace, warn};

use crate::config::AppConfig;

pub fn spawn_workers(cfg: Arc<AppConfig>, tx: Sender<Update>) -> Result<()> {
    let workers = cfg.runtime.arancini_workers;
    if workers == 0 {
        anyhow::bail!("arancini runtime requires at least one worker thread");
    }

    let core_ids = core_affinity::get_core_ids();
    info!(
        "starting arancini runtime with {} monoio workers on {}",
        workers, cfg.bmp.host
    );

    for worker_id in 0..workers {
        let tx = tx.clone();
        let host = cfg.bmp.host;
        let pinned_core = core_ids
            .as_ref()
            .and_then(|ids| ids.get(worker_id % ids.len()).cloned());

        std::thread::Builder::new()
            .name(format!("arancini-worker-{}", worker_id))
            .spawn(move || {
                if let Some(core_id) = pinned_core {
                    let _ = core_affinity::set_for_current(core_id);
                }

                monoio::start::<monoio::LegacyDriver, _>(async move {
                    if let Err(err) = worker_loop(worker_id, host, tx).await {
                        error!("arancini worker {} failed: {}", worker_id, err);
                    }
                });
            })?;
    }

    Ok(())
}

async fn worker_loop(worker_id: usize, host: SocketAddr, tx: Sender<Update>) -> Result<()> {
    // SO_REUSEPORT is enabled to let the kernel spread accepted sessions across workers.
    let opts = ListenerOpts::new().reuse_port(true).reuse_addr(true);
    let listener = TcpListener::bind_with_config(host, &opts)?;
    info!("arancini worker {} listening on {}", worker_id, host);

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
        monoio::spawn(async move {
            if let Err(err) = handle_connection(stream, socket, tx).await {
                error!("arancini connection {} failed: {}", socket, err);
            }
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    socket: SocketAddr,
    tx: Sender<Update>,
) -> Result<()> {
    loop {
        let (res, common_header) = stream.read_exact(Box::new([0_u8; 6])).await;
        let common_header = match res {
            Ok(_) => common_header,
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::BrokenPipe
                ) =>
            {
                debug!("{}: arancini session closed ({})", socket, err);
                break;
            }
            Err(err) => anyhow::bail!("{}: failed to read BMP header: {}", socket, err),
        };

        let common_header = *common_header;
        let message_version = common_header[0];
        if message_version != 3 {
            anyhow::bail!("{}: unsupported BMP version {}", socket, message_version);
        }

        let packet_length = u32::from_be_bytes(common_header[1..5].try_into().unwrap()) as usize;
        if packet_length < 6 {
            anyhow::bail!("{}: invalid BMP packet length {}", socket, packet_length);
        }

        let message_type = common_header[5];
        if message_type > 6 {
            anyhow::bail!("{}: unsupported BMP message type {}", socket, message_type);
        }

        let mut packet = Vec::with_capacity(packet_length);
        packet.extend_from_slice(&common_header);

        if packet_length > 6 {
            let (res, body) = stream.read_exact(vec![0_u8; packet_length - 6]).await;
            match res {
                Ok(_) => packet.extend_from_slice(body.as_slice()),
                Err(err)
                    if matches!(
                        err.kind(),
                        std::io::ErrorKind::UnexpectedEof
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::BrokenPipe
                    ) =>
                {
                    debug!("{}: arancini session closed while reading body", socket);
                    break;
                }
                Err(err) => anyhow::bail!("{}: failed to read BMP packet body: {}", socket, err),
            }
        }

        trace!(
            "{}: arancini read BMP packet ({} bytes)",
            socket,
            packet_length
        );
        let mut bytes = Bytes::from(packet);
        process_bmp_message::<MemoryStore>(None, tx.clone(), socket, &mut bytes).await?;
    }

    Ok(())
}
