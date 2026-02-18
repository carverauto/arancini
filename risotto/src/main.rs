mod arancini;
mod bmp;
mod bridge;
mod config;
mod producer;
mod serializer;
mod state;
mod update_capnp;

use anyhow::Result;
use futures::future::pending;
use socket2::{Domain, Protocol, SockRef, Socket, Type};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_graceful::Shutdown;
use tracing::{debug, error, trace, warn};

use risotto_lib::state::{new_state, AsyncState};
use risotto_lib::state_store::memory::MemoryStore;
use risotto_lib::state_store::store::StateStore;
use risotto_lib::update::Update;

use crate::bridge::BridgeSender;
use crate::config::{configure, AppConfig, BMPConfig, RuntimeMode};

fn build_tokio_bmp_listener(cfg: &BMPConfig) -> Result<TcpListener> {
    let domain = match cfg.host {
        std::net::SocketAddr::V4(_) => Domain::IPV4,
        std::net::SocketAddr::V6(_) => Domain::IPV6,
    };

    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;
    if let Some(recv_buf_size) = cfg.socket_recv_buffer_bytes {
        socket.set_recv_buffer_size(recv_buf_size)?;
    }

    socket.bind(&cfg.host.into())?;
    socket.listen(cfg.listener_backlog)?;
    socket.set_nonblocking(true)?;

    let std_listener: std::net::TcpListener = socket.into();
    Ok(TcpListener::from_std(std_listener)?)
}

async fn bmp_handler<T: StateStore>(
    state: Option<AsyncState<T>>,
    cfg: Arc<AppConfig>,
    tx: Sender<Update>,
) {
    let bmp_config = cfg.bmp.clone();

    debug!("binding bmp listener to {}", bmp_config.host);
    let bmp_listener = match build_tokio_bmp_listener(&bmp_config) {
        Ok(listener) => listener,
        Err(err) => {
            error!("failed to build BMP listener: {}", err);
            return;
        }
    };

    loop {
        let (mut bmp_stream, socket) = match bmp_listener.accept().await {
            Ok(incoming) => incoming,
            Err(err) => {
                error!("failed to accept BMP connection: {}", err);
                continue;
            }
        };
        if let Err(err) = bmp_stream.set_nodelay(true) {
            warn!("{}: failed to set TCP_NODELAY: {}", socket, err);
        }
        if let Some(recv_buf_size) = bmp_config.socket_recv_buffer_bytes {
            let socket_ref = SockRef::from(&bmp_stream);
            if let Err(err) = socket_ref.set_recv_buffer_size(recv_buf_size) {
                warn!(
                    "{}: failed to set SO_RCVBUF={} on accepted socket: {}",
                    socket, recv_buf_size, err
                );
            }
        }

        let bmp_state = state.clone();
        let tx = tx.clone();

        // Spawn a new task for the BMP connection with TCP stream
        tokio::spawn(async move {
            if let Err(err) = bmp::handle(&mut bmp_stream, bmp_state, tx).await {
                error!("Error handling BMP connection: {}", err);
            }

            drop(bmp_stream);
        });
    }
}

async fn arancini_handler(cfg: Arc<AppConfig>, tx: BridgeSender) {
    if cfg.curation.enabled {
        error!("arancini runtime currently requires curation to be disabled (--curation-disable)");
        return;
    }

    if let Err(err) = arancini::spawn_workers(cfg, tx) {
        error!("failed to spawn arancini workers: {}", err);
        return;
    }

    pending::<()>().await;
}

async fn producer_handler(cfg: Arc<AppConfig>, rx: Receiver<Update>) {
    let kafka_config = cfg.kafka.clone();
    if let Err(err) = producer::handle(&kafka_config, rx).await {
        error!("Error handling Kafka producer: {}", err);
    }
}

async fn curation_state_handler<T: StateStore + serde::Serialize>(
    state: AsyncState<T>,
    cfg: Arc<AppConfig>,
) {
    let curation_config = cfg.curation.clone();
    if let Err(err) = state::dump_handler(state.clone(), curation_config).await {
        error!("Error dumping curation state: {}", err);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = Arc::new(configure().await?);
    trace!("{:?}", cfg);

    let curation_config = cfg.curation.clone();
    let shutdown: Shutdown = Shutdown::default();

    // Initialize curation state if enabled
    let state = if curation_config.enabled {
        debug!("curation is enabled");
        let store = MemoryStore::new();
        let state = new_state(store);
        state::load(state.clone(), curation_config.clone()).await;
        shutdown.spawn_task(curation_state_handler(state.clone(), cfg.clone()));
        Some(state)
    } else {
        debug!("curation is disabled - forwarding all updates as-is");
        None
    };

    // Producer ingress channel
    let (producer_tx, rx) = channel(cfg.kafka.mpsc_buffer_size);

    // Initialize tasks
    let ingress_task = if cfg.runtime.mode == RuntimeMode::Arancini {
        let (bridge_tx, bridge_rx) = bridge::bounded_channel(cfg.runtime.arancini_bridge_buffer_size);
        if let Some(capacity) = bridge_tx.queue_capacity() {
            debug!("arancini bridge queue capacity set to {}", capacity);
        }
        if let Err(err) = bridge::spawn_sidecar_thread(bridge_rx, producer_tx.clone()) {
            anyhow::bail!("failed to start arancini sidecar thread: {}", err);
        }

        shutdown.spawn_task(arancini_handler(cfg.clone(), bridge_tx))
    } else {
        shutdown.spawn_task(bmp_handler(state.clone(), cfg.clone(), producer_tx.clone()))
    };
    let producer_task = shutdown.spawn_task(producer_handler(cfg.clone(), rx));
    tokio::select! {
        biased;
        _ = shutdown.shutdown_with_limit(Duration::from_secs(1)) => {}
        _ = ingress_task => {}
        _ = producer_task => {}
    }

    Ok(())
}
