mod arancini;
mod bridge;
mod config;
mod nats;
mod producer;
mod serializer;
mod update_capnp;

use anyhow::Result;
use futures::future::pending;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver};
use tokio_graceful::Shutdown;
use tracing::{debug, error, trace, warn};

use arancini_lib::update::Update;

use crate::bridge::BridgeSender;
use crate::config::{configure, AppConfig};

async fn arancini_handler(cfg: Arc<AppConfig>, tx: BridgeSender) {
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

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = Arc::new(configure().await?);
    trace!("{:?}", cfg);

    let curation_config = cfg.curation.clone();
    let shutdown: Shutdown = Shutdown::default();

    if curation_config.enabled {
        debug!("arancini curation enabled with per-worker shard state");
    } else {
        debug!("curation is disabled - forwarding all updates as-is");
    }

    // Producer ingress channel
    let (producer_tx, rx) = channel(cfg.kafka.mpsc_buffer_size);

    // Initialize tasks
    let (bridge_tx, bridge_rx) = bridge::bounded_channel(cfg.runtime.arancini_bridge_buffer_size);
    if let Some(capacity) = bridge_tx.queue_capacity() {
        debug!("arancini bridge queue capacity set to {}", capacity);
    }
    if cfg.nats.enabled {
        if let Err(err) = bridge::spawn_nats_sidecar_thread(bridge_rx, cfg.nats.clone()) {
            anyhow::bail!("failed to start arancini NATS sidecar thread: {}", err);
        }
    } else {
        warn!("arancini running without NATS sidecar enabled; using Kafka forwarder bridge");
        if let Err(err) = bridge::spawn_sidecar_thread(bridge_rx, producer_tx.clone()) {
            anyhow::bail!("failed to start arancini sidecar thread: {}", err);
        }
    }

    let ingress_task = shutdown.spawn_task(arancini_handler(cfg.clone(), bridge_tx));
    let producer_task = if cfg.nats.enabled {
        shutdown.spawn_task(async { pending::<()>().await })
    } else {
        shutdown.spawn_task(producer_handler(cfg.clone(), rx))
    };
    tokio::select! {
        biased;
        _ = shutdown.shutdown_with_limit(Duration::from_secs(1)) => {}
        _ = ingress_task => {}
        _ = producer_task => {}
    }

    Ok(())
}
