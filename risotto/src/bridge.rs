use anyhow::Result;
use crossfire::mpsc;
use crossfire::{AsyncRxTrait, AsyncTxTrait};
use metrics::{counter, gauge};
use risotto_lib::sender::UpdateSender;
use risotto_lib::update::Update;
use std::future::Future;
use std::pin::Pin;
use std::thread;
use tokio::sync::mpsc::Sender;
use tracing::debug;

type BridgeFlavor = mpsc::Array<Update>;
type BridgeTx = crossfire::MAsyncTx<BridgeFlavor>;
type BridgeRx = crossfire::AsyncRx<BridgeFlavor>;

#[derive(Clone)]
pub(crate) struct BridgeSender {
    tx: BridgeTx,
}

impl BridgeSender {
    pub(crate) fn new(tx: BridgeTx) -> Self {
        Self { tx }
    }

    pub(crate) fn queue_capacity(&self) -> Option<usize> {
        self.tx.capacity()
    }
}

impl UpdateSender for BridgeSender {
    fn send<'a>(
        &'a self,
        update: Update,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            self.tx
                .send(update)
                .await
                .map_err(|err| anyhow::anyhow!("failed to send update to arancini bridge: {}", err))?;
            if let Some(capacity) = self.tx.capacity() {
                gauge!("risotto_arancini_bridge_queue_fill_ratio")
                    .set(self.tx.len() as f64 / capacity as f64);
            }
            counter!("risotto_arancini_bridge_tx_total").increment(1);
            Ok(())
        })
    }
}

pub(crate) fn bounded_channel(size: usize) -> (BridgeSender, BridgeRx) {
    let (tx, rx) = mpsc::bounded_async(size);
    (BridgeSender::new(tx), rx)
}

pub(crate) async fn sidecar_forwarder(rx: BridgeRx, tx: Sender<Update>) -> Result<()> {
    debug!("starting arancini tokio sidecar forwarder");
    loop {
        let update = match rx.recv().await {
            Ok(update) => update,
            Err(_) => {
                debug!("arancini bridge receiver closed");
                break;
            }
        };

        tx.send(update)
            .await
            .map_err(|err| anyhow::anyhow!("failed forwarding bridge update: {}", err))?;
        counter!("risotto_arancini_bridge_rx_total").increment(1);
        if let Some(capacity) = rx.capacity() {
            gauge!("risotto_arancini_bridge_queue_fill_ratio").set(rx.len() as f64 / capacity as f64);
        }
    }
    Ok(())
}

pub(crate) fn spawn_sidecar_thread(rx: BridgeRx, tx: Sender<Update>) -> Result<()> {
    thread::Builder::new()
        .name("arancini-sidecar".to_string())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(err) => {
                    tracing::error!("failed to build arancini sidecar runtime: {}", err);
                    return;
                }
            };

            runtime.block_on(async move {
                if let Err(err) = sidecar_forwarder(rx, tx).await {
                    tracing::error!("arancini sidecar forwarder failed: {}", err);
                }
            });
        })
        .map(|_| ())
        .map_err(|err| anyhow::anyhow!("failed to spawn arancini sidecar thread: {}", err))
}
