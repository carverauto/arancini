use anyhow::Result;
use async_nats::jetstream;
use crossfire::mpsc;
use crossfire::{AsyncRxTrait, AsyncTxTrait};
use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;
use metrics::{counter, gauge, histogram};
use risotto_lib::sender::UpdateSender;
use risotto_lib::update::Update;
use std::future::Future;
use std::net::IpAddr;
use std::pin::Pin;
use std::thread;
use std::time::Instant;
use tokio::sync::mpsc::Sender;
use tracing::{debug, error};

use crate::config::NatsConfig;
use crate::serializer::serialize_update;

type BridgeFlavor = mpsc::Array<Update>;
type BridgeTx = crossfire::MAsyncTx<BridgeFlavor>;
type BridgeRx = crossfire::AsyncRx<BridgeFlavor>;
type AckWaiter = Pin<Box<dyn Future<Output = (Instant, Result<()>)> + Send>>;

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
    fn send<'a>(&'a self, update: Update) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            self.tx.send(update).await.map_err(|err| {
                anyhow::anyhow!("failed to send update to arancini bridge: {}", err)
            })?;
            if let Some(capacity) = self.tx.capacity() {
                gauge!("risotto_arancini_bridge_queue_fill_ratio")
                    .set(self.tx.len() as f64 / capacity as f64);
            }
            gauge!("risotto_arancini_bridge_queue_depth").set(self.tx.len() as f64);
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
            gauge!("risotto_arancini_bridge_queue_fill_ratio")
                .set(rx.len() as f64 / capacity as f64);
        }
        gauge!("risotto_arancini_bridge_queue_depth").set(rx.len() as f64);
    }
    Ok(())
}

fn nats_subject_for_update(base_subject: &str, update: &Update) -> String {
    let router_ip = router_ip_subject_token(update.router_addr);
    let afi_safi = afi_safi_subject_token(update);
    format!(
        "{}.{}.{}.{}",
        base_subject, router_ip, update.peer_asn, afi_safi
    )
}

fn router_ip_subject_token(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => {
            let [a, b, c, d] = v4.octets();
            format!("v4_{}_{}_{}_{}", a, b, c, d)
        }
        IpAddr::V6(v6) => {
            if let Some(v4_mapped) = v6.to_ipv4_mapped() {
                return router_ip_subject_token(IpAddr::V4(v4_mapped));
            }

            let segments = v6.segments();
            format!(
                "v6_{:x}_{:x}_{:x}_{:x}_{:x}_{:x}_{:x}_{:x}",
                segments[0],
                segments[1],
                segments[2],
                segments[3],
                segments[4],
                segments[5],
                segments[6],
                segments[7]
            )
        }
    }
}

fn afi_safi_subject_token(update: &Update) -> String {
    let (afi, safi) = if update.announced {
        (update.attrs.mp_reach_afi, update.attrs.mp_reach_safi)
    } else {
        (update.attrs.mp_unreach_afi, update.attrs.mp_unreach_safi)
    };

    let afi = afi.unwrap_or(match update.prefix_addr {
        IpAddr::V4(_) => 1u16,
        IpAddr::V6(_) => 2u16,
    });
    let safi = safi.unwrap_or(1u8);

    format!("{}_{}", afi, safi)
}

async fn nats_sidecar_publisher(rx: BridgeRx, cfg: NatsConfig) -> Result<()> {
    let client = crate::nats::connect(&cfg).await?;
    let jetstream = jetstream::new(client);
    let mut in_flight_acks: FuturesUnordered<AckWaiter> = FuturesUnordered::new();

    debug!(
        "starting arancini NATS sidecar publisher to {} subject {}",
        cfg.server, cfg.subject
    );

    loop {
        while let Some((enqueued_at, ack_result)) = in_flight_acks.next().now_or_never().flatten() {
            match ack_result {
                Ok(_) => {
                    counter!("risotto_arancini_nats_ack_success_total").increment(1);
                    histogram!("risotto_arancini_nats_ack_lag_seconds")
                        .record(enqueued_at.elapsed().as_secs_f64());
                }
                Err(err) => {
                    counter!("risotto_arancini_nats_ack_failure_total").increment(1);
                    error!("JetStream ACK failed: {}", err);
                }
            }
            gauge!("risotto_arancini_nats_ack_inflight").set(in_flight_acks.len() as f64);
        }

        let update = match rx.recv().await {
            Ok(update) => update,
            Err(_) => {
                debug!("arancini bridge receiver closed");
                break;
            }
        };
        counter!("risotto_arancini_bridge_rx_total").increment(1);
        if let Some(capacity) = rx.capacity() {
            gauge!("risotto_arancini_bridge_queue_fill_ratio")
                .set(rx.len() as f64 / capacity as f64);
        }
        gauge!("risotto_arancini_bridge_queue_depth").set(rx.len() as f64);

        let subject = nats_subject_for_update(&cfg.subject, &update);
        let payload = serialize_update(&update);
        let enqueued_at = Instant::now();
        let publish_start = Instant::now();
        match jetstream.publish(subject.clone(), payload.into()).await {
            Ok(ack) => {
                counter!("risotto_arancini_nats_publish_enqueued_total").increment(1);
                histogram!("risotto_arancini_nats_publish_enqueue_latency_seconds")
                    .record(publish_start.elapsed().as_secs_f64());
                in_flight_acks.push(Box::pin(async move {
                    let res = ack
                        .await
                        .map(|_| ())
                        .map_err(|err| anyhow::anyhow!("JetStream publish ACK error: {}", err));
                    (enqueued_at, res)
                }));
                gauge!("risotto_arancini_nats_ack_inflight").set(in_flight_acks.len() as f64);
            }
            Err(err) => {
                counter!("risotto_arancini_nats_publish_errors_total").increment(1);
                histogram!("risotto_arancini_nats_publish_enqueue_latency_seconds")
                    .record(publish_start.elapsed().as_secs_f64());
                error!(
                    "JetStream publish enqueue failed for subject {}: {}",
                    subject, err
                );
            }
        }

        while in_flight_acks.len() >= cfg.max_in_flight_acks {
            if let Some((enqueued_at, ack_result)) = in_flight_acks.next().await {
                match ack_result {
                    Ok(_) => {
                        counter!("risotto_arancini_nats_ack_success_total").increment(1);
                        histogram!("risotto_arancini_nats_ack_lag_seconds")
                            .record(enqueued_at.elapsed().as_secs_f64());
                    }
                    Err(err) => {
                        counter!("risotto_arancini_nats_ack_failure_total").increment(1);
                        error!("JetStream ACK failed while draining inflight: {}", err);
                    }
                }
                gauge!("risotto_arancini_nats_ack_inflight").set(in_flight_acks.len() as f64);
            }
        }
    }

    while let Some((enqueued_at, ack_result)) = in_flight_acks.next().await {
        match ack_result {
            Ok(_) => {
                counter!("risotto_arancini_nats_ack_success_total").increment(1);
                histogram!("risotto_arancini_nats_ack_lag_seconds")
                    .record(enqueued_at.elapsed().as_secs_f64());
            }
            Err(err) => {
                counter!("risotto_arancini_nats_ack_failure_total").increment(1);
                error!("JetStream ACK failed during drain: {}", err);
            }
        }
    }
    gauge!("risotto_arancini_nats_ack_inflight").set(0.0);

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

pub(crate) fn spawn_nats_sidecar_thread(rx: BridgeRx, cfg: NatsConfig) -> Result<()> {
    thread::Builder::new()
        .name("arancini-sidecar-nats".to_string())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(err) => {
                    tracing::error!("failed to build arancini nats sidecar runtime: {}", err);
                    return;
                }
            };

            runtime.block_on(async move {
                if let Err(err) = nats_sidecar_publisher(rx, cfg).await {
                    tracing::error!("arancini NATS sidecar failed: {}", err);
                }
            });
        })
        .map(|_| ())
        .map_err(|err| anyhow::anyhow!("failed to spawn arancini NATS sidecar thread: {}", err))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use futures::FutureExt;
    use risotto_lib::update::UpdateAttributes;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Duration};

    fn test_update(router_addr: IpAddr, prefix_addr: IpAddr, announced: bool) -> Update {
        Update {
            time_received_ns: Utc::now(),
            time_bmp_header_ns: Utc::now(),
            router_addr,
            router_port: 4000,
            peer_addr: IpAddr::V4(Ipv4Addr::new(198, 51, 100, 1)),
            peer_bgp_id: Ipv4Addr::new(198, 51, 100, 1),
            peer_asn: 64512,
            prefix_addr,
            prefix_len: 24,
            is_post_policy: false,
            is_adj_rib_out: false,
            announced,
            synthetic: false,
            attrs: Arc::new(UpdateAttributes::default()),
        }
    }

    #[test]
    fn nats_subject_uses_schema_fields() {
        let mut update = test_update(
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)),
            IpAddr::V4(Ipv4Addr::new(203, 0, 113, 0)),
            true,
        );
        let attrs = UpdateAttributes {
            mp_reach_afi: Some(1),
            mp_reach_safi: Some(1),
            ..Default::default()
        };
        update.attrs = Arc::new(attrs);

        let subject = nats_subject_for_update("arancini.updates", &update);
        assert_eq!(subject, "arancini.updates.v4_192_0_2_10.64512.1_1");
    }

    #[test]
    fn nats_subject_normalizes_ipv4_mapped_ipv6_router() {
        let mut update = test_update(
            IpAddr::V6(Ipv4Addr::new(203, 0, 113, 9).to_ipv6_mapped()),
            IpAddr::V4(Ipv4Addr::new(203, 0, 113, 0)),
            true,
        );
        let attrs = UpdateAttributes {
            mp_reach_afi: Some(1),
            mp_reach_safi: Some(1),
            ..Default::default()
        };
        update.attrs = Arc::new(attrs);

        let subject = nats_subject_for_update("arancini.updates", &update);
        assert_eq!(subject, "arancini.updates.v4_203_0_113_9.64512.1_1");
    }

    #[test]
    fn nats_subject_uses_unreach_for_withdraws() {
        let mut update = test_update(
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            false,
        );
        let attrs = UpdateAttributes {
            mp_unreach_afi: Some(2),
            mp_unreach_safi: Some(128),
            ..Default::default()
        };
        update.attrs = Arc::new(attrs);

        let subject = nats_subject_for_update("arancini.updates", &update);
        assert_eq!(subject, "arancini.updates.v6_0_0_0_0_0_0_0_1.64512.2_128");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bridge_sender_applies_backpressure_when_bounded_queue_is_full() {
        let (sender, rx) = bounded_channel(1);
        sender
            .send(test_update(
                IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
                IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)),
                true,
            ))
            .await
            .expect("first send should succeed");

        let mut second_send = sender.send(test_update(
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2)),
            IpAddr::V4(Ipv4Addr::new(203, 0, 113, 2)),
            true,
        ));
        assert!(
            second_send.as_mut().now_or_never().is_none(),
            "second send should be pending while queue is full"
        );

        // Drain one item and ensure the pending sender can make progress.
        let _ = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("drain should not timeout")
            .expect("bridge receiver should still be open");
        timeout(Duration::from_secs(1), second_send)
            .await
            .expect("second send completion should not timeout")
            .expect("second send should succeed once capacity is available");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sidecar_forwarder_returns_error_if_downstream_channel_is_closed() {
        let (bridge_tx, bridge_rx) = bounded_channel(4);
        let (downstream_tx, downstream_rx) = mpsc::channel(1);
        drop(downstream_rx);

        let forwarder = tokio::spawn(sidecar_forwarder(bridge_rx, downstream_tx));
        bridge_tx
            .send(test_update(
                IpAddr::V4(Ipv4Addr::new(198, 51, 100, 10)),
                IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
                true,
            ))
            .await
            .expect("bridge enqueue should succeed");

        let result = timeout(Duration::from_secs(1), forwarder)
            .await
            .expect("forwarder completion should not timeout")
            .expect("forwarder join should succeed");
        assert!(
            result.is_err(),
            "forwarder should return error when downstream channel is closed"
        );
    }
}
