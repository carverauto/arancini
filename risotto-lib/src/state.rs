use anyhow::Result;
use chrono::Utc;
use core::net::IpAddr;
use metrics::{counter, gauge};
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{debug, trace};

use crate::sender::UpdateSender;
use crate::state_store::store::StateStore;
use crate::update::{map_to_ipv6, Update, UpdateAttributes, UpdateMetadata};

pub type AsyncState<T> = Arc<Mutex<State<T>>>;
pub type RouterPeerUpdate = (IpAddr, IpAddr, TimedPrefix);

pub fn new_state<T: StateStore + Send>(store: T) -> AsyncState<T> {
    Arc::new(Mutex::new(State::new(store)))
}

pub struct State<T: StateStore> {
    pub store: T,
}

impl<T: StateStore> State<T> {
    pub fn new(store: T) -> State<T> {
        State { store }
    }

    pub fn get_updates_by_peer(
        &self,
        router_addr: &IpAddr,
        peer_addr: &IpAddr,
    ) -> Result<Vec<TimedPrefix>> {
        Ok(self.store.get_updates_by_peer(router_addr, peer_addr))
    }

    pub fn add_peer(&mut self, router_addr: &IpAddr, peer_addr: &IpAddr) -> Result<()> {
        self.store.add_peer(router_addr, peer_addr);
        Ok(())
    }

    pub fn remove_peer(&mut self, router_addr: &IpAddr, peer_addr: &IpAddr) -> Result<()> {
        self.store.remove_peer(router_addr, peer_addr);
        gauge!(
            "risotto_state_updates",
            "router" => router_addr.to_string(),
            "peer" => peer_addr.to_string()
        )
        .set(0);
        Ok(())
    }

    pub fn update(
        &mut self,
        router_addr: &IpAddr,
        peer_addr: &IpAddr,
        update: &Update,
    ) -> Result<bool> {
        let emit = self.store.update(router_addr, &peer_addr, update);
        if emit {
            let delta = if update.announced { 1.0 } else { -1.0 };
            gauge!(
                "risotto_state_updates",
                "router" => router_addr.to_string(),
                "peer" => peer_addr.to_string()
            )
            .increment(delta);
        }
        Ok(emit)
    }
}

#[derive(Serialize, Deserialize, Eq, Clone)]
pub struct TimedPrefix {
    pub prefix_addr: IpAddr,
    pub prefix_len: u8,
    pub is_post_policy: bool,
    pub is_adj_rib_out: bool,
    pub timestamp: i64,
}

impl PartialEq for TimedPrefix {
    fn eq(&self, other: &Self) -> bool {
        self.prefix_addr == other.prefix_addr
            && self.prefix_len == other.prefix_len
            && self.is_post_policy == other.is_post_policy
            && self.is_adj_rib_out == other.is_adj_rib_out
    }
}

impl Hash for TimedPrefix {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.prefix_addr.hash(state);
        self.prefix_len.hash(state);
        self.is_post_policy.hash(state);
        self.is_adj_rib_out.hash(state);
    }
}

pub fn synthesize_withdraw_update(prefix: &TimedPrefix, metadata: UpdateMetadata) -> Update {
    Update {
        time_received_ns: Utc::now(),
        time_bmp_header_ns: Utc::now(),
        router_addr: map_to_ipv6(metadata.router_socket.ip()),
        router_port: metadata.router_socket.port(),
        peer_addr: map_to_ipv6(metadata.peer_addr),
        peer_bgp_id: metadata.peer_bgp_id,
        peer_asn: metadata.peer_asn,
        prefix_addr: map_to_ipv6(prefix.prefix_addr),
        prefix_len: prefix.prefix_len,
        is_post_policy: prefix.is_post_policy,
        is_adj_rib_out: prefix.is_adj_rib_out,
        announced: false,
        synthetic: true,
        attrs: Arc::new(UpdateAttributes::synthetic_withdraw()),
    }
}

pub async fn peer_up_withdraws_handler<T: StateStore, S: UpdateSender>(
    state: AsyncState<T>,
    tx: S,
    metadata: UpdateMetadata,
    sleep_time: u64,
) -> Result<()> {
    let startup = chrono::Utc::now();
    sleep(Duration::from_secs(sleep_time)).await;

    debug!(
        "[{} - {} - removing updates older than {} after waited {} seconds",
        metadata.router_socket, metadata.peer_addr, startup, sleep_time
    );

    let state_lock = state.lock().await;
    let timed_prefixes = state_lock
        .store
        .get_updates_by_peer(&metadata.router_socket.ip(), &metadata.peer_addr);

    drop(state_lock);

    let mut stale_prefixes = Vec::new();
    for timed_prefix in timed_prefixes {
        if timed_prefix.timestamp < startup.timestamp_millis() {
            stale_prefixes.push(timed_prefix);
        }
    }

    debug!(
        "[{} - {} - emitting {} synthetic withdraw updates",
        metadata.router_socket,
        metadata.peer_addr,
        stale_prefixes.len()
    );

    counter!("risotto_tx_updates_total").increment(stale_prefixes.len() as u64);

    for prefix in &stale_prefixes {
        let update = synthesize_withdraw_update(prefix, metadata.clone());
        trace!("{:?}", update);
        tx.send(update).await?;
    }

    // Remove updates from state after sends complete; do not await while holding the lock.
    let mut state_lock = state.lock().await;
    for prefix in &stale_prefixes {
        let update = synthesize_withdraw_update(prefix, metadata.clone());
        state_lock
            .store
            .update(&update.router_addr, &metadata.peer_addr, &update);
    }

    Ok(())
}

/// Process pre-parsed updates through the state machine
/// This is the core processing logic used by both collectors and curators
pub async fn process_updates<T: StateStore, S: UpdateSender>(
    state: Option<AsyncState<T>>,
    tx: S,
    updates: Vec<Update>,
) -> Result<()> {
    if updates.is_empty() {
        return Ok(());
    }

    match state {
        Some(state) => {
            // Gather emit candidates while locked.
            let mut to_emit = Vec::new();
            {
                let mut state_lock = state.lock().await;
                for update in updates {
                    debug!("Processing update: {:?}", update);
                    let should_emit =
                        state_lock.update(&update.router_addr, &update.peer_addr, &update)?;
                    if should_emit {
                        to_emit.push(update);
                    }
                }
            }

            // Emit after lock release so slow channel sends cannot stall state updates.
            for update in to_emit {
                trace!("Emitting update: {:?}", update);
                tx.send(update).await?;

                counter!("risotto_tx_updates_total").increment(1);
            }
        }
        None => {
            // Stateless mode: forward all updates as-is
            for update in updates {
                trace!("Forwarding update: {:?}", update);

                counter!("risotto_tx_updates_total").increment(1);

                tx.send(update).await?;
            }
        }
    }

    Ok(())
}

/// Process a single pre-parsed update through the state machine
/// Convenience wrapper for single update processing
pub async fn process_update<T: StateStore, S: UpdateSender>(
    state: Option<AsyncState<T>>,
    tx: S,
    update: Update,
) -> Result<()> {
    process_updates(state, tx, vec![update]).await
}
