use anyhow::Result;
use async_nats::{Client, ConnectOptions};

use crate::config::NatsConfig;

pub(crate) async fn connect(cfg: &NatsConfig) -> Result<Client> {
    let mut options = ConnectOptions::new();

    let has_tls_material = cfg.tls_ca_cert_path.is_some()
        || (cfg.tls_client_cert_path.is_some() && cfg.tls_client_key_path.is_some());

    if cfg.tls_first {
        options = options.tls_first();
    } else if cfg.tls_required || has_tls_material {
        options = options.require_tls(true);
    }

    if let Some(path) = &cfg.tls_ca_cert_path {
        options = options.add_root_certificates(path.clone());
    }

    if let (Some(cert), Some(key)) = (&cfg.tls_client_cert_path, &cfg.tls_client_key_path) {
        options = options.add_client_certificate(cert.clone(), key.clone());
    }

    options
        .connect(cfg.server.clone())
        .await
        .map_err(|err| anyhow::anyhow!("failed to connect to NATS at {}: {}", cfg.server, err))
}
