use anyhow::Result;
use clap::{Parser, ValueEnum};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use futures::future::try_join_all;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::net::lookup_host;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub runtime: RuntimeConfig,
    pub bmp: BMPConfig,
    pub kafka: KafkaConfig,
    pub nats: NatsConfig,
    pub curation: CurationConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SnapshotBackendArg {
    Auto,
    Local,
    NatsKv,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotBackend {
    Local,
    NatsKv,
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub arancini_workers: usize,
    pub arancini_bridge_buffer_size: usize,
}

#[derive(Debug, Clone)]
pub struct BMPConfig {
    pub host: SocketAddr,
    pub listener_backlog: i32,
    pub socket_recv_buffer_bytes: Option<usize>,
    pub arancini_fixed_slot_size_bytes: usize,
    pub arancini_fixed_slot_count: usize,
    pub arancini_max_frame_size_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub disable: bool,
    pub brokers: Vec<SocketAddr>,
    pub topic: String,
    pub auth_protocol: String,
    pub auth_sasl_username: String,
    pub auth_sasl_password: String,
    pub auth_sasl_mechanism: String,
    pub message_max_bytes: usize,
    pub message_timeout_ms: usize,
    pub batch_wait_time: u64,
    pub batch_wait_interval: u64,
    pub mpsc_buffer_size: usize,
}

#[derive(Debug, Clone)]
pub struct NatsConfig {
    pub enabled: bool,
    pub server: String,
    pub subject: String,
    pub max_in_flight_acks: usize,
    pub tls_required: bool,
    pub tls_first: bool,
    pub tls_ca_cert_path: Option<PathBuf>,
    pub tls_client_cert_path: Option<PathBuf>,
    pub tls_client_key_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct CurationConfig {
    pub enabled: bool,
    pub state_path: String,
    pub state_save_interval: u64,
    pub snapshot_backend: SnapshotBackend,
    pub snapshot_nats_kv_bucket: String,
    pub snapshot_nats_object_store_bucket: String,
    pub snapshot_sidecar_buffer_size: usize,
}

#[derive(Parser, Debug)]
#[command(author, version, about = "Arancini BGP Monitoring Protocol (BMP) collector", long_about = None)]
pub struct Cli {
    /// Number of monoio worker threads used by Arancini runtime mode
    #[arg(long, default_value_t = num_cpus::get())]
    pub arancini_workers: usize,

    /// Buffer capacity for Arancini monoio->tokio bridge channel
    #[arg(long, default_value_t = 100000)]
    pub arancini_bridge_buffer_size: usize,

    /// BMP listener address (IP or FQDN)
    #[arg(long, default_value = "0.0.0.0:4000")]
    pub bmp_address: String,

    /// TCP listen backlog for BMP ingress listener sockets
    #[arg(long, default_value_t = 1024)]
    pub bmp_listener_backlog: i32,

    /// Requested SO_RCVBUF size in bytes for BMP ingress sockets
    #[arg(long)]
    pub bmp_socket_recv_buffer_bytes: Option<usize>,

    /// Arancini fixed slot size in bytes for BMP ingress buffers
    #[arg(long, default_value_t = 64 * 1024)]
    pub arancini_fixed_slot_size_bytes: usize,

    /// Arancini fixed slot count per worker for BMP ingress buffers
    #[arg(long, default_value_t = 8)]
    pub arancini_fixed_slot_count: usize,

    /// Maximum accepted BMP frame size in bytes in Arancini mode
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    pub arancini_max_frame_size_bytes: usize,

    /// Kafka brokers (comma-separated list of address:port)
    #[arg(long, value_delimiter(','), default_value = "localhost:9092")]
    pub kafka_brokers: Vec<String>,

    /// Disable Kafka producer
    #[arg(long)]
    pub kafka_disable: bool,

    /// Kafka producer topic
    #[arg(long, default_value = "arancini-updates")]
    pub kafka_topic: String,

    /// Kafka Authentication Protocol (e.g., PLAINTEXT, SASL_PLAINTEXT)
    #[arg(long, default_value = "PLAINTEXT")]
    pub kafka_auth_protocol: String,

    /// Kafka Authentication SASL Username
    #[arg(long, default_value = "arancini")]
    pub kafka_auth_sasl_username: String,

    /// Kafka Authentication SASL Password
    #[arg(long, default_value = "arancini")]
    pub kafka_auth_sasl_password: String,

    /// Kafka Authentication SASL Mechanism (e.g., PLAIN, SCRAM-SHA-512)
    #[arg(long, default_value = "SCRAM-SHA-512")]
    pub kafka_auth_sasl_mechanism: String,

    /// Kafka message max bytes
    #[arg(long, default_value_t = 990000)]
    pub kafka_message_max_bytes: usize,

    /// Kafka producer batch size (bytes)
    #[arg(long, default_value_t = 500000)]
    pub kafka_message_timeout_ms: usize,

    /// Kafka producer batch wait time (ms)
    #[arg(long, default_value_t = 1000)]
    pub kafka_batch_wait_time: u64,

    /// Kafka producer batch wait interval (ms)
    #[arg(long, default_value_t = 100)]
    pub kafka_batch_wait_interval: u64,

    /// Kafka MPSC bufer size
    #[arg(long, default_value_t = 100000)]
    pub kafka_mpsc_buffer_size: usize,

    /// Enable NATS JetStream sidecar publishing path (Arancini mode)
    #[arg(long)]
    pub nats_enable: bool,

    /// NATS server URL
    #[arg(long, default_value = "nats://127.0.0.1:4222")]
    pub nats_server: String,

    /// NATS JetStream publish base subject prefix
    #[arg(long, default_value = "arancini.updates")]
    pub nats_subject: String,

    /// Max number of in-flight JetStream ACK futures handled by sidecar
    #[arg(long, default_value_t = 4096)]
    pub nats_max_in_flight_acks: usize,

    /// Require TLS for NATS connection
    #[arg(long)]
    pub nats_tls_required: bool,

    /// Use TLS-first handshake mode for NATS (server must support handshake_first)
    #[arg(long)]
    pub nats_tls_first: bool,

    /// PEM bundle path for NATS TLS root CA certificates
    #[arg(long)]
    pub nats_tls_ca_cert_path: Option<PathBuf>,

    /// PEM path for NATS client certificate (mTLS)
    #[arg(long)]
    pub nats_tls_client_cert_path: Option<PathBuf>,

    /// PEM path for NATS client private key (mTLS)
    #[arg(long)]
    pub nats_tls_client_key_path: Option<PathBuf>,

    /// Metrics listener address (IP or FQDN) for Prometheus endpoint
    #[arg(long, default_value = "0.0.0.0:8080")]
    pub metrics_address: String,

    /// Disable curation (deduplication and synthetic withdraws)
    /// When disabled, all BMP updates are forwarded as-is without state tracking
    #[arg(long)]
    pub curation_disable: bool,

    /// Path to save curation state file
    #[arg(long, default_value = "state.bin")]
    pub curation_state_path: String,

    /// Interval (in seconds) to save curation state to disk
    #[arg(long, default_value_t = 10)]
    pub curation_state_interval: u64,

    /// Snapshot backend mode for Arancini worker shard persistence
    /// "auto" selects NATS KV/Object Store when running on Kubernetes, local files otherwise
    #[arg(long, value_enum, default_value_t = SnapshotBackendArg::Auto)]
    pub curation_snapshot_backend: SnapshotBackendArg,

    /// NATS KV bucket name used to store shard snapshot manifests
    #[arg(long, default_value = "arancini-snapshots")]
    pub curation_snapshot_nats_kv_bucket: String,

    /// NATS Object Store bucket name used to store shard snapshot payload blobs
    #[arg(long, default_value = "arancini-snapshot-blobs")]
    pub curation_snapshot_nats_object_store_bucket: String,

    /// Snapshot sidecar command queue capacity for Arancini workers
    #[arg(long, default_value_t = 1024)]
    pub curation_snapshot_sidecar_buffer_size: usize,

    /// Set the verbosity level
    #[command(flatten)]
    pub verbose: Verbosity<InfoLevel>,
}

fn set_logging(cli: &Cli) -> Result<()> {
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_max_level(cli.verbose)
        .with_target(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

fn set_metrics(metrics_address: SocketAddr) {
    let prom_builder = PrometheusBuilder::new();
    prom_builder
        .with_http_listener(metrics_address)
        .install()
        .expect("Failed to install Prometheus metrics exporter");

    // Producer metrics
    metrics::describe_counter!(
        "risotto_kafka_messages_total",
        "Total number of Kafka messages produced"
    );

    // State metrics
    metrics::describe_counter!("risotto_state_dump_total", "Total number of state dumps");
    metrics::describe_counter!(
        "risotto_state_dump_duration_seconds",
        "Duration of state dump in seconds"
    );
    metrics::describe_gauge!("risotto_peer_established", "Peer established for a router");
    metrics::describe_gauge!("risotto_state_updates", "Number of updates in the state");

    // Statistics metrics
    metrics::describe_counter!(
        "risotto_bmp_messages_total",
        "Total number of BMP messages received"
    );
    metrics::describe_counter!(
        "risotto_bmp_parse_errors_total",
        "Total number of BMP messages that failed to parse"
    );
    metrics::describe_counter!(
        "risotto_rx_updates_total",
        "Total number of updates received"
    );
    metrics::describe_counter!(
        "risotto_tx_updates_total",
        "Total number of updates transmitted"
    );
    metrics::describe_counter!(
        "risotto_arancini_ingest_slot_growth_total",
        "Number of Arancini ingest slot ring capacity growth events"
    );
    metrics::describe_counter!(
        "risotto_arancini_ingest_frame_buffer_growth_total",
        "Number of Arancini ingest frame-buffer capacity growth events"
    );
    metrics::describe_counter!(
        "risotto_arancini_session_claim_total",
        "Total number of Arancini router-session ownership claims"
    );
    metrics::describe_counter!(
        "risotto_arancini_session_release_total",
        "Total number of Arancini router-session ownership releases"
    );
    metrics::describe_counter!(
        "risotto_arancini_session_ownership_conflicts_total",
        "Total number of Arancini router-session ownership conflicts across workers"
    );
    metrics::describe_counter!(
        "risotto_arancini_session_duplicate_claims_total",
        "Total number of duplicate Arancini router-session claims by the same worker"
    );
    metrics::describe_counter!(
        "risotto_arancini_session_ownership_assert_failures_total",
        "Total number of Arancini router-session ownership assertion failures"
    );
    metrics::describe_counter!(
        "risotto_arancini_bridge_tx_total",
        "Total number of updates sent into the Arancini monoio->tokio bridge"
    );
    metrics::describe_counter!(
        "risotto_arancini_bridge_rx_total",
        "Total number of updates drained from the Arancini monoio->tokio bridge"
    );
    metrics::describe_gauge!(
        "risotto_arancini_bridge_queue_fill_ratio",
        "Current fill ratio of the Arancini monoio->tokio bridge queue"
    );
    metrics::describe_gauge!(
        "risotto_arancini_bridge_queue_depth",
        "Current depth of the Arancini monoio->tokio bridge queue"
    );
    metrics::describe_gauge!(
        "risotto_arancini_snapshot_queue_fill_ratio",
        "Current fill ratio of the Arancini snapshot command queue"
    );
    metrics::describe_gauge!(
        "risotto_arancini_snapshot_queue_depth",
        "Current depth of the Arancini snapshot command queue"
    );
    metrics::describe_counter!(
        "risotto_arancini_snapshot_persist_total",
        "Total number of shard snapshots persisted by snapshot backend"
    );
    metrics::describe_counter!(
        "risotto_arancini_snapshot_restore_total",
        "Total number of shard snapshot restore requests served by snapshot backend"
    );
    metrics::describe_counter!(
        "risotto_arancini_snapshot_errors_total",
        "Total number of shard snapshot backend errors"
    );
    metrics::describe_counter!(
        "risotto_arancini_worker_rx_updates_total",
        "Total number of updates decoded per Arancini worker"
    );
    metrics::describe_counter!(
        "risotto_arancini_worker_tx_updates_total",
        "Total number of emitted updates per Arancini worker"
    );
    metrics::describe_histogram!(
        "risotto_arancini_snapshot_duration_seconds",
        "Elapsed seconds to complete per-worker shard snapshot persistence"
    );
    metrics::describe_counter!(
        "risotto_arancini_nats_publish_enqueued_total",
        "Total number of updates enqueued for JetStream publish"
    );
    metrics::describe_histogram!(
        "risotto_arancini_nats_publish_enqueue_latency_seconds",
        "Elapsed seconds to enqueue JetStream publish requests"
    );
    metrics::describe_counter!(
        "risotto_arancini_nats_publish_errors_total",
        "Total number of JetStream publish enqueue errors"
    );
    metrics::describe_counter!(
        "risotto_arancini_nats_ack_success_total",
        "Total number of successful JetStream publish acknowledgements"
    );
    metrics::describe_counter!(
        "risotto_arancini_nats_ack_failure_total",
        "Total number of failed JetStream publish acknowledgements"
    );
    metrics::describe_gauge!(
        "risotto_arancini_nats_ack_inflight",
        "Current number of in-flight JetStream publish acknowledgements"
    );
    metrics::describe_histogram!(
        "risotto_arancini_nats_ack_lag_seconds",
        "Elapsed seconds from publish enqueue to JetStream acknowledgement"
    );
}

pub async fn resolve_address(address: String) -> Result<SocketAddr> {
    match lookup_host(&address).await?.next() {
        Some(addr) => Ok(addr),
        None => anyhow::bail!("Failed to resolve address: {}", address),
    }
}

pub async fn configure() -> Result<AppConfig> {
    let cli = Cli::parse();

    // Set up tracing
    set_logging(&cli).map_err(|e| anyhow::anyhow!("Failed to set up logging: {}", e))?;

    if cli.nats_tls_client_cert_path.is_some() ^ cli.nats_tls_client_key_path.is_some() {
        anyhow::bail!(
            "both --nats-tls-client-cert-path and --nats-tls-client-key-path must be provided for mTLS"
        );
    }

    // Resolve addresses
    let (bmp_addr, metrics_addr) = tokio::try_join!(
        resolve_address(cli.bmp_address),
        resolve_address(cli.metrics_address)
    )
    .map_err(|e| anyhow::anyhow!("Failed during initial address resolution: {}", e))?;

    let resolved_kafka_brokers = if !cli.kafka_disable {
        try_join_all(
            cli.kafka_brokers
                .iter()
                .map(|broker| resolve_address(broker.clone())),
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to resolve Kafka broker: {}", e))?
    } else {
        Vec::new()
    };

    let snapshot_backend = match cli.curation_snapshot_backend {
        SnapshotBackendArg::Auto => {
            if std::env::var_os("KUBERNETES_SERVICE_HOST").is_some() {
                SnapshotBackend::NatsKv
            } else {
                SnapshotBackend::Local
            }
        }
        SnapshotBackendArg::Local => SnapshotBackend::Local,
        SnapshotBackendArg::NatsKv => SnapshotBackend::NatsKv,
    };

    // Set up metrics
    set_metrics(metrics_addr);

    Ok(AppConfig {
        runtime: RuntimeConfig {
            arancini_workers: cli.arancini_workers,
            arancini_bridge_buffer_size: cli.arancini_bridge_buffer_size,
        },
        bmp: BMPConfig {
            host: bmp_addr,
            listener_backlog: cli.bmp_listener_backlog,
            socket_recv_buffer_bytes: cli.bmp_socket_recv_buffer_bytes,
            arancini_fixed_slot_size_bytes: cli.arancini_fixed_slot_size_bytes,
            arancini_fixed_slot_count: cli.arancini_fixed_slot_count,
            arancini_max_frame_size_bytes: cli.arancini_max_frame_size_bytes,
        },
        kafka: KafkaConfig {
            disable: cli.kafka_disable,
            brokers: resolved_kafka_brokers,
            topic: cli.kafka_topic,
            auth_protocol: cli.kafka_auth_protocol,
            auth_sasl_username: cli.kafka_auth_sasl_username,
            auth_sasl_password: cli.kafka_auth_sasl_password,
            auth_sasl_mechanism: cli.kafka_auth_sasl_mechanism,
            message_max_bytes: cli.kafka_message_max_bytes,
            message_timeout_ms: cli.kafka_message_timeout_ms,
            batch_wait_time: cli.kafka_batch_wait_time,
            batch_wait_interval: cli.kafka_batch_wait_interval,
            mpsc_buffer_size: cli.kafka_mpsc_buffer_size,
        },
        nats: NatsConfig {
            enabled: cli.nats_enable,
            server: cli.nats_server,
            subject: cli.nats_subject,
            max_in_flight_acks: cli.nats_max_in_flight_acks,
            tls_required: cli.nats_tls_required,
            tls_first: cli.nats_tls_first,
            tls_ca_cert_path: cli.nats_tls_ca_cert_path,
            tls_client_cert_path: cli.nats_tls_client_cert_path,
            tls_client_key_path: cli.nats_tls_client_key_path,
        },
        curation: CurationConfig {
            enabled: !cli.curation_disable,
            state_path: cli.curation_state_path,
            state_save_interval: cli.curation_state_interval,
            snapshot_backend,
            snapshot_nats_kv_bucket: cli.curation_snapshot_nats_kv_bucket,
            snapshot_nats_object_store_bucket: cli.curation_snapshot_nats_object_store_bucket,
            snapshot_sidecar_buffer_size: cli.curation_snapshot_sidecar_buffer_size,
        },
    })
}
