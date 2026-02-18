use anyhow::Result;
use async_nats::jetstream;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(about = "JetStream stream bootstrap helper")]
struct Cli {
    #[arg(long, default_value = "nats://127.0.0.1:4222")]
    server: String,
    #[arg(long, default_value = "ARANCINI_UPDATES")]
    stream: String,
    #[arg(long, default_value = "arancini.updates.>")]
    subject: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = async_nats::connect(cli.server.clone())
        .await
        .map_err(|err| anyhow::anyhow!("failed to connect to {}: {}", cli.server, err))?;
    let js = jetstream::new(client);

    match js.get_stream(&cli.stream).await {
        Ok(_) => {
            println!("stream '{}' already exists", cli.stream);
        }
        Err(_) => {
            js.create_stream(jetstream::stream::Config {
                name: cli.stream.clone(),
                subjects: vec![cli.subject.clone()],
                ..Default::default()
            })
            .await
            .map_err(|err| anyhow::anyhow!("failed to create stream '{}': {}", cli.stream, err))?;
            println!(
                "created stream '{}' for subject '{}'",
                cli.stream, cli.subject
            );
        }
    }

    Ok(())
}
