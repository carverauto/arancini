use anyhow::Result;
use bytes::BytesMut;

use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tracing::{debug, trace};

use risotto_lib::process_bmp_message;
use risotto_lib::state::AsyncState;
use risotto_lib::state_store::store::StateStore;
use risotto_lib::update::Update;

pub async fn handle<T: StateStore>(
    stream: &mut TcpStream,
    state: Option<AsyncState<T>>,
    tx: Sender<Update>,
) -> Result<()> {
    let socket = stream.peer_addr()?;
    debug!("{}: session established", socket.to_string());

    // Reuse a single buffer and split complete BMP frames from it.
    let mut buffer = BytesMut::with_capacity(64 * 1024);

    loop {
        let n = stream.read_buf(&mut buffer).await?;
        if n == 0 {
            debug!("{}: connection closed by peer", socket.to_string());
            break;
        }

        loop {
            if buffer.len() < 6 {
                break;
            }

            // BMP common header.
            let message_version = buffer[0];
            if message_version != 3 {
                anyhow::bail!(
                    "{}: unsupported BMP message version {}",
                    socket,
                    message_version
                );
            }

            let packet_length =
                u32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]) as usize;
            if packet_length < 6 {
                anyhow::bail!(
                    "{}: invalid BMP message length {} (must be >= 6)",
                    socket,
                    packet_length
                );
            }

            let message_type = buffer[5];
            if message_type > 6 {
                anyhow::bail!("{}: unsupported BMP message type {}", socket, message_type);
            }

            if buffer.len() < packet_length {
                break;
            }

            let mut frame = buffer.split_to(packet_length).freeze();
            trace!("{}: Read {} bytes", socket, packet_length);
            process_bmp_message(state.clone(), tx.clone(), socket, &mut frame).await?;
        }
    }

    debug!("{}: session finished", socket.to_string());
    Ok(())
}
