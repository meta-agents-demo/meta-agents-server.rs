//! Raw TCP transport: newline-delimited JSON. Each line is one event (or an
//! array of events); the server answers each line with one ack line.

use crate::model::{Ack, Transport};
use crate::state::{AppState, MAX_INGEST_PAYLOAD_BYTES};
use std::io;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

#[derive(Debug, PartialEq, Eq)]
enum BoundedLine {
    Eof,
    Line(String),
    TooLarge,
    InvalidUtf8,
}

/// Read one newline-delimited frame without ever allocating beyond `max`
/// payload bytes. `\n` and the optional `\r` in a CRLF terminator are framing,
/// not payload, including when the two bytes arrive in different read buffers.
/// An oversized frame closes that connection after an explicit error
/// acknowledgement; reconnecting starts from a clean framing boundary.
async fn read_bounded_line<R>(reader: &mut R, max: usize) -> io::Result<BoundedLine>
where
    R: AsyncBufRead + Unpin,
{
    let mut frame = Vec::with_capacity(max.min(8 * 1024));
    let mut pending_carriage_return = false;

    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if pending_carriage_return {
                if frame.len() >= max {
                    return Ok(BoundedLine::TooLarge);
                }
                frame.push(b'\r');
            }
            if frame.is_empty() {
                return Ok(BoundedLine::Eof);
            }
            break;
        }

        let newline = available.iter().position(|byte| *byte == b'\n');

        // A deferred CR followed immediately by LF is a split CRLF delimiter.
        // Otherwise it was ordinary payload and must be counted before copying
        // the current buffer.
        if pending_carriage_return {
            if newline == Some(0) {
                reader.consume(1);
                break;
            }
            if frame.len() >= max {
                return Ok(BoundedLine::TooLarge);
            }
            frame.push(b'\r');
            pending_carriage_return = false;
        }

        let segment_end = newline.unwrap_or(available.len());
        let segment = &available[..segment_end];
        let trailing_carriage_return = segment.last() == Some(&b'\r');
        let payload = if trailing_carriage_return {
            &segment[..segment.len() - 1]
        } else {
            segment
        };

        if frame.len().saturating_add(payload.len()) > max {
            return Ok(BoundedLine::TooLarge);
        }
        frame.extend_from_slice(payload);

        match newline {
            Some(position) => {
                reader.consume(position + 1);
                break;
            }
            None => {
                pending_carriage_return = trailing_carriage_return;
                reader.consume(segment_end);
            }
        }
    }

    match String::from_utf8(frame) {
        Ok(line) => Ok(BoundedLine::Line(line)),
        Err(_) => Ok(BoundedLine::InvalidUtf8),
    }
}

async fn write_ack<W>(writer: &mut W, ack: Ack) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut payload = serde_json::to_string(&ack).unwrap_or_else(|_| "{\"ok\":false}".to_string());
    payload.push('\n');
    writer.write_all(payload.as_bytes()).await
}

pub async fn serve(listener: TcpListener, state: AppState) {
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(error) => {
                tracing::warn!("tcp accept error: {error}");
                continue;
            }
        };
        tracing::debug!("tcp connection from {peer}");
        let state = state.clone();
        tokio::spawn(async move {
            let (read_half, mut write_half) = stream.into_split();
            let mut reader = BufReader::new(read_half);
            loop {
                let line = match read_bounded_line(&mut reader, MAX_INGEST_PAYLOAD_BYTES).await {
                    Ok(BoundedLine::Eof) => break,
                    Ok(BoundedLine::Line(line)) => line,
                    Ok(BoundedLine::TooLarge) => {
                        let _ = write_ack(
                            &mut write_half,
                            Ack::err(format!(
                                "payload exceeds {MAX_INGEST_PAYLOAD_BYTES} byte limit"
                            )),
                        )
                        .await;
                        break;
                    }
                    Ok(BoundedLine::InvalidUtf8) => {
                        if write_ack(&mut write_half, Ack::err("payload must be UTF-8"))
                            .await
                            .is_err()
                        {
                            break;
                        }
                        continue;
                    }
                    Err(error) => {
                        tracing::debug!("tcp read error from {peer}: {error}");
                        break;
                    }
                };

                if line.trim().is_empty() {
                    continue;
                }
                let ack = match state.ingest_json(Transport::Tcp, &line) {
                    Ok(stored) => Ack::ok(stored.last().map(|event| event.seq).unwrap_or(0)),
                    Err(error) => Ack::err(error),
                };
                if write_ack(&mut write_half, ack).await.is_err() {
                    break;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bounded_reader_accepts_exact_limit_and_unterminated_final_line() {
        let input = b"1234\r\nlast";
        let mut reader = BufReader::new(&input[..]);
        assert_eq!(
            read_bounded_line(&mut reader, 4).await.unwrap(),
            BoundedLine::Line("1234".to_string())
        );
        assert_eq!(
            read_bounded_line(&mut reader, 4).await.unwrap(),
            BoundedLine::Line("last".to_string())
        );
        assert_eq!(
            read_bounded_line(&mut reader, 4).await.unwrap(),
            BoundedLine::Eof
        );
    }

    #[tokio::test]
    async fn bounded_reader_accepts_crlf_split_across_read_buffers() {
        let input = b"1234\r\nnext\n";
        let mut reader = BufReader::with_capacity(5, &input[..]);
        assert_eq!(
            read_bounded_line(&mut reader, 4).await.unwrap(),
            BoundedLine::Line("1234".to_string())
        );
        assert_eq!(
            read_bounded_line(&mut reader, 4).await.unwrap(),
            BoundedLine::Line("next".to_string())
        );
    }

    #[tokio::test]
    async fn lone_carriage_return_counts_as_payload_at_eof() {
        let input = b"1234\r";
        let mut reader = BufReader::new(&input[..]);
        assert_eq!(
            read_bounded_line(&mut reader, 4).await.unwrap(),
            BoundedLine::TooLarge
        );
    }

    #[tokio::test]
    async fn bounded_reader_rejects_before_copying_past_the_limit() {
        let input = b"12345\n";
        let mut reader = BufReader::new(&input[..]);
        assert_eq!(
            read_bounded_line(&mut reader, 4).await.unwrap(),
            BoundedLine::TooLarge
        );
    }

    #[tokio::test]
    async fn bounded_reader_rejects_non_utf8_without_poisoning_the_next_frame() {
        let input = [0xff, b'\n', b'o', b'k', b'\n'];
        let mut reader = BufReader::new(&input[..]);
        assert_eq!(
            read_bounded_line(&mut reader, 8).await.unwrap(),
            BoundedLine::InvalidUtf8
        );
        assert_eq!(
            read_bounded_line(&mut reader, 8).await.unwrap(),
            BoundedLine::Line("ok".to_string())
        );
    }
}
