use crate::connection::tcp::Client;
use crate::error::Result;
use crate::protocol::packet::ClientPacket;
use crate::runtime::io::{AsyncReadExt, AsyncWriteExt};

impl Client {
    /// Ping the server to verify the connection is alive.
    pub async fn ping(&self) -> Result<()> {
        let mut guard = self.pool.get().await?;
        // Mark in-flight before the Ping: a future dropped before the Pong
        // must not return a socket with a stray reply pending to the pool.
        guard.mark_response_in_flight();
        let result = async {
            let stream = guard.stream_mut();
            stream.write_packet(&[ClientPacket::Ping as u8]).await?;
            stream.flush().await?;
            let mut pkt = [0u8; 1];
            stream.read_exact(&mut pkt).await?;
            if pkt[0] != 4 {
                return Err(crate::error::Error::Protocol(format!(
                    "expected Pong (4), got {}",
                    pkt[0]
                )));
            }
            Ok(())
        }
        .await;
        guard.finish_response(&result);
        result
    }

    /// Execute a SELECT and return only the number of rows in Data packets.
    ///
    /// Column bytes are consumed and discarded without constructing owned
    /// [`crate::protocol::block::Block`] values. Use this for count-only scans
    /// and benchmark cases where the caller does not need typed column
    /// materialization.
    pub async fn query_row_count(&self, sql: &str) -> Result<usize> {
        self.query(sql).row_count().await
    }

    /// Start building a batch of queries (explicit opt-in to pipelining).
    pub fn batch(&self) -> super::batch::BatchBuilder<'_> {
        super::batch::BatchBuilder::new(self)
    }

    /// Server info from handshake.
    pub async fn server_info(&self) -> Result<crate::protocol::handshake::ServerInfo> {
        let guard = self.pool.get().await?;
        Ok(guard.server_info().clone())
    }
}
