//! smol runtime engine.

use std::future::Future;
use std::io as std_io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::runtime::{AsyncRead as RuntimeAsyncRead, AsyncWrite as RuntimeAsyncWrite, Runtime};

/// smol runtime engine.
///
/// This implements the runtime-neutral contract. The high-level async client is
/// still Tokio-backed until the connection/pool layer is made generic over
/// [`Runtime`].
#[derive(Debug, Clone, Copy, Default)]
pub struct SmolRuntime;

impl RuntimeAsyncRead for async_net::TcpStream {
    fn poll_read(
        self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8],
    ) -> Poll<std_io::Result<usize>> {
        futures_io::AsyncRead::poll_read(self, cx, buf)
    }
}

impl RuntimeAsyncWrite for async_net::TcpStream {
    fn poll_write(
        self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8],
    ) -> Poll<std_io::Result<usize>> {
        futures_io::AsyncWrite::poll_write(self, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std_io::Result<()>> {
        futures_io::AsyncWrite::poll_flush(self, cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std_io::Result<()>> {
        futures_io::AsyncWrite::poll_close(self, cx)
    }
}

impl Runtime for SmolRuntime {
    type Stream = async_net::TcpStream;
    type Sleep<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
    type Connect<'a> = Pin<Box<dyn Future<Output = std_io::Result<Self::Stream>> + Send + 'a>>;
    type Resolve<'a> = Pin<Box<dyn Future<Output = std_io::Result<Vec<SocketAddr>>> + Send + 'a>>;

    fn sleep(&self, duration: Duration) -> Self::Sleep<'_> {
        Box::pin(async move {
            smol::Timer::after(duration).await;
        })
    }

    fn connect(&self, addr: SocketAddr) -> Self::Connect<'_> {
        Box::pin(async move { async_net::TcpStream::connect(addr).await })
    }

    fn resolve(&self, host: &str, port: u16) -> Self::Resolve<'_> {
        let addr = format!("{host}:{port}");
        Box::pin(async move {
            let addrs = async_net::resolve(addr).await?;
            Ok(addrs.into_iter().collect())
        })
    }

    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        smol::spawn(future).detach();
    }
}
