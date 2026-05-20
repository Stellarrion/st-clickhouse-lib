//! Tokio runtime engine.

use std::future::Future;
use std::io as std_io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::runtime::{AsyncRead as RuntimeAsyncRead, AsyncWrite as RuntimeAsyncWrite, Runtime};
pub(crate) mod io {
    #[cfg(test)]
    pub(crate) use tokio::io::duplex;
    pub(crate) use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
}

pub(crate) mod net {
    pub(crate) use tokio::net::{TcpStream, ToSocketAddrs, lookup_host};
}

pub(crate) mod sync {
    pub(crate) use tokio::sync::{Mutex, MutexGuard, RwLock, mpsc};
}

pub(crate) mod time {
    pub(crate) use tokio::time::{Instant, sleep, timeout};
}

pub(crate) fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(future)
}

/// Built-in Tokio runtime engine.
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioRuntime;

impl RuntimeAsyncRead for net::TcpStream {
    fn poll_read(
        self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8],
    ) -> Poll<std_io::Result<usize>> {
        let mut read_buf = tokio::io::ReadBuf::new(buf);
        match tokio::io::AsyncRead::poll_read(self, cx, &mut read_buf) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(read_buf.filled().len())),
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RuntimeAsyncWrite for net::TcpStream {
    fn poll_write(
        self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8],
    ) -> Poll<std_io::Result<usize>> {
        tokio::io::AsyncWrite::poll_write(self, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std_io::Result<()>> {
        tokio::io::AsyncWrite::poll_flush(self, cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std_io::Result<()>> {
        tokio::io::AsyncWrite::poll_shutdown(self, cx)
    }
}

impl Runtime for TokioRuntime {
    type Stream = net::TcpStream;
    type Sleep<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
    type Connect<'a> = Pin<Box<dyn Future<Output = std_io::Result<Self::Stream>> + Send + 'a>>;
    type Resolve<'a> = Pin<Box<dyn Future<Output = std_io::Result<Vec<SocketAddr>>> + Send + 'a>>;

    fn sleep(&self, duration: Duration) -> Self::Sleep<'_> {
        Box::pin(tokio::time::sleep(duration))
    }

    fn connect(&self, addr: SocketAddr) -> Self::Connect<'_> {
        Box::pin(async move { net::TcpStream::connect(addr).await })
    }

    fn resolve(&self, host: &str, port: u16) -> Self::Resolve<'_> {
        let addr = format!("{host}:{port}");
        Box::pin(async move { Ok(net::lookup_host(addr).await?.collect::<Vec<_>>()) })
    }

    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(future);
    }
}
