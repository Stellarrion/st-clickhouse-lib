//! Runtime abstraction used by the async transport.
//!
//! The protocol and sync client do not depend on any async runtime. Async
//! transports depend on this small facade so runtime engines can be implemented
//! as feature-gated modules inside this crate without changing protocol code.

use std::future::Future;
use std::io as std_io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

/// Runtime-neutral async reader contract for runtime engines.
///
/// It intentionally mirrors the minimal operations the ClickHouse native
/// protocol needs instead of exposing a concrete runtime's I/O traits.
pub trait AsyncRead {
    fn poll_read(
        self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8],
    ) -> Poll<std_io::Result<usize>>;
}

/// Runtime-neutral async writer contract for runtime engines.
pub trait AsyncWrite {
    fn poll_write(
        self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8],
    ) -> Poll<std_io::Result<usize>>;

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std_io::Result<()>>;

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std_io::Result<()>>;
}

/// A bidirectional runtime stream.
pub trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> AsyncStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

/// A bidirectional stream for single-threaded/local runtimes.
pub trait LocalAsyncStream: AsyncRead + AsyncWrite + Unpin {}

impl<T> LocalAsyncStream for T where T: AsyncRead + AsyncWrite + Unpin {}

/// Minimal runtime contract expected from runtime engine modules.
///
/// The built-in Tokio engine is enabled by the `tokio` feature. Other
/// `Send`-capable engines can implement this trait and plug their stream type
/// into the same protocol layer without changing ClickHouse serialization code.
pub trait Runtime {
    type Stream: AsyncStream + 'static;
    type Sleep<'a>: Future<Output = ()> + Send + 'a
    where
        Self: 'a;
    type Connect<'a>: Future<Output = std_io::Result<Self::Stream>> + Send + 'a
    where
        Self: 'a;
    type Resolve<'a>: Future<Output = std_io::Result<Vec<SocketAddr>>> + Send + 'a
    where
        Self: 'a;

    fn sleep(&self, duration: Duration) -> Self::Sleep<'_>;

    fn connect(&self, addr: SocketAddr) -> Self::Connect<'_>;

    fn resolve(&self, host: &str, port: u16) -> Self::Resolve<'_>;

    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static;
}

/// Runtime contract for single-threaded/local engines.
///
/// This keeps non-`Send` runtimes honest instead of forcing unsafe `Send`
/// wrappers around runtime-owned file descriptors or timers.
pub trait LocalRuntime {
    type Stream: LocalAsyncStream + 'static;
    type Sleep<'a>: Future<Output = ()> + 'a
    where
        Self: 'a;
    type Connect<'a>: Future<Output = std_io::Result<Self::Stream>> + 'a
    where
        Self: 'a;
    type Resolve<'a>: Future<Output = std_io::Result<Vec<SocketAddr>>> + 'a
    where
        Self: 'a;

    fn sleep(&self, duration: Duration) -> Self::Sleep<'_>;

    fn connect(&self, addr: SocketAddr) -> Self::Connect<'_>;

    fn resolve(&self, host: &str, port: u16) -> Self::Resolve<'_>;

    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + 'static;
}

#[cfg(feature = "tokio")]
pub mod tokio_runtime;

#[cfg(feature = "smol")]
pub mod smol_runtime;

#[cfg(feature = "tokio")]
pub(crate) use tokio_runtime::*;

#[cfg(feature = "smol")]
pub use smol_runtime::SmolRuntime;

#[cfg(feature = "tokio")]
pub use tokio_runtime::TokioRuntime;
