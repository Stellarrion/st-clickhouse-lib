use crate::connection::query_builder::QueryBuilder;
use crate::error::Result;
use crate::protocol::block::{Block, RawBlock};

/// Typed raw result for [`QueryBuilder::fetch`].
///
/// Use `client.query(sql).fetch::<RawBlocks>().await?` when the caller needs
/// exact native block payloads instead of materialized [`Block`] values.
#[derive(Debug, Clone)]
pub struct RawBlocks(pub Vec<RawBlock>);

impl RawBlocks {
    pub fn into_inner(self) -> Vec<RawBlock> {
        self.0
    }

    pub fn as_slice(&self) -> &[RawBlock] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl std::ops::Deref for RawBlocks {
    type Target = [RawBlock];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl IntoIterator for RawBlocks {
    type IntoIter = std::vec::IntoIter<RawBlock>;
    type Item = RawBlock;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Typed row-count result for [`QueryBuilder::fetch`].
///
/// This uses the discard/count read path and does not allocate blocks or rows.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RowCount(pub usize);

impl RowCount {
    pub fn get(self) -> usize {
        self.0
    }
}

impl From<RowCount> for usize {
    fn from(value: RowCount) -> Self {
        value.0
    }
}

/// Typed scalar result for [`QueryBuilder::fetch`].
///
/// Scalars are wrapped to avoid ambiguity between "one row" and "one column".
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Scalar<T>(pub T);

impl<T> Scalar<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> std::ops::Deref for Scalar<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Result type accepted by [`QueryBuilder::fetch`].
///
/// Implemented for [`Block`], `Vec<T>`, `T`, `Option<T>`, [`Scalar<T>`],
/// [`RawBlocks`], and [`RowCount`].
#[allow(async_fn_in_trait)]
pub trait QueryResult: Sized {
    async fn fetch_from(query: QueryBuilder<'_>) -> Result<Self>;
}

impl QueryResult for Block {
    async fn fetch_from(query: QueryBuilder<'_>) -> Result<Self> {
        query.block().await
    }
}

impl<T> QueryResult for Vec<T>
where
    T: crate::row::Row + Send,
{
    async fn fetch_from(query: QueryBuilder<'_>) -> Result<Self> {
        query.all::<T>().await
    }
}

impl<T> QueryResult for T
where
    T: crate::row::Row + Send,
{
    async fn fetch_from(query: QueryBuilder<'_>) -> Result<Self> {
        query.one::<T>().await
    }
}

impl<T> QueryResult for Option<T>
where
    T: crate::row::Row + Send,
{
    async fn fetch_from(query: QueryBuilder<'_>) -> Result<Self> {
        query.optional::<T>().await
    }
}

impl<T> QueryResult for Scalar<T>
where
    T: crate::column::ClickHouseColumn + Send + 'static,
{
    async fn fetch_from(query: QueryBuilder<'_>) -> Result<Self> {
        query.scalar::<T>().await.map(Scalar)
    }
}

impl QueryResult for RawBlocks {
    async fn fetch_from(query: QueryBuilder<'_>) -> Result<Self> {
        query.raw().await.map(RawBlocks)
    }
}

impl QueryResult for RowCount {
    async fn fetch_from(query: QueryBuilder<'_>) -> Result<Self> {
        query.row_count().await.map(RowCount)
    }
}
