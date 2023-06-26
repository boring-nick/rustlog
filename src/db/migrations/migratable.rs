use async_trait::async_trait;
use clickhouse::Client;
use futures::Future;

#[async_trait]
pub trait Migratable<'a> {
    async fn run(&self, db: &'a Client) -> anyhow::Result<()>;
}

#[async_trait]
impl<'a> Migratable<'a> for &str {
    async fn run(&self, db: &'a Client) -> anyhow::Result<()> {
        db.query(self).execute().await?;
        Ok(())
    }
}

#[async_trait]
impl<'a, F, O> Migratable<'a> for F
where
    F: Fn(&'a Client) -> O + Sync + Send,
    O: Future<Output = anyhow::Result<()>> + Send,
{
    async fn run(&self, db: &'a Client) -> anyhow::Result<()> {
        self(db).await
    }
}
