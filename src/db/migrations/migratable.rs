use clickhouse::Client;
use futures::Future;

pub trait Migratable<'a> {
    fn run(&self, db: &'a Client) -> impl Future<Output = anyhow::Result<()>>;
}

impl<'a> Migratable<'a> for &str {
    async fn run(&self, db: &'a Client) -> anyhow::Result<()> {
        db.query(self).execute().await?;
        Ok(())
    }
}

impl<'a, F, O> Migratable<'a> for F
where
    F: Fn(&'a Client) -> O + Sync + Send,
    O: Future<Output = anyhow::Result<()>> + Send,
{
    async fn run(&self, db: &'a Client) -> anyhow::Result<()> {
        self(db).await
    }
}
