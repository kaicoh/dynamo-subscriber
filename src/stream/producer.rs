use super::{
    channel::ProducerHalf,
    types::{GetShardsOutput, Lineages, Shard},
    DynamodbClient, Error,
};

use async_trait::async_trait;
use aws_sdk_dynamodbstreams::types::{Record, ShardIteratorType};
use std::{cmp, sync::Arc};
use tokio::{
    sync::mpsc,
    time::{sleep, Duration},
};
use tracing::error;

#[async_trait]
pub trait StreamProducerExt<Client>: Send + Sync
where
    Client: DynamodbClient + 'static,
{
    fn client(&self) -> Arc<Client>;

    fn table_name(&self) -> &str;

    fn stream_arn(&self) -> &str;

    fn set_stream_arn(&mut self, stream_arn: String);

    fn interval(&self) -> Option<&Duration>;

    fn shards(&mut self) -> Vec<Shard>;

    fn set_shards(&mut self, shards: Vec<Shard>);

    fn shard_iterator_type(&self) -> ShardIteratorType;

    fn send_records(&mut self, records: Vec<Record>);

    fn channel(&mut self) -> &mut ProducerHalf;

    async fn init(&mut self) -> Result<(), Error> {
        let stream_arn = self.client().get_stream_arn(self.table_name()).await?;
        self.set_stream_arn(stream_arn);

        let shards = self.get_all_shards().await?;
        let shards = self
            .get_shard_iterators(shards, self.shard_iterator_type())
            .await;

        self.set_shards(shards);
        self.channel().send_init();
        Ok(())
    }

    async fn iterate(&mut self) -> Result<Vec<Record>, Error> {
        let lineages: Lineages = self.shards().into();
        let (mut shards, records) = lineages.get_records(self.client()).await;

        let new_shards = self
            .get_all_shards()
            .await?
            .into_iter()
            .filter(|shard| !shards.iter().any(|s| s.id() == shard.id()))
            .collect::<Vec<Shard>>();
        let mut new_shards = self
            .get_shard_iterators(new_shards, ShardIteratorType::Latest)
            .await;

        shards.append(&mut new_shards);
        self.set_shards(shards);

        Ok(records)
    }

    async fn streaming(&mut self) {
        ok_or_return!(self.init().await, |err| {
            error!(
                "Unexpected error during initialization: {err}. Skip polling {} table.",
                self.table_name(),
            );
        });

        loop {
            let records = ok_or_return!(self.iterate().await, |err| {
                error!(
                    "Unexpected error during iteration: {err}. Stop polling {} table.",
                    self.table_name(),
                );
            });

            if self.channel().should_close() {
                return;
            }

            if !records.is_empty() {
                self.send_records(records);
            }

            if let Some(interval) = self.interval() {
                sleep(*interval).await;
            }
        }
    }

    async fn get_all_shards(&self) -> Result<Vec<Shard>, Error> {
        let GetShardsOutput {
            mut shards,
            mut next_shard_id,
        } = self.client().get_shards(self.stream_arn(), None).await?;

        while next_shard_id.is_some() {
            let mut output = self
                .client()
                .get_shards(self.stream_arn(), next_shard_id.take())
                .await?;
            shards.append(&mut output.shards);
            next_shard_id = output.next_shard_id;
        }

        Ok(shards)
    }

    async fn get_shard_iterators(
        &self,
        shards: Vec<Shard>,
        shard_iterator_type: ShardIteratorType,
    ) -> Vec<Shard> {
        let (tx, mut rx) = mpsc::channel::<Shard>(cmp::max(1, shards.len()));
        let mut output: Vec<Shard> = vec![];

        for shard in shards {
            let tx = tx.clone();
            let client = self.client();
            let stream_arn = self.stream_arn().to_string();
            let shard_iterator_type = shard_iterator_type.clone();

            tokio::spawn(async move {
                let shard = match client
                    .get_shard_with_iterator(stream_arn, shard, shard_iterator_type)
                    .await
                {
                    Ok(shard) => shard,
                    Err(err) => {
                        error!("Unexpected error during getting shard iterator: {err}");
                        return;
                    }
                };

                if let Err(err) = tx.send(shard).await {
                    error!("Unexpected error during sending shard: {err}");
                }
            });
        }

        drop(tx);

        while let Some(shard) = rx.recv().await {
            output.push(shard);
        }

        output
    }
}
