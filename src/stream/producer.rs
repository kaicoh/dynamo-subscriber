use super::{
    types::{GetShardsOutput, Lineages, Shard},
    DynamodbClient, Error,
};

use async_trait::async_trait;
use aws_sdk_dynamodbstreams::types::{Record, ShardIteratorType};
use std::{cmp, sync::Arc};
use tokio::{
    sync::{
        mpsc,
        oneshot::{error::TryRecvError, Receiver, Sender},
    },
    time::{sleep, Duration},
};
use tracing::error;

#[async_trait]
pub trait StreamProducer<Client>: Send + Sync
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

    fn rx_close(&mut self) -> &mut Receiver<()>;

    fn send_records(&mut self, records: Vec<Record>);

    fn should_close(&mut self) -> bool {
        !matches!(self.rx_close().try_recv(), Err(TryRecvError::Empty))
    }

    fn init_sender(&mut self) -> Option<Sender<()>>;

    fn send_initialized(&mut self) {
        if let Some(tx) = self.init_sender() {
            if let Err(err) = tx.send(()) {
                error!(
                    "Unexpected error during sending initialized event: {:#?}",
                    err
                );
            }
        }
    }

    async fn init(&mut self) -> Result<(), Error> {
        let stream_arn = self.client().get_stream_arn(self.table_name()).await?;
        self.set_stream_arn(stream_arn);

        let shards = self.get_all_shards().await?;
        let shards = self
            .get_shard_iterators(shards, self.shard_iterator_type())
            .await;

        self.set_shards(shards);
        self.send_initialized();
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
        if let Err(err) = self.init().await {
            error!(
                "Unexpected error during initialization: {err}. Skip polling {} table.",
                self.table_name(),
            );
            return;
        }

        loop {
            match self.iterate().await {
                Ok(records) => {
                    if !records.is_empty() {
                        self.send_records(records);
                    }
                }
                Err(err) => {
                    error!(
                        "Unexpected error during iteration: {err}. Stop polling {} table.",
                        self.table_name(),
                    );
                    return;
                }
            }

            if self.should_close() {
                return;
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
