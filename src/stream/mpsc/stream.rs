mod builder;

pub use builder::MpscStreamBuilder;

use super::{
    channel::{self, ConsumerHalf, ProducerHalf},
    types::Shard,
    DynamodbClient, StreamConsumerExt, StreamProducerExt,
};

use async_trait::async_trait;
use aws_sdk_dynamodbstreams::types::{Record, ShardIteratorType};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{sync::mpsc, time::Duration};
use tokio_stream::Stream;
use tracing::error;

#[derive(Debug)]
struct MpscStreamProducer<Client>
where
    Client: DynamodbClient + 'static,
{
    table_name: String,
    stream_arn: String,
    shards: Vec<Shard>,
    channel: ProducerHalf,
    client: Client,
    shard_iterator_type: ShardIteratorType,
    interval: Option<Duration>,
    sender: mpsc::Sender<Vec<Record>>,
}

#[async_trait]
impl<Client> StreamProducerExt<Client> for MpscStreamProducer<Client>
where
    Client: DynamodbClient + 'static,
{
    fn client(&self) -> Arc<Client> {
        Arc::new(self.client.clone())
    }

    fn table_name(&self) -> &str {
        self.table_name.as_str()
    }

    fn interval(&self) -> Option<&Duration> {
        self.interval.as_ref()
    }

    fn shard_iterator_type(&self) -> ShardIteratorType {
        self.shard_iterator_type.clone()
    }

    fn stream_arn(&self) -> &str {
        self.stream_arn.as_str()
    }

    fn set_stream_arn(&mut self, stream_arn: String) {
        self.stream_arn = stream_arn;
    }

    fn shards(&mut self) -> Vec<Shard> {
        self.shards.to_vec()
    }

    fn set_shards(&mut self, shards: Vec<Shard>) {
        self.shards = shards;
    }

    fn send_records(&mut self, records: Vec<Record>) {
        let tx = self.sender.clone();
        tokio::spawn(async move {
            if let Err(err) = tx.send(records).await {
                error!("Unexpected error during sending records. {err}");
            }
        });
    }

    fn channel(&mut self) -> &mut ProducerHalf {
        &mut self.channel
    }
}

#[derive(Debug)]
pub struct MpscStream {
    inner: mpsc::Receiver<Vec<Record>>,
    channel: ConsumerHalf,
}

impl StreamConsumerExt for MpscStream {
    fn channel(&mut self) -> &mut ConsumerHalf {
        &mut self.channel
    }
}

impl Drop for MpscStream {
    fn drop(&mut self) {
        self.inner.close();
        self.channel().close(|| {});
    }
}

// https://docs.rs/crate/tokio-stream/0.1.14/source/src/wrappers/mpsc_bounded.rs
impl Stream for MpscStream {
    type Item = Vec<Record>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_recv(cx)
    }
}

impl AsRef<mpsc::Receiver<Vec<Record>>> for MpscStream {
    fn as_ref(&self) -> &mpsc::Receiver<Vec<Record>> {
        &self.inner
    }
}

impl AsMut<mpsc::Receiver<Vec<Record>>> for MpscStream {
    fn as_mut(&mut self) -> &mut mpsc::Receiver<Vec<Record>> {
        &mut self.inner
    }
}
