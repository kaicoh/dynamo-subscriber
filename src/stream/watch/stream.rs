mod builder;

pub use builder::WatchStreamBuilder;

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
use tokio::{
    sync::watch::{self, error::RecvError},
    time::Duration,
};
use tokio_stream::Stream;
use tokio_util::sync::ReusableBoxFuture;
use tracing::error;

#[derive(Debug)]
struct WatchStreamProducer<Client>
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
    sender: watch::Sender<Vec<Record>>,
}

#[async_trait]
impl<Client> StreamProducerExt<Client> for WatchStreamProducer<Client>
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
        if let Err(err) = self.sender.send(records) {
            error!("Unexpected error during sending records. {err}");
        }
    }

    fn channel(&mut self) -> &mut ProducerHalf {
        &mut self.channel
    }
}

type BoxedReceiver =
    ReusableBoxFuture<'static, (Result<(), RecvError>, watch::Receiver<Vec<Record>>)>;

#[derive(Debug)]
pub struct WatchStream {
    inner: BoxedReceiver,
    channel: ConsumerHalf,
}

impl StreamConsumerExt for WatchStream {
    fn channel(&mut self) -> &mut ConsumerHalf {
        &mut self.channel
    }
}

impl Drop for WatchStream {
    fn drop(&mut self) {
        self.channel().close(|| {});
    }
}

// Ref: https://docs.rs/crate/tokio-stream/0.1.14/source/src/wrappers/watch.rs
impl Stream for WatchStream {
    type Item = Vec<Record>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (result, mut rx) = match self.inner.poll(cx) {
            Poll::Ready(res) => res,
            Poll::Pending => {
                return Poll::Pending;
            }
        };

        match result {
            Ok(_) => {
                let records = (*rx.borrow_and_update()).clone();
                self.inner.set(make_future(rx));
                Poll::Ready(Some(records))
            }
            Err(_) => {
                self.inner.set(make_future(rx));
                Poll::Ready(None)
            }
        }
    }
}

async fn make_future(
    mut rx: watch::Receiver<Vec<Record>>,
) -> (Result<(), RecvError>, watch::Receiver<Vec<Record>>) {
    let result = rx.changed().await;
    (result, rx)
}

impl WatchStream {
    fn new(channel: ConsumerHalf, receiver: watch::Receiver<Vec<Record>>) -> Self {
        Self {
            channel,
            inner: ReusableBoxFuture::new(async move { (Ok(()), receiver) }),
        }
    }

    fn from_changes(channel: ConsumerHalf, receiver: watch::Receiver<Vec<Record>>) -> Self {
        Self {
            channel,
            inner: ReusableBoxFuture::new(make_future(receiver)),
        }
    }
}

impl Unpin for WatchStream {}
