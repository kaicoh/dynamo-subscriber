mod builder;

pub use builder::MpscStreamBuilder;

use super::{types::Shard, DynamodbClient, StreamConsumer, StreamProducer};

use async_trait::async_trait;
use aws_sdk_dynamodbstreams::types::{Record, ShardIteratorType};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    sync::{mpsc, oneshot},
    time::Duration,
};
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
    init_sender: Option<oneshot::Sender<()>>,
    client: Client,
    shard_iterator_type: ShardIteratorType,
    interval: Option<Duration>,
    sender: mpsc::Sender<Vec<Record>>,
    receiver: oneshot::Receiver<()>,
}

#[async_trait]
impl<Client> StreamProducer<Client> for MpscStreamProducer<Client>
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

    fn rx_close(&mut self) -> &mut oneshot::Receiver<()> {
        &mut self.receiver
    }

    fn send_records(&mut self, records: Vec<Record>) {
        let tx = self.sender.clone();
        tokio::spawn(async move {
            if let Err(err) = tx.send(records).await {
                error!("Unexpected error during sending records. {err}");
            }
        });
    }

    fn init_sender(&mut self) -> Option<oneshot::Sender<()>> {
        self.init_sender.take()
    }
}

#[derive(Debug)]
pub struct MpscStream {
    sender: Option<oneshot::Sender<()>>,
    inner: mpsc::Receiver<Vec<Record>>,
    initialized: bool,
    init_receiver: oneshot::Receiver<()>,
}

#[async_trait]
impl StreamConsumer for MpscStream {
    fn tx_close(&mut self) -> Option<oneshot::Sender<()>> {
        self.sender.take()
    }

    fn close(&mut self) {
        self.inner.close();
        if let Some(tx) = self.tx_close() {
            if let Err(err) = tx.send(()) {
                error!("Unexpected error during sending close event. {:?}", err);
            }
        }
    }

    fn init_receiver(&mut self) -> &mut oneshot::Receiver<()> {
        &mut self.init_receiver
    }

    fn initialized(&self) -> bool {
        self.initialized
    }

    fn done_initialization(&mut self) {
        self.initialized = true;
    }
}

impl Drop for MpscStream {
    fn drop(&mut self) {
        self.inner.close();
        if let Some(tx) = self.tx_close() {
            let _ = tx.send(());
        }
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
