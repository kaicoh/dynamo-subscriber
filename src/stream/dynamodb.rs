use super::{
    channel::{self, ConsumerChannel, ProducerChannel},
    types::{GetShardsOutput, Lineages, Shard},
    DynamodbClient, Error,
};
use aws_sdk_dynamodbstreams::types::{Record, ShardIteratorType};
use std::{
    cmp,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    sync::mpsc,
    time::{sleep, Duration},
};
use tokio_stream::Stream;
use tracing::error;

/// The polling half of DynamoDB Streams.
#[derive(Debug)]
pub struct DynamodbStreamProducer<Client>
where
    Client: DynamodbClient + 'static,
{
    table_name: String,
    stream_arn: String,
    shards: Option<Vec<Shard>>,
    channel: ProducerChannel,
    client: Client,
    shard_iterator_type: ShardIteratorType,
    interval: Option<Duration>,
    sender: mpsc::Sender<Vec<Record>>,
}

impl<Client> DynamodbStreamProducer<Client>
where
    Client: DynamodbClient + 'static,
{
    fn client(&self) -> Arc<Client> {
        Arc::new(self.client.clone())
    }

    /// Get shards and shard iterator ids for first attempt to get records.
    async fn init(&mut self) -> Result<(), Error> {
        let stream_arn = self.client.get_stream_arn(&self.table_name).await?;
        self.stream_arn = stream_arn;

        let shards = self.get_all_shards().await?;
        let shards = self
            .get_shard_iterators(shards, self.shard_iterator_type.clone())
            .await;

        self.shards = Some(shards);
        self.channel.send_init();

        Ok(())
    }

    /// Get records and renew shards for next iteration.
    async fn iterate(&mut self) -> Result<Vec<Record>, Error> {
        let lineages: Lineages = self.shards.take().unwrap_or_default().into();
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
        self.shards = Some(shards);

        Ok(records)
    }

    /// Poll the DynamoDB Streams.
    async fn streaming(&mut self) {
        ok_or_return!(self.init().await, |err| {
            error!(
                "Unexpected error during initialization: {err}. Skip polling {} table.",
                self.table_name,
            );
        });

        loop {
            let records = ok_or_return!(self.iterate().await, |err| {
                error!(
                    "Unexpected error during iteration: {err}. Stop polling {} table.",
                    self.table_name,
                );
            });

            if self.channel.should_close() {
                return;
            }

            if !records.is_empty() && self.sender.send(records).await.is_err() {
                return;
            }

            if let Some(duration) = self.interval {
                sleep(duration).await;
            }
        }
    }

    /// Get all shards from the DynamoDB table.
    async fn get_all_shards(&self) -> Result<Vec<Shard>, Error> {
        let GetShardsOutput {
            mut shards,
            mut last_shard_id,
        } = self.client.get_shards(&self.stream_arn, None).await?;

        while last_shard_id.is_some() {
            let mut output = self
                .client
                .get_shards(&self.stream_arn, last_shard_id.take())
                .await?;
            shards.append(&mut output.shards);
            last_shard_id = output.last_shard_id;
        }

        Ok(shards)
    }

    /// Get and set shard iterator.
    async fn get_shard_iterators(
        &self,
        shards: Vec<Shard>,
        shard_iterator_type: ShardIteratorType,
    ) -> Vec<Shard> {
        let (tx, mut rx) = mpsc::channel::<Shard>(cmp::max(1, shards.len()));
        let mut output: Vec<Shard> = vec![];
        let client = self.client();

        for shard in shards {
            let tx = tx.clone();
            let client = Arc::clone(&client);
            let stream_arn = self.stream_arn.clone();
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

/// Represent DynamoDB Stream.
///
/// This struct receives DynamoDB Stream records from polling half and emit them as Rust Stream.
#[derive(Debug)]
pub struct DynamodbStream {
    receiver: mpsc::Receiver<Vec<Record>>,
    channel: Option<ConsumerChannel>,
}

impl DynamodbStream {
    /// Get [`ConsumerChannel`] as communication channel to the stream.
    ///
    /// Once you take a channel from this method, you can't take it anymore from the same channel
    /// because this method also passes the ownership of the channel.
    ///
    /// ```rust,no_run
    /// use aws_config::BehaviorVersion;
    /// use dynamo_subscriber as subscriber;
    ///
    /// # async fn wrapper() {
    /// # let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    /// # let client = subscriber::Client::new(&config);
    /// let mut stream = subscriber::stream::builder()
    ///     .client(client)
    ///     .table_name("People")
    ///     .build();
    /// let channel = stream.take_channel();
    /// assert!(channel.is_some());
    ///
    /// let channel = stream.take_channel();
    /// assert!(channel.is_none());
    /// # }
    /// ```
    pub fn take_channel(&mut self) -> Option<ConsumerChannel> {
        self.channel.take()
    }
}

impl Stream for DynamodbStream {
    type Item = Vec<Record>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl Drop for DynamodbStream {
    fn drop(&mut self) {
        self.receiver.close();
        if let Some(mut channel) = self.take_channel() {
            channel.close(|| {});
        }
    }
}

impl AsRef<mpsc::Receiver<Vec<Record>>> for DynamodbStream {
    fn as_ref(&self) -> &mpsc::Receiver<Vec<Record>> {
        &self.receiver
    }
}

impl AsMut<mpsc::Receiver<Vec<Record>>> for DynamodbStream {
    fn as_mut(&mut self) -> &mut mpsc::Receiver<Vec<Record>> {
        &mut self.receiver
    }
}

/// A builder for [`DynamodbStream`].
#[derive(Debug)]
pub struct DynamodbStreamBuilder<Client>
where
    Client: DynamodbClient + 'static,
{
    table_name: Option<String>,
    client: Option<Client>,
    shard_iterator_type: ShardIteratorType,
    interval: Option<Duration>,
    buffer: usize,
}

impl<Client> DynamodbStreamBuilder<Client>
where
    Client: DynamodbClient + 'static,
{
    /// Create a new `DynamodbStreamBuilder`.
    pub fn new() -> Self {
        Self {
            table_name: None,
            client: None,
            shard_iterator_type: ShardIteratorType::Latest,
            interval: Some(Duration::from_secs(3)),
            buffer: 100,
        }
    }

    /// Set table name you want to retrieve records from.
    ///
    /// **Setting any table name is required** before the build method is called.
    pub fn table_name(self, table_name: impl Into<String>) -> Self {
        Self {
            table_name: Some(table_name.into()),
            ..self
        }
    }

    /// Set client to call AWS APIs.
    ///
    /// **Setting any client is required** before the build method is called.
    pub fn client(self, client: Client) -> Self {
        Self {
            client: Some(client),
            ..self
        }
    }

    /// Set [`ShardIteratorType`] to get records for the first time.
    /// After the first time, the DynamodbStream uses the shard iterator from the previous
    /// `get records` operation outputs.
    ///
    /// Setting any shard iterator type is optional. If you omit calling this method,
    /// `ShardIteratorType::Latest` is used as default value.
    pub fn shard_iterator_type(self, shard_iterator_type: ShardIteratorType) -> Self {
        Self {
            shard_iterator_type,
            ..self
        }
    }

    /// Set interval between polling attempts. When None is provided there are no intervals between
    /// polling iterations.
    ///
    /// Setting any interval is optional. If you omit calling this method,
    /// `3 seconds` is used as default value.
    pub fn interval(self, interval: Option<Duration>) -> Self {
        Self { interval, ..self }
    }

    /// Set the buffer for [`tokio::sync::mpsc::channel`](tokio::sync::mpsc::channel).
    ///
    /// The stream records are stored up to the buffer size unless the records are consumed.
    /// Once the buffer is full, attempts to receive records from the DynamoDB Streams will
    /// wait until the records is consumed.
    ///
    /// This method will panic when given zero as buffer size.
    ///
    /// Setting buffer size is optional. If you omit calling this method,
    /// `100` is used as default value.
    pub fn buffer(self, buffer: usize) -> Self {
        if buffer == 0 {
            panic!("buffer must be positive.");
        }

        Self { buffer, ..self }
    }

    /// Consumes the builder and constructs a [`DynamodbStream`].
    ///
    /// This method will panic if no table name is set or no client is set.
    pub fn build(self) -> DynamodbStream {
        let (c_half, rx) = self.build_producer();

        DynamodbStream {
            receiver: rx,
            channel: Some(c_half),
        }
    }

    fn build_producer(self) -> (ConsumerChannel, mpsc::Receiver<Vec<Record>>) {
        let table_name = self.table_name.expect("`table_name` is required");
        let client = self.client.expect("`client` is required");

        let (p_half, c_half) = channel::new();
        let (tx_mpsc, rx_mpsc) = mpsc::channel::<Vec<Record>>(self.buffer);

        let mut producer = DynamodbStreamProducer {
            table_name,
            stream_arn: "".to_string(),
            shards: None,
            channel: p_half,
            client,
            shard_iterator_type: self.shard_iterator_type,
            interval: self.interval,
            sender: tx_mpsc,
        };

        tokio::spawn(async move {
            producer.streaming().await;
        });

        (c_half, rx_mpsc)
    }
}

impl<Client> Default for DynamodbStreamBuilder<Client>
where
    Client: DynamodbClient + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}
