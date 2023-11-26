use super::*;

#[derive(Debug)]
pub struct MpscStreamBuilder<Client>
where
    Client: DynamodbClient + 'static,
{
    table_name: Option<String>,
    client: Option<Client>,
    shard_iterator_type: ShardIteratorType,
    interval: Option<Duration>,
    buffer: usize,
}

impl<Client> MpscStreamBuilder<Client>
where
    Client: DynamodbClient + 'static,
{
    pub fn new() -> Self {
        Self {
            table_name: None,
            client: None,
            shard_iterator_type: ShardIteratorType::Latest,
            interval: Some(Duration::from_secs(3)),
            buffer: 100,
        }
    }

    pub fn table_name(self, table_name: impl Into<String>) -> Self {
        Self {
            table_name: Some(table_name.into()),
            ..self
        }
    }

    pub fn client(self, client: Client) -> Self {
        Self {
            client: Some(client),
            ..self
        }
    }

    pub fn shard_iterator_type(self, shard_iterator_type: ShardIteratorType) -> Self {
        Self {
            shard_iterator_type,
            ..self
        }
    }

    pub fn interval(self, interval: Option<Duration>) -> Self {
        Self { interval, ..self }
    }

    pub fn buffer(self, buffer: usize) -> Self {
        if buffer == 0 {
            panic!("buffer must be positive.");
        }

        Self { buffer, ..self }
    }

    pub fn build(self) -> MpscStream {
        let (c_half, rx) = self.build_producer();

        MpscStream {
            inner: rx,
            channel: c_half,
        }
    }

    fn build_producer(self) -> (ConsumerHalf, mpsc::Receiver<Vec<Record>>) {
        let table_name = self.table_name.expect("`table_name` is required");
        let client = self.client.expect("`client` is required");

        let (p_half, c_half) = channel::new();
        let (tx_mpsc, rx_mpsc) = mpsc::channel::<Vec<Record>>(self.buffer);

        let mut producer = MpscStreamProducer {
            table_name,
            stream_arn: "".to_string(),
            shards: vec![],
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

impl<Client> Default for MpscStreamBuilder<Client>
where
    Client: DynamodbClient + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}
