use super::*;

#[derive(Debug)]
pub struct WatchStreamBuilder<Client>
where
    Client: DynamodbClient + 'static,
{
    table_name: Option<String>,
    client: Option<Client>,
    shard_iterator_type: ShardIteratorType,
    interval: Option<Duration>,
}

impl<Client> WatchStreamBuilder<Client>
where
    Client: DynamodbClient + 'static,
{
    pub fn new() -> Self {
        Self {
            table_name: None,
            client: None,
            shard_iterator_type: ShardIteratorType::Latest,
            interval: Some(Duration::from_secs(3)),
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

    pub fn build(self) -> WatchStream {
        let (c_half, rx) = self.build_producer();
        WatchStream::new(c_half, rx)
    }

    pub fn from_changes(self) -> WatchStream {
        let (c_half, rx) = self.build_producer();
        WatchStream::from_changes(c_half, rx)
    }

    fn build_producer(self) -> (ConsumerHalf, watch::Receiver<Vec<Record>>) {
        let table_name = self.table_name.expect("`table_name` is required");
        let client = self.client.expect("`client` is required");

        let (p_half, c_half) = channel::new();
        let (tx_watch, rx_watch) = watch::channel::<Vec<Record>>(vec![]);

        let mut producer = WatchStreamProducer {
            table_name,
            stream_arn: "".to_string(),
            shards: vec![],
            channel: p_half,
            client,
            shard_iterator_type: self.shard_iterator_type,
            interval: self.interval,
            sender: tx_watch,
        };

        tokio::spawn(async move {
            producer.streaming().await;
        });

        (c_half, rx_watch)
    }
}

impl<Client> Default for WatchStreamBuilder<Client>
where
    Client: DynamodbClient + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}
