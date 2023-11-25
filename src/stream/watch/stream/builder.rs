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
        let (tx, rx_init, rx) = self.build_producer();
        WatchStream::new(tx, rx, rx_init)
    }

    pub fn from_changes(self) -> WatchStream {
        let (tx, rx_init, rx) = self.build_producer();
        WatchStream::from_changes(tx, rx, rx_init)
    }

    fn build_producer(
        self,
    ) -> (
        oneshot::Sender<()>,
        oneshot::Receiver<()>,
        watch::Receiver<Vec<Record>>,
    ) {
        assert!(self.table_name.is_some(), "`table_name` must set");
        assert!(self.client.is_some(), "`client` must set");

        let table_name = self.table_name.unwrap();
        let client = self.client.unwrap();

        let (tx_oneshot, rx_oneshot) = oneshot::channel::<()>();
        let (tx_init, rx_init) = oneshot::channel::<()>();
        let (tx_watch, rx_watch) = watch::channel::<Vec<Record>>(vec![]);

        let mut producer = WatchStreamProducer {
            table_name,
            stream_arn: "".to_string(),
            shards: vec![],
            init_sender: Some(tx_init),
            client,
            shard_iterator_type: self.shard_iterator_type,
            interval: self.interval,
            sender: tx_watch,
            receiver: rx_oneshot,
        };

        tokio::spawn(async move {
            producer.streaming().await;
        });

        (tx_oneshot, rx_init, rx_watch)
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
