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
    buffer: Option<usize>,
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
            buffer: Some(100),
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
        Self {
            buffer: Some(buffer),
            ..self
        }
    }

    pub fn build(self) -> MpscStream {
        let (tx, rx_init, rx) = self.build_producer();

        MpscStream {
            sender: Some(tx),
            inner: rx,
            initialized: false,
            init_receiver: rx_init,
        }
    }

    fn build_producer(
        self,
    ) -> (
        oneshot::Sender<()>,
        oneshot::Receiver<()>,
        mpsc::Receiver<Vec<Record>>,
    ) {
        assert!(self.table_name.is_some(), "`table_name` must set");
        assert!(self.client.is_some(), "`client` must set");
        assert!(self.buffer.is_some(), "`buffer` must set");

        let table_name = self.table_name.unwrap();
        let client = self.client.unwrap();
        let buffer = self.buffer.unwrap();

        let (tx_oneshot, rx_oneshot) = oneshot::channel::<()>();
        let (tx_init, rx_init) = oneshot::channel::<()>();
        let (tx_mpsc, rx_mpsc) = mpsc::channel::<Vec<Record>>(buffer);

        let mut producer = MpscStreamProducer {
            table_name,
            stream_arn: "".to_string(),
            shards: vec![],
            init_sender: Some(tx_init),
            client,
            shard_iterator_type: self.shard_iterator_type,
            interval: self.interval,
            sender: tx_mpsc,
            receiver: rx_oneshot,
        };

        tokio::spawn(async move {
            producer.streaming().await;
        });

        (tx_oneshot, rx_init, rx_mpsc)
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
