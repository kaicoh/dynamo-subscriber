mod stream;

use super::{types, DynamodbClient, StreamConsumer, StreamProducer};

pub use stream::{MpscStream, MpscStreamBuilder};

pub fn builder<Client>() -> MpscStreamBuilder<Client>
where
    Client: DynamodbClient + 'static,
{
    MpscStreamBuilder::new()
}
