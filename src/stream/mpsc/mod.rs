mod stream;

use super::{channel, types, DynamodbClient, StreamConsumerExt, StreamProducerExt};

pub use stream::{MpscStream, MpscStreamBuilder};

pub fn builder<Client>() -> MpscStreamBuilder<Client>
where
    Client: DynamodbClient + 'static,
{
    MpscStreamBuilder::new()
}
