use aws_sdk_dynamodbstreams as dynamodbstreams;

#[derive(Debug, Clone)]
pub struct Shard {
    id: String,
    iterator: Option<String>,
    parent_shard_id: Option<String>,
}

impl Shard {
    pub fn new(shard: dynamodbstreams::types::Shard) -> Option<Self> {
        let dynamodbstreams::types::Shard {
            shard_id,
            parent_shard_id,
            ..
        } = shard;

        shard_id.map(|id| Self {
            id,
            iterator: None,
            parent_shard_id,
        })
    }

    pub fn id(&self) -> &str {
        self.id.as_str()
    }

    pub fn iterator(&self) -> Option<&str> {
        self.iterator.as_deref()
    }

    pub fn parent_shard_id(&self) -> Option<&str> {
        self.parent_shard_id.as_deref()
    }

    pub fn set_iterator(self, iterator: Option<String>) -> Self {
        Self {
            iterator,
            ..self
        }
    }
}
