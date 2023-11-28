use aws_sdk_dynamodbstreams as dynamodbstreams;

/// A shard representation to retreive DynamoDB Streams records.
#[derive(Debug, Clone)]
pub struct Shard {
    id: String,
    iterator: Option<String>,
    parent_shard_id: Option<String>,
}

impl Shard {
    /// Create a new shard from
    /// [`aws_sdk_dynamodbstreams::types::Shard`](aws_sdk_dynamodbstreams::types::Shard).
    /// If the input's shard_id is None, this method also returns None.
    ///
    /// ```rust
    /// use aws_sdk_dynamodbstreams as dynamodbstreams;
    /// use dynamo_subscriber::types::Shard;
    ///
    /// // Create a aws_sdk_dynamodbstreams's Shard with shard_id
    /// let original = dynamodbstreams::types::Shard::builder()
    ///     .shard_id("0001")
    ///     .build();
    /// assert!(original.shard_id().is_some());
    ///
    /// // This case, the shard should exist.
    /// let shard = Shard::new(original);
    /// assert!(shard.is_some());
    ///
    /// // Create a aws_sdk_dynamodbstreams's Shard without shard_id
    /// let original = dynamodbstreams::types::Shard::builder()
    ///     .build();
    /// assert!(original.shard_id().is_none());
    ///
    /// // This case, the shard should not exist.
    /// let shard = Shard::new(original);
    /// assert!(shard.is_none());
    /// ```
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

    /// Return the shard id.
    pub fn id(&self) -> &str {
        self.id.as_str()
    }

    /// Return the shard iterator id.
    pub fn iterator(&self) -> Option<&str> {
        self.iterator.as_deref()
    }

    /// Return the parent shard id.
    pub fn parent_shard_id(&self) -> Option<&str> {
        self.parent_shard_id.as_deref()
    }

    /// Return [`Option<Shard>`] with passed shard iterator id.
    /// Setting None as the shard iterator means the shard drops because None shard iterator will
    /// get no records from the DynamoDB Table.
    pub fn set_iterator(self, iterator: Option<String>) -> Option<Self> {
        if iterator.is_some() {
            Some(Self { iterator, ..self })
        } else {
            None
        }
    }
}
