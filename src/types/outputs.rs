use super::Shard;

use aws_sdk_dynamodbstreams::types::Record;

/// An output representation of get shards operation.
#[derive(Debug, Clone)]
pub struct GetShardsOutput {
    /// The shards that are retrieved by the operation.
    pub shards: Vec<Shard>,

    /// The shard ID of the item where the operation stopped, inclusive of the previous result set.
    /// Use this value to start a new operation, excluding this value in the new request.
    ///
    /// If `last_shard_id` is None, then the "last page" of results has been processed and
    /// there is currently no more data to be retrieved.
    ///
    /// If `last_shard_id` is Some, it does not necessarily mean that there is more data
    /// in the result set. The only way to know when you have reached the end of
    /// the result set is when `last_shard_id` is None.
    pub last_shard_id: Option<String>,
}

/// An output representation of get records operation.
#[derive(Debug, Clone)]
pub struct GetRecordsOutput {
    /// The shard that the records are retrieved from and has renewed shard iterator
    /// for next `get_records` operation.
    /// The shard will be None, if the renewed shard iterator is None. Because the None shard
    /// iterator means no more records will be retrieved from the shard.
    pub shard: Option<Shard>,

    /// The records that are retrieved by the operation.
    pub records: Vec<Record>,
}
