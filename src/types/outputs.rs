use super::Shard;

use aws_sdk_dynamodbstreams::types::Record;

#[derive(Debug, Clone)]
pub struct GetShardsOutput {
    pub shards: Vec<Shard>,
    pub next_shard_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GetRecordsOutput {
    pub shard: Shard,
    pub records: Vec<Record>,
}
