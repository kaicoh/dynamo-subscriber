use super::{
    error::Error,
    types::{GetRecordsOutput, GetShardsOutput, Shard},
};

use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_sdk_dynamodb::Client as DbClient;
use aws_sdk_dynamodbstreams::{Client as StreamsClient, types::ShardIteratorType};

#[derive(Debug, Clone)]
pub struct Client {
    db: DbClient,
    streams: StreamsClient,
}

impl Client {
    pub fn new(config: &SdkConfig) -> Self {
        Self {
            db: DbClient::new(&config),
            streams: StreamsClient::new(&config),
        }
    }
}

#[async_trait]
pub trait DynamodbClient: Clone + Send + Sync {
    /// Return LatestStreamArn from Dynamodb table description.
    async fn get_stream_arn(
        &self,
        table_name: impl Into<String> + Send,
    ) -> Result<String, Error>;

    /// Return shards and next shard id from Dynamodb Stream description.
    async fn get_shards(
        &self,
        stream_arn: impl Into<String> + Send,
        exclusive_start_shard_id: Option<String>,
    ) -> Result<GetShardsOutput, Error>;

    /// Return shard with shard iterator id.
    async fn get_shard_with_iterator(
        &self,
        stream_arn: impl Into<String> + Send,
        shard: Shard,
        shard_iterator_type: ShardIteratorType,
    ) -> Result<Shard, Error>;

    /// Return records from shard.
    async fn get_records(
        &self,
        shard: Shard,
    ) -> Result<GetRecordsOutput, Error>;
}

#[async_trait]
impl DynamodbClient for Client {
    async fn get_stream_arn(
        &self,
        table_name: impl Into<String> + Send,
    ) -> Result<String, Error> {
        let table_name: String = table_name.into();

        self.db
            .describe_table()
            .table_name(&table_name)
            .send()
            .await
            .map_err(|err| Error::SdkError(Box::new(err)))?
            .table
            .and_then(|table| table.latest_stream_arn)
            .ok_or(Error::NotFoundStream(table_name))
    }

    async fn get_shards(
        &self,
        stream_arn: impl Into<String> + Send,
        exclusive_start_shard_id: Option<String>,
    ) -> Result<GetShardsOutput, Error> {
        let stream_arn: String = stream_arn.into();

        self.streams
            .describe_stream()
            .stream_arn(&stream_arn)
            .set_exclusive_start_shard_id(exclusive_start_shard_id)
            .send()
            .await
            .map_err(|err| Error::SdkError(Box::new(err)))?
            .stream_description
            .map(|description| {
                let shards = description
                    .shards
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(Shard::new)
                    .collect::<Vec<Shard>>();
                let next_shard_id = description.last_evaluated_shard_id;

                GetShardsOutput {
                    shards,
                    next_shard_id,
                }
            })
            .ok_or(Error::NotFoundStreamDescription(stream_arn))
    }

    async fn get_shard_with_iterator(
        &self,
        stream_arn: impl Into<String> + Send,
        shard: Shard,
        shard_iterator_type: ShardIteratorType,
    ) -> Result<Shard, Error> {
        let iterator = self.streams
            .get_shard_iterator()
            .stream_arn(stream_arn)
            .shard_id(shard.id())
            .shard_iterator_type(shard_iterator_type)
            .send()
            .await
            .map_err(|err| Error::SdkError(Box::new(err)))?
            .shard_iterator;

        Ok(shard.set_iterator(iterator))
    }

    async fn get_records(
        &self,
        shard: Shard,
    ) -> Result<GetRecordsOutput, Error> {
        let iterator = shard.iterator().map(|val| val.to_string());

        self.streams
            .get_records()
            .set_shard_iterator(iterator)
            .send()
            .await
            .map_err(|err| Error::SdkError(Box::new(err)))
            .map(|output| {
                let shard = shard.set_iterator(output.next_shard_iterator);
                let records = output.records.unwrap_or_default();

                GetRecordsOutput {
                    shard,
                    records,
                }
            })
    }
}
