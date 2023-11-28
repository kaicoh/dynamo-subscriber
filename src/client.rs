use super::{
    error::Error,
    types::{GetRecordsOutput, GetShardsOutput, Shard},
};

use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_sdk_dynamodb::Client as DbClient;
use aws_sdk_dynamodbstreams::{
    error::SdkError,
    operation::{
        get_records::{GetRecordsError, GetRecordsOutput as SdkGetRecordsOutput},
        get_shard_iterator::GetShardIteratorError,
    },
    types::ShardIteratorType,
    Client as StreamsClient,
};
use tracing::warn;

/// Client for both Amazon DynamoDB and Amazon DynamoDB Streams.
///
/// A [`SdkConfig`] is required to construct a client.
/// You can select any ways to get [`SdkConfig`] and pass it
/// to a client.
///
/// For example, if you want to subscribe dynamodb streams from your dynamodb-local
/// running on localhost:8000, set `endpoint_url` to your [`SdkConfig`].
///
/// ```rust,no_run
/// use dynamo_subscriber::Client;
///
/// # async fn wrapper() {
/// let config = aws_config::load_from_env()
///     .await
///     .into_builder()
///     .endpoint_url("http://localhost:8000")
///     .build();
/// let client = Client::new(&config);
/// # }
/// ```
/// See the [`aws-config` docs](aws_config) for more information on customizing configuration.
#[derive(Debug, Clone)]
pub struct Client {
    db: DbClient,
    streams: StreamsClient,
}

impl Client {
    /// Create a new client using passed configuration.
    ///
    /// ```rust,no_run
    /// use dynamo_subscriber::Client;
    ///
    /// # async fn wrapper() {
    /// let config = aws_config::load_from_env().await;
    /// let client = Client::new(&config);
    /// # }
    /// ```
    pub fn new(config: &SdkConfig) -> Self {
        Self {
            db: DbClient::new(config),
            streams: StreamsClient::new(config),
        }
    }
}

/// An interface to receive DynamoDB Streams records.
#[async_trait]
pub trait DynamodbClient: Clone + Send + Sync {
    /// Return DynamoDB Stream Arn from DynamoDB
    /// [`TableDescription`](aws_sdk_dynamodb::types::TableDescription).
    async fn get_stream_arn(&self, table_name: impl Into<String> + Send) -> Result<String, Error>;

    /// Return a vector of [`Shard`](crate::types::Shard) and shard id for next iteration.
    async fn get_shards(
        &self,
        stream_arn: impl Into<String> + Send,
        exclusive_start_shard_id: Option<String>,
    ) -> Result<GetShardsOutput, Error>;

    /// Return a [`Option<Shard>`](crate::types::Shard) that is the shard passed as an argument with shard
    /// iterator id.
    /// Return None if the aws sdk operation fails due to `ResourceNotFound` of `TrimmedDataAccess`.
    async fn get_shard_with_iterator(
        &self,
        stream_arn: impl Into<String> + Send,
        shard: Shard,
        shard_iterator_type: ShardIteratorType,
    ) -> Result<Option<Shard>, Error>;

    /// Return a vector of [`Record`](aws_sdk_dynamodbstreams::types::Record) and a
    /// [`Shard`](crate::types::Shard) with shard iterator id for next getting records call.
    async fn get_records(&self, shard: Shard) -> Result<GetRecordsOutput, Error>;
}

#[async_trait]
impl DynamodbClient for Client {
    async fn get_stream_arn(&self, table_name: impl Into<String> + Send) -> Result<String, Error> {
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
                let last_shard_id = description.last_evaluated_shard_id;

                GetShardsOutput {
                    shards,
                    last_shard_id,
                }
            })
            .ok_or(Error::NotFoundStreamDescription(stream_arn))
    }

    async fn get_shard_with_iterator(
        &self,
        stream_arn: impl Into<String> + Send,
        shard: Shard,
        shard_iterator_type: ShardIteratorType,
    ) -> Result<Option<Shard>, Error> {
        let iterator = self
            .streams
            .get_shard_iterator()
            .stream_arn(stream_arn)
            .shard_id(shard.id())
            .shard_iterator_type(shard_iterator_type)
            .send()
            .await
            .map(|output| output.shard_iterator)
            .or_else(empty_iterator)?;

        Ok(shard.set_iterator(iterator))
    }

    async fn get_records(&self, shard: Shard) -> Result<GetRecordsOutput, Error> {
        let iterator = shard.iterator().map(|val| val.to_string());

        self.streams
            .get_records()
            .set_shard_iterator(iterator)
            .send()
            .await
            .or_else(empty_records)
            .map(|output| {
                let shard = shard.set_iterator(output.next_shard_iterator);
                let records = output.records.unwrap_or_default();

                GetRecordsOutput { shard, records }
            })
    }
}

fn empty_iterator(err: SdkError<GetShardIteratorError>) -> Result<Option<String>, Error> {
    use GetShardIteratorError::*;

    match err {
        SdkError::ServiceError(e) => {
            let e = e.into_err();
            match e {
                // Retrun Ok(None) if the response is either `ResourceNotFound` or `TrimmedDataAccess`
                // This means the shard will drop silently because returning None as shard iterator
                // id results in returning Ok(None) from `get_shard_with_iterator` method.
                ResourceNotFoundException(_) | TrimmedDataAccessException(_) => {
                    warn!("GetShardIterator operation failed due to {e}");
                    warn!("{:#?}", e);
                    Ok(None)
                }
                _ => Err(Error::SdkError(Box::new(e))),
            }
        }
        _ => Err(Error::SdkError(Box::new(err))),
    }
}

fn empty_records(err: SdkError<GetRecordsError>) -> Result<SdkGetRecordsOutput, Error> {
    use GetRecordsError::*;

    match err {
        SdkError::ServiceError(e) => {
            let e = e.into_err();
            match e {
                // Retrun Ok with default SdkGetRecordsOutput if the response is one of
                // `ExpiredIterator`, `LimitExceeded`, `ResourceNotFound` and `TrimmedDataAccess`.
                // This means the shard will drop silently because returning None as shard iterator
                // id results in returning None as shard in GetRecordsOutput from `get_records` method.
                ExpiredIteratorException(_)
                | LimitExceededException(_)
                | ResourceNotFoundException(_)
                | TrimmedDataAccessException(_) => {
                    warn!("GetRecords operation failed due to {e}");
                    warn!("{:#?}", e);
                    Ok(SdkGetRecordsOutput::builder().build())
                }
                _ => Err(Error::SdkError(Box::new(e))),
            }
        }
        _ => Err(Error::SdkError(Box::new(err))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_smithy_runtime_api::{
        client::{orchestrator::HttpResponse, result::ServiceError},
        http::StatusCode,
    };
    use aws_smithy_types::body::SdkBody;

    #[test]
    fn empty_iterator_converts_some_errors_to_ok() {
        use aws_sdk_dynamodbstreams::types::error::*;

        let e = ResourceNotFoundException::builder()
            .message("error")
            .build();
        let err = service_error(GetShardIteratorError::ResourceNotFoundException(e));
        assert!(empty_iterator(err).is_ok());

        let e = InternalServerError::builder().message("error").build();
        let err = service_error(GetShardIteratorError::InternalServerError(e));
        assert!(empty_iterator(err).is_err());

        let e = TrimmedDataAccessException::builder()
            .message("error")
            .build();
        let err = service_error(GetShardIteratorError::TrimmedDataAccessException(e));
        assert!(empty_iterator(err).is_ok());
    }

    #[test]
    fn empty_records_converts_some_errors_to_ok() {
        use aws_sdk_dynamodbstreams::types::error::*;

        let e = ResourceNotFoundException::builder()
            .message("error")
            .build();
        let err = service_error(GetRecordsError::ResourceNotFoundException(e));
        assert!(empty_records(err).is_ok());

        let e = InternalServerError::builder().message("error").build();
        let err = service_error(GetRecordsError::InternalServerError(e));
        assert!(empty_records(err).is_err());

        let e = ExpiredIteratorException::builder().message("error").build();
        let err = service_error(GetRecordsError::ExpiredIteratorException(e));
        assert!(empty_records(err).is_ok());

        let e = LimitExceededException::builder().message("error").build();
        let err = service_error(GetRecordsError::LimitExceededException(e));
        assert!(empty_records(err).is_ok());

        let e = TrimmedDataAccessException::builder()
            .message("error")
            .build();
        let err = service_error(GetRecordsError::TrimmedDataAccessException(e));
        assert!(empty_records(err).is_ok());
    }

    fn service_error<E>(error: E) -> SdkError<E, HttpResponse> {
        let resp = HttpResponse::new(StatusCode::try_from(400).unwrap(), SdkBody::empty());
        let inner = ServiceError::builder().source(error).raw(resp).build();
        SdkError::ServiceError(inner)
    }
}
