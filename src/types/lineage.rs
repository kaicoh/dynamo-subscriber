use super::super::client::DynamodbClient;
use super::Shard;

use async_recursion::async_recursion;
use aws_sdk_dynamodbstreams::types::Record;
use std::{cmp, sync::Arc};
use tokio::sync::mpsc::{self, Sender};
use tracing::error;

#[derive(Debug, Clone)]
struct Lineage {
    shard: Shard,
    children: Vec<Lineage>,
}

impl Lineage {
    fn new(shard: Shard) -> Self {
        Self {
            shard,
            children: vec![],
        }
    }

    fn shard_id(&self) -> &str {
        self.shard.id()
    }

    fn parent_shard_id(&self) -> Option<&str> {
        self.shard.parent_shard_id()
    }

    fn set_children(&mut self, children: Vec<Lineage>) {
        self.children = children;
    }

    fn has(&self, shard_id: Option<&str>) -> bool {
        if let Some(id) = shard_id {
            if self.shard_id() == id {
                true
            } else {
                self.children.iter().any(|child| child.has(shard_id))
            }
        } else {
            false
        }
    }

    fn is_child(&self, shard_id: &str) -> bool {
        if let Some(parent_shard_id) = self.parent_shard_id() {
            parent_shard_id == shard_id
        } else {
            false
        }
    }

    fn set_descendant(&mut self, desc: &Lineage) {
        if let Some(parent_shard_id) = desc.parent_shard_id() {
            if self.shard_id() == parent_shard_id {
                self.children.push(desc.clone());
            } else {
                for lineage in self.children.iter_mut() {
                    lineage.set_descendant(desc);
                }
            }
        }
    }

    #[async_recursion]
    async fn get_records<Client>(
        self,
        client: Arc<Client>,
        tx: Sender<(Option<Shard>, Vec<Record>)>,
    ) where
        Client: DynamodbClient + 'static,
    {
        let Lineage { shard, children } = self;

        let (shard, records) = client
            .get_records(shard)
            .await
            .map(|output| (Some(output.shard), output.records))
            .unwrap_or_else(|err| {
                error!("Unexpected error during getting records: {err}");
                (None, vec![])
            });

        if let Err(err) = tx.send((shard, records)).await {
            error!("Unexpected error during sending shard and records: {err}");
        }

        for child in children {
            let tx = tx.clone();
            let client = Arc::clone(&client);

            tokio::spawn(async move {
                child.get_records(client, tx).await;
            });
        }
    }
}

#[derive(Debug, Clone)]
pub struct Lineages(Vec<Lineage>);

impl Lineages {
    fn new() -> Self {
        Self(vec![])
    }

    fn init(lineages: Self, shard: Shard) -> Self {
        let lineages = lineages.0;
        let (children, mut lineages): (Vec<Lineage>, Vec<Lineage>) = lineages
            .into_iter()
            .partition(|lineage| lineage.is_child(shard.id()));

        let mut lineage = Lineage::new(shard.clone());
        lineage.set_children(children);

        if let Some(ancestor) = lineages.iter_mut().find(|l| l.has(shard.parent_shard_id())) {
            ancestor.set_descendant(&lineage);
        } else {
            lineages.push(lineage);
        }

        Self(lineages)
    }

    pub async fn get_records<Client>(self, client: Arc<Client>) -> (Vec<Shard>, Vec<Record>)
    where
        Client: DynamodbClient + 'static,
    {
        let mut shards: Vec<Shard> = vec![];
        let mut records: Vec<Record> = vec![];

        let buf = cmp::max(1, shards.len());
        let (tx, mut rx) = mpsc::channel::<(Option<Shard>, Vec<Record>)>(buf);

        for lineage in self.0 {
            let client = Arc::clone(&client);
            let tx = tx.clone();

            tokio::spawn(async move {
                lineage.get_records(client, tx).await;
            });
        }

        drop(tx);

        while let Some((opt, mut _records)) = rx.recv().await {
            if let Some(shard) = opt {
                shards.push(shard);
            }

            if !_records.is_empty() {
                records.append(&mut _records);
            }
        }

        // TODO
        // sort records

        (shards, records)
    }
}

impl From<Vec<Shard>> for Lineages {
    fn from(shards: Vec<Shard>) -> Self {
        shards.into_iter().fold(Self::new(), Self::init)
    }
}
