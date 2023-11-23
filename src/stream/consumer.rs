use async_trait::async_trait;
use tokio::sync::oneshot::Sender;
use tracing::error;

#[async_trait]
pub trait StreamConsumer {
    fn tx_close(&mut self) -> Option<Sender<()>>;

    fn close(&mut self) {
        if let Some(tx) = self.tx_close() {
            if let Err(err) = tx.send(()) {
                error!("Unexpected error during sending close event. {:?}", err);
            }
        }
    }
}
