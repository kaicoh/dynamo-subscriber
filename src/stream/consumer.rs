use super::Error;

use async_trait::async_trait;
use tokio::sync::oneshot::{error::TryRecvError, Receiver, Sender};
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

    fn init_receiver(&mut self) -> &mut Receiver<()>;

    fn initialized(&self) -> bool;

    fn done_initialization(&mut self);

    fn is_initialized(&mut self) -> Result<bool, Error> {
        if self.initialized() {
            Ok(true)
        } else {
            match self.init_receiver().try_recv() {
                Ok(_) => {
                    self.done_initialization();
                    Ok(true)
                }
                Err(TryRecvError::Empty) => Ok(false),
                Err(TryRecvError::Closed) => Err(Error::Disconnected(
                    "StreamConsumer initialized channel".to_string(),
                )),
            }
        }
    }
}
