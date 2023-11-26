use tokio::sync::oneshot::{self, error::TryRecvError, Receiver, Sender};
use tracing::error;

pub fn new() -> (ProducerHalf, ConsumerHalf) {
    let (tx_init, rx_init) = oneshot::channel::<()>();
    let (tx_close, rx_close) = oneshot::channel::<()>();
    (
        ProducerHalf::new(tx_init, rx_close),
        ConsumerHalf::new(tx_close, rx_init),
    )
}

#[derive(Debug)]
pub struct ProducerHalf {
    /// Sender half to send `Initialized` event.
    sender: Option<Sender<()>>,

    /// Receiver half to receive `Close` event.
    receiver: Receiver<()>,
}

impl ProducerHalf {
    fn new(sender: Sender<()>, receiver: Receiver<()>) -> Self {
        Self {
            sender: Some(sender),
            receiver,
        }
    }

    pub fn send_init(&mut self) {
        if let Some(tx) = self.sender.take() {
            if let Err(err) = tx.send(()) {
                error!(
                    "Unexpected error during sending initialized event: {:?}",
                    err
                );
            }
        }
    }

    pub fn should_close(&mut self) -> bool {
        !matches!(self.receiver.try_recv(), Err(TryRecvError::Empty))
    }
}

#[derive(Debug)]
pub struct ConsumerHalf {
    /// Sender half to send `Close` event.
    sender: Option<Sender<()>>,

    /// Receiver half to receive `Initialized` event.
    receiver: Receiver<()>,
}

impl ConsumerHalf {
    fn new(sender: Sender<()>, receiver: Receiver<()>) -> Self {
        Self {
            sender: Some(sender),
            receiver,
        }
    }

    pub fn close(&mut self, f: impl FnOnce()) {
        if let Some(tx) = self.sender.take() {
            let _ = tx.send(()).map_err(|_| f());
        }
    }

    pub fn initialized(&mut self) -> bool {
        matches!(self.receiver.try_recv(), Err(TryRecvError::Closed))
    }
}
