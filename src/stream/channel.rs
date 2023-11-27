use tokio::sync::oneshot::{self, error::TryRecvError, Receiver, Sender};
use tracing::error;

/// Create a pair of channel.
pub fn new() -> (ProducerChannel, ConsumerChannel) {
    let (tx_init, rx_init) = oneshot::channel::<()>();
    let (tx_close, rx_close) = oneshot::channel::<()>();
    (
        ProducerChannel::new(tx_init, rx_close),
        ConsumerChannel::new(tx_close, rx_init),
    )
}

/// Communication channel half in the stream.
///
/// Using this channel, the stream does the following.
///
/// - Send `Initialized` event to the channel half.
/// - Receive `Stop polling` event from the channel half.
#[derive(Debug)]
pub struct ProducerChannel {
    /// Sender half to send `Initialized` event.
    sender: Option<Sender<()>>,

    /// Receiver half to receive `Close` event.
    receiver: Receiver<()>,
}

impl ProducerChannel {
    fn new(sender: Sender<()>, receiver: Receiver<()>) -> Self {
        Self {
            sender: Some(sender),
            receiver,
        }
    }

    /// Send `Initialized` event to the channel half.
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

    /// Return true if the `Stop polling` event is received.
    pub fn should_close(&mut self) -> bool {
        !matches!(self.receiver.try_recv(), Err(TryRecvError::Empty))
    }
}

/// Communication channel half to the stream.
///
/// Using this channel, you can do the following.
///
/// - Confirm that the stream is ready to polling or not.
/// - Send `Stop polling` event to the stream.
#[derive(Debug)]
pub struct ConsumerChannel {
    /// Sender half to send `Close` event.
    sender: Option<Sender<()>>,

    /// Receiver half to receive `Initialized` event.
    receiver: Receiver<()>,
}

impl ConsumerChannel {
    fn new(sender: Sender<()>, receiver: Receiver<()>) -> Self {
        Self {
            sender: Some(sender),
            receiver,
        }
    }

    /// Send `Stop polling` event to the stream. The passed closure is executed only when
    /// sending event fails.
    pub fn close(&mut self, f: impl FnOnce()) {
        if let Some(tx) = self.sender.take() {
            let _ = tx.send(()).map_err(|_| f());
        }
    }

    /// Return true if the stream is ready to polling.
    pub fn initialized(&mut self) -> bool {
        matches!(self.receiver.try_recv(), Err(TryRecvError::Closed))
    }
}
