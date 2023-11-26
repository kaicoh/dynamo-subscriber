use super::channel::ConsumerHalf;

use tracing::error;

pub trait StreamConsumerExt {
    fn channel(&mut self) -> &mut ConsumerHalf;

    fn close(&mut self) {
        self.channel().close(|| {
            error!("Unexpected error during sending close event.");
        });
    }

    fn initialized(&mut self) -> bool {
        self.channel().initialized()
    }
}
