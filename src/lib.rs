//! An SQS poller that backs off exponentially while the queue is empty.
//!
//! Long polling alone does not fix the cost of watching an idle queue: a
//! 20 second long poll still adds up to about 4,320 empty receives a day,
//! per queue, per consumer. This crate sleeps between polls instead. The
//! sleep starts short, grows exponentially while the queue stays empty,
//! and resets to the minimum the moment a message arrives, so a busy
//! queue is drained at full speed and an idle queue costs one receive
//! per `max_interval` (plus the long poll wait itself).
//!
//! ```no_run
//! use std::time::Duration;
//! use sqs_poller::{run, Config};
//! use tokio_util::sync::CancellationToken;
//!
//! #[tokio::main]
//! async fn main() {
//!     let aws = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
//!     let client = aws_sdk_sqs::Client::new(&aws);
//!     let config = Config::new("https://sqs.ap-northeast-1.amazonaws.com/123456789012/jobs");
//!     let shutdown = CancellationToken::new();
//!     let stats = run(config, client, |message| async move {
//!         println!("{}", message.body.as_deref().unwrap_or_default());
//!         Ok(())
//!     }, shutdown).await;
//!     println!("received {}, deleted {}", stats.received, stats.deleted);
//! }
//! ```

mod backoff;

use std::error::Error as StdError;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_sqs::Client;
use tokio_util::sync::CancellationToken;

pub use aws_sdk_sqs::types::Message;
pub use backoff::Backoff;

/// The result a handler returns. `Ok(())` deletes the message. `Err`
/// leaves it in the queue, where the visibility timeout makes it
/// available again and the queue's redrive policy eventually sends it
/// to the dead-letter queue.
pub type HandlerResult = Result<(), Box<dyn StdError + Send + Sync>>;

/// Errors the poller reports through the `on_error` callback. Polling
/// continues after every error, so these are notifications, not control
/// flow.
#[derive(Debug)]
pub enum Error {
    /// A `ReceiveMessage` call failed.
    Receive(Box<dyn StdError + Send + Sync>),
    /// A `DeleteMessage` call failed. The message stays in the queue and
    /// will be delivered again.
    Delete(Box<dyn StdError + Send + Sync>),
    /// The handler returned an error for a message. The message stays in
    /// the queue.
    Handler(Box<dyn StdError + Send + Sync>),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Receive(e) => write!(f, "receive failed: {e}"),
            Error::Delete(e) => write!(f, "delete failed: {e}"),
            Error::Handler(e) => write!(f, "handler failed: {e}"),
        }
    }
}

impl StdError for Error {}

/// Poller configuration.
#[derive(Clone)]
pub struct Config {
    /// The queue to poll.
    pub queue_url: String,
    /// Sleep after the first empty poll, and the value the backoff
    /// resets to when a message arrives. Default 1 s.
    pub min_interval: Duration,
    /// Longest sleep between polls. Default 300 s.
    pub max_interval: Duration,
    /// Growth factor per empty poll. Default 2.0.
    pub multiplier: f64,
    /// `WaitTimeSeconds` of each receive (long polling). Default 20.
    pub wait_time_seconds: i32,
    /// `MaxNumberOfMessages` of each receive. Default 10.
    pub max_messages: i32,
    /// Applies equal jitter to each sleep (the second half of the delay
    /// is randomized), so many pollers on the same queue do not wake up
    /// in lockstep. Default true.
    pub jitter: bool,
    /// Called with every receive, delete, and handler error. Polling
    /// continues regardless. Default: errors are dropped silently.
    pub on_error: Option<Arc<dyn Fn(Error) + Send + Sync>>,
}

impl Config {
    pub fn new(queue_url: impl Into<String>) -> Self {
        Self {
            queue_url: queue_url.into(),
            min_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(300),
            multiplier: 2.0,
            wait_time_seconds: 20,
            max_messages: 10,
            jitter: true,
            on_error: None,
        }
    }

    fn report(&self, error: Error) {
        if let Some(on_error) = &self.on_error {
            on_error(error);
        }
    }
}

/// Counters accumulated over one [`run`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    /// Messages handed to the handler.
    pub received: u64,
    /// Messages deleted after the handler returned `Ok`.
    pub deleted: u64,
    /// Messages the handler rejected, plus messages whose delete call
    /// failed. Both stay in the queue for redelivery.
    pub failed: u64,
    /// Receives that returned no message.
    pub empty_polls: u64,
    /// `ReceiveMessage` calls that failed.
    pub receive_errors: u64,
}

fn jittered(delay: Duration, jitter: bool) -> Duration {
    if !jitter {
        return delay;
    }
    let half = delay / 2;
    half + half.mul_f64(fastrand::f64())
}

/// Sleeps for `delay`, returning `false` if shutdown fired first.
async fn sleep_or_shutdown(delay: Duration, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        _ = shutdown.cancelled() => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

/// Polls the queue until `shutdown` is cancelled, handing each message
/// to `handler`.
///
/// The first poll runs immediately. While receives come back empty, the
/// sleep between polls grows exponentially (see [`Backoff`]); a receive
/// with at least one message resets it. Messages are processed one at a
/// time, in receive order.
///
/// # Panics
///
/// Panics if the intervals and multiplier do not satisfy the
/// [`Backoff::new`] requirements.
pub async fn run<F, Fut>(
    config: Config,
    client: Client,
    handler: F,
    shutdown: CancellationToken,
) -> Stats
where
    F: Fn(Message) -> Fut + Send + Sync,
    Fut: Future<Output = HandlerResult> + Send,
{
    let mut backoff = Backoff::new(config.min_interval, config.max_interval, config.multiplier);
    let mut stats = Stats::default();
    loop {
        let output = tokio::select! {
            _ = shutdown.cancelled() => break,
            out = client
                .receive_message()
                .queue_url(&config.queue_url)
                .wait_time_seconds(config.wait_time_seconds)
                .max_number_of_messages(config.max_messages)
                .send() => out,
        };
        match output {
            Ok(out) => {
                let messages = out.messages.unwrap_or_default();
                if messages.is_empty() {
                    stats.empty_polls += 1;
                    let delay = jittered(backoff.on_empty(), config.jitter);
                    if !sleep_or_shutdown(delay, &shutdown).await {
                        break;
                    }
                } else {
                    backoff.on_message();
                    for message in messages {
                        stats.received += 1;
                        let receipt_handle = message.receipt_handle.clone();
                        match handler(message).await {
                            Ok(()) => match receipt_handle {
                                Some(handle) => {
                                    match client
                                        .delete_message()
                                        .queue_url(&config.queue_url)
                                        .receipt_handle(handle)
                                        .send()
                                        .await
                                    {
                                        Ok(_) => stats.deleted += 1,
                                        Err(e) => {
                                            stats.failed += 1;
                                            config.report(Error::Delete(Box::new(e)));
                                        }
                                    }
                                }
                                None => {
                                    stats.failed += 1;
                                    config.report(Error::Delete(Box::new(std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        "message had no receipt handle",
                                    ))));
                                }
                            },
                            Err(e) => {
                                stats.failed += 1;
                                config.report(Error::Handler(e));
                            }
                        }
                    }
                }
            }
            Err(e) => {
                stats.receive_errors += 1;
                config.report(Error::Receive(Box::new(e)));
                let delay = jittered(backoff.on_empty(), config.jitter);
                if !sleep_or_shutdown(delay, &shutdown).await {
                    break;
                }
            }
        }
    }
    stats
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults() {
        let config = Config::new("https://example.com/queue");
        assert_eq!(config.min_interval, Duration::from_secs(1));
        assert_eq!(config.max_interval, Duration::from_secs(300));
        assert_eq!(config.multiplier, 2.0);
        assert_eq!(config.wait_time_seconds, 20);
        assert_eq!(config.max_messages, 10);
        assert!(config.jitter);
    }

    #[test]
    fn jitter_stays_within_half_to_full() {
        let delay = Duration::from_secs(100);
        for _ in 0..100 {
            let got = jittered(delay, true);
            assert!(got >= Duration::from_secs(50) && got <= delay, "{got:?}");
        }
        assert_eq!(jittered(delay, false), delay);
    }
}
