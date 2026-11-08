use std::time::Duration;

/// Exponential backoff for the sleep between polls.
///
/// The delay starts at `min`. Every empty poll grows it by `multiplier`,
/// up to `max`. A poll that returns a message resets it to `min`, so a
/// busy queue is polled again right away and an idle queue costs one
/// receive per `max` interval (plus the long poll wait itself).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Backoff {
    min: Duration,
    max: Duration,
    multiplier: f64,
    current: Duration,
}

impl Backoff {
    /// Creates a backoff starting at `min`, growing by `multiplier` per
    /// empty poll, and never sleeping longer than `max`.
    ///
    /// # Panics
    ///
    /// Panics if `min` is zero, if `max < min`, or if `multiplier < 1.0`.
    pub fn new(min: Duration, max: Duration, multiplier: f64) -> Self {
        assert!(!min.is_zero(), "min interval must be positive");
        assert!(max >= min, "max interval must be at least min");
        assert!(multiplier >= 1.0, "multiplier must be at least 1.0");
        Self {
            min,
            max,
            multiplier,
            current: min,
        }
    }

    /// The delay to sleep now, before the next poll.
    pub fn current(&self) -> Duration {
        self.current
    }

    /// Records an empty poll: returns the delay to sleep, then grows the
    /// delay for the next empty poll.
    pub fn on_empty(&mut self) -> Duration {
        let delay = self.current;
        let grown = self.current.mul_f64(self.multiplier);
        self.current = grown.min(self.max);
        delay
    }

    /// Records a poll that returned at least one message: the next empty
    /// poll starts from `min` again.
    pub fn on_message(&mut self) {
        self.current = self.min;
    }

    /// Restarts the sequence from `min`.
    pub fn reset(&mut self) {
        self.current = self.min;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backoff() -> Backoff {
        Backoff::new(Duration::from_secs(1), Duration::from_secs(60), 2.0)
    }

    #[test]
    fn grows_exponentially() {
        let mut b = backoff();
        let delays: Vec<Duration> = (0..5).map(|_| b.on_empty()).collect();
        assert_eq!(
            delays,
            vec![
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4),
                Duration::from_secs(8),
                Duration::from_secs(16),
            ]
        );
    }

    #[test]
    fn caps_at_max() {
        let mut b = backoff();
        for _ in 0..20 {
            b.on_empty();
        }
        assert_eq!(b.on_empty(), Duration::from_secs(60));
        assert_eq!(b.current(), Duration::from_secs(60));
    }

    #[test]
    fn message_resets_to_min() {
        let mut b = backoff();
        b.on_empty();
        b.on_empty();
        b.on_empty();
        b.on_message();
        assert_eq!(b.on_empty(), Duration::from_secs(1));
    }

    #[test]
    fn fractional_multiplier() {
        let mut b = Backoff::new(Duration::from_millis(100), Duration::from_secs(10), 1.5);
        assert_eq!(b.on_empty(), Duration::from_millis(100));
        assert_eq!(b.on_empty(), Duration::from_millis(150));
        assert_eq!(b.on_empty(), Duration::from_millis(225));
    }
}
