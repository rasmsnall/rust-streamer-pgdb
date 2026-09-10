//! A bounded multi-producer, multi-consumer channel.
//!
//! `std::sync::mpsc` is single-consumer, so it cannot feed a pool of decode workers from
//! one reader. This is the smallest thing that can: a `VecDeque` behind a `Mutex` with a
//! not-empty and a not-full `Condvar`. No `unsafe`, and no new dependency.
//!
//! Both ends are `Clone` and `Send`. [`Sender::send`] blocks while the queue is full;
//! [`Receiver::recv`] blocks while it is empty and returns `None` once every [`Sender`]
//! is dropped, which is how the pool learns to shut down. Symmetrically, `send` returns
//! the value back as `Err` once every [`Receiver`] is dropped.
//!
//! Executes on whichever threads hold the ends. Nothing here is async.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

/// Shared state behind both ends of one channel.
struct Shared<T> {
    state: Mutex<State<T>>,
    not_empty: Condvar,
    not_full: Condvar,
    capacity: usize,
}

/// The queue and the live end counts, all guarded by one lock.
struct State<T> {
    items: VecDeque<T>,
    senders: usize,
    receivers: usize,
}

/// The sending end. Clone it to add a producer.
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

/// The receiving end. Clone it to add a consumer.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
}

/// Creates a channel that holds at most `capacity` items, clamped up to one.
///
/// # Panics
///
/// Does not panic.
///
/// # Examples
///
/// ```
/// use pgdelta::chan::bounded;
///
/// let (tx, rx) = bounded::<u32>(4);
/// tx.send(1).unwrap();
/// drop(tx);
/// assert_eq!(rx.recv(), Some(1));
/// assert_eq!(rx.recv(), None);
/// ```
pub fn bounded<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            items: VecDeque::new(),
            senders: 1,
            receivers: 1,
        }),
        not_empty: Condvar::new(),
        not_full: Condvar::new(),
        capacity: capacity.max(1),
    });
    (
        Sender {
            shared: Arc::clone(&shared),
        },
        Receiver { shared },
    )
}

impl<T> Sender<T> {
    /// Appends `value`, blocking while the queue is full.
    ///
    /// # Errors
    ///
    /// Returns `Err(value)`, handing the value back, if every [`Receiver`] has been
    /// dropped.
    ///
    /// # Panics
    ///
    /// Panics only if the lock was poisoned by another thread panicking while holding it.
    pub fn send(&self, value: T) -> Result<(), T> {
        let mut state = self.shared.state.lock().unwrap();
        loop {
            if state.receivers == 0 {
                return Err(value);
            }
            if state.items.len() < self.shared.capacity {
                state.items.push_back(value);
                drop(state);
                self.shared.not_empty.notify_one();
                return Ok(());
            }
            state = self.shared.not_full.wait(state).unwrap();
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.state.lock().unwrap().senders += 1;
        Sender {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().unwrap();
        state.senders -= 1;
        if state.senders == 0 {
            drop(state);
            // Wake every blocked receiver so each can observe the closed channel.
            self.shared.not_empty.notify_all();
        }
    }
}

impl<T> Receiver<T> {
    /// Removes and returns the oldest item, blocking while the queue is empty.
    ///
    /// # Errors
    ///
    /// Returns `None`, permanently, once the queue is empty and every [`Sender`] has been
    /// dropped.
    ///
    /// # Panics
    ///
    /// Panics only if the lock was poisoned by another thread panicking while holding it.
    pub fn recv(&self) -> Option<T> {
        let mut state = self.shared.state.lock().unwrap();
        loop {
            if let Some(value) = state.items.pop_front() {
                drop(state);
                self.shared.not_full.notify_one();
                return Some(value);
            }
            if state.senders == 0 {
                return None;
            }
            state = self.shared.not_empty.wait(state).unwrap();
        }
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.shared.state.lock().unwrap().receivers += 1;
        Receiver {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().unwrap();
        state.receivers -= 1;
        if state.receivers == 0 {
            drop(state);
            // Wake every blocked sender so each can observe the closed channel.
            self.shared.not_full.notify_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn delivers_in_fifo_order() {
        let (tx, rx) = bounded(8);
        for i in 0..8 {
            tx.send(i).unwrap();
        }
        drop(tx);
        let got: Vec<_> = std::iter::from_fn(|| rx.recv()).collect();
        assert_eq!(got, (0..8).collect::<Vec<_>>());
    }

    #[test]
    fn recv_ends_when_all_senders_drop() {
        let (tx, rx) = bounded::<u8>(2);
        let tx2 = tx.clone();
        drop(tx);
        drop(tx2);
        assert_eq!(rx.recv(), None);
    }

    #[test]
    fn send_fails_when_all_receivers_drop() {
        let (tx, rx) = bounded(2);
        let rx2 = rx.clone();
        drop(rx);
        drop(rx2);
        assert_eq!(tx.send(9), Err(9));
    }

    #[test]
    fn send_blocks_until_a_receiver_makes_room() {
        let (tx, rx) = bounded(1);
        tx.send(1).unwrap();

        let writer = thread::spawn(move || {
            // Blocks: the queue is full until the reader takes the first item.
            tx.send(2).unwrap();
            tx.send(3).unwrap();
        });

        thread::sleep(Duration::from_millis(20));
        assert_eq!(rx.recv(), Some(1));
        assert_eq!(rx.recv(), Some(2));
        assert_eq!(rx.recv(), Some(3));
        writer.join().unwrap();
    }

    #[test]
    fn many_producers_and_consumers_lose_nothing() {
        let (tx, rx) = bounded(16);
        let producers = 4;
        let per = 250;

        let mut handles = Vec::new();
        for p in 0..producers {
            let tx = tx.clone();
            handles.push(thread::spawn(move || {
                for i in 0..per {
                    tx.send(p * per + i).unwrap();
                }
            }));
        }
        drop(tx);

        let consumers: Vec<_> = (0..3)
            .map(|_| {
                let rx = rx.clone();
                thread::spawn(move || {
                    let mut seen = Vec::new();
                    while let Some(v) = rx.recv() {
                        seen.push(v);
                    }
                    seen
                })
            })
            .collect();
        drop(rx);

        for h in handles {
            h.join().unwrap();
        }
        let mut all: Vec<_> = consumers
            .into_iter()
            .flat_map(|c| c.join().unwrap())
            .collect();
        all.sort_unstable();
        assert_eq!(all, (0..producers * per).collect::<Vec<_>>());
    }
}
