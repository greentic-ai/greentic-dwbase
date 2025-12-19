//! Local in-memory stream engine using per-subscriber channels.
//!
//! This implementation is synchronous and uses poll semantics (no async streams) to stay WASM-friendly.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Mutex;

use dwbase_core::{Atom, WorldKey};
use dwbase_engine::{AtomFilter, DwbaseError, Result, StreamEngine};

#[derive(Debug)]
struct Subscriber {
    filter: AtomFilter,
    sender: Sender<Atom>,
    receiver: Receiver<Atom>,
}

/// Local implementation of `StreamEngine` using per-subscriber channels and poll semantics.
pub struct LocalStreamEngine {
    next_id: AtomicUsize,
    subscribers: Mutex<HashMap<usize, Subscriber>>,
}

impl LocalStreamEngine {
    pub fn new() -> Self {
        Self {
            next_id: AtomicUsize::new(1),
            subscribers: Mutex::new(HashMap::new()),
        }
    }

    fn matches_filter(atom: &Atom, filter: &AtomFilter) -> bool {
        if let Some(world) = &filter.world {
            if atom.world() != world {
                return false;
            }
        }
        if !filter.kinds.is_empty() && !filter.kinds.contains(atom.kind()) {
            return false;
        }
        if !filter.labels.is_empty() && !filter.labels.iter().all(|l| atom.labels().contains(l)) {
            return false;
        }
        if !filter.flags.is_empty() && !filter.flags.iter().all(|f| atom.flags().contains(f)) {
            return false;
        }
        if let Some(since) = &filter.since {
            if atom.timestamp().0 < since.0 {
                return false;
            }
        }
        if let Some(until) = &filter.until {
            if atom.timestamp().0 > until.0 {
                return false;
            }
        }
        true
    }
}

impl Default for LocalStreamEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamEngine for LocalStreamEngine {
    type Handle = usize;

    fn publish(&self, atom: &Atom) -> Result<()> {
        let mut to_remove = Vec::new();
        let subscribers = self.subscribers.lock().expect("subscribers lock poisoned");
        for (id, sub) in subscribers.iter() {
            if Self::matches_filter(atom, &sub.filter) && sub.sender.send(atom.clone()).is_err() {
                to_remove.push(*id);
            }
        }
        drop(subscribers);

        if !to_remove.is_empty() {
            let mut subs = self.subscribers.lock().expect("subscribers lock poisoned");
            for id in to_remove {
                subs.remove(&id);
            }
        }

        Ok(())
    }

    fn subscribe(&self, world: &WorldKey, filter: AtomFilter) -> Result<Self::Handle> {
        let (tx, rx) = mpsc::channel();
        let mut filter = filter;
        filter.world = Some(world.clone());
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let subscriber = Subscriber {
            filter,
            sender: tx,
            receiver: rx,
        };
        let mut subscribers = self.subscribers.lock().expect("subscribers lock poisoned");
        subscribers.insert(id, subscriber);
        Ok(id)
    }

    fn poll(&self, handle: &Self::Handle) -> Result<Option<Atom>> {
        let mut subscribers = self.subscribers.lock().expect("subscribers lock poisoned");
        if let Some(sub) = subscribers.get_mut(handle) {
            match sub.receiver.try_recv() {
                Ok(atom) => Ok(Some(atom)),
                Err(mpsc::TryRecvError::Empty) => Ok(None),
                Err(mpsc::TryRecvError::Disconnected) => {
                    subscribers.remove(handle);
                    Ok(None)
                }
            }
        } else {
            Err(DwbaseError::Stream(format!("unknown handle {}", handle)))
        }
    }

    fn stop(&self, handle: Self::Handle) -> Result<()> {
        let mut subscribers = self.subscribers.lock().expect("subscribers lock poisoned");
        subscribers.remove(&handle);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dwbase_core::{Atom, AtomId, AtomKind, Importance, Timestamp, WorkerKey};

    fn atom(id: &str, world: &str, labels: &[&str]) -> Atom {
        let mut builder = Atom::builder(
            AtomId::new(id),
            WorldKey::new(world),
            WorkerKey::new("wkr"),
            AtomKind::Observation,
            Timestamp::new("2024-01-01T00:00:00Z"),
            Importance::new(0.5).unwrap(),
            "{}",
        );
        for l in labels {
            builder = builder.add_label(*l);
        }
        builder.build()
    }

    #[test]
    fn subscribe_publish_poll_delivers_atoms() {
        let engine = LocalStreamEngine::new();
        let world = WorldKey::new("w1");
        let handle = engine
            .subscribe(&world, AtomFilter::default())
            .expect("subscribe");
        let a1 = atom("a1", "w1", &[]);
        engine.publish(&a1).unwrap();

        let polled = engine.poll(&handle).unwrap();
        assert_eq!(polled.as_ref().unwrap().id(), a1.id());
        // no additional items
        assert!(engine.poll(&handle).unwrap().is_none());
    }

    #[test]
    fn filtering_is_respected() {
        let engine = LocalStreamEngine::new();
        let world = WorldKey::new("w1");
        let filter = AtomFilter {
            world: Some(world.clone()),
            kinds: vec![AtomKind::Observation],
            labels: vec!["keep".into()],
            flags: vec![],
            since: None,
            until: None,
            limit: None,
        };
        let handle = engine.subscribe(&world, filter).unwrap();

        let drop_atom = atom("drop", "w1", &["other"]);
        let keep_atom = atom("keep", "w1", &["keep"]);

        engine.publish(&drop_atom).unwrap();
        engine.publish(&keep_atom).unwrap();

        // drop is filtered out
        assert!(engine.poll(&handle).unwrap().is_some());
        let polled = engine.poll(&handle).unwrap();
        assert!(polled.is_none());
    }
}
