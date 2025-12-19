//! Security primitives for DWBase: capabilities, trust, and gatekeeping.
//!
//! The gatekeeper enforces per-worker capabilities and simple rate limits using a token bucket.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use dwbase_core::{AtomKind, Importance, WorldKey};
use dwbase_engine::{DwbaseError, Gatekeeper, NewAtom, Question, Result, WorldAction};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Capabilities {
    pub read_worlds: Vec<WorldKey>,
    pub write_worlds: Vec<WorldKey>,
    pub labels_write: Vec<String>,
    pub kinds_write: Vec<AtomKind>,
    pub importance_cap: Option<f32>,
    pub rate_limits: RateLimits,
    pub offline_policy: OfflinePolicy,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RateLimits {
    /// Max remembers per second.
    pub remember_per_sec: Option<f64>,
    /// Max asks per minute.
    pub ask_per_min: Option<f64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub enum OfflinePolicy {
    #[default]
    Allow,
    Deny,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TrustStore {
    pub worker_scores: HashMap<String, f32>,
    pub node_scores: HashMap<String, f32>,
}

impl TrustStore {
    pub fn get_worker(&self, worker: &str) -> Option<f32> {
        self.worker_scores.get(worker).copied()
    }
}

#[derive(Debug, Error)]
pub enum SecurityError {
    #[error("capability denied: {0}")]
    Capability(String),
    #[error("rate limited: {0}")]
    RateLimited(String),
}

impl From<SecurityError> for DwbaseError {
    fn from(e: SecurityError) -> Self {
        DwbaseError::CapabilityDenied(e.to_string())
    }
}

trait Clock: Send + Sync {
    fn now(&self) -> Instant;
}

#[derive(Default)]
struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

#[derive(Debug)]
struct TokenBucket {
    capacity: f64,
    tokens: f64,
    last_refill: Instant,
    refill_per_sec: f64,
}

impl TokenBucket {
    fn new(rate_per_sec: f64, now: Instant) -> Self {
        let capacity = rate_per_sec.max(1.0);
        Self {
            capacity,
            tokens: capacity,
            last_refill: now,
            refill_per_sec: rate_per_sec,
        }
    }

    fn allow(&mut self, now: Instant) -> bool {
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        self.last_refill = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

pub struct LocalGatekeeper {
    caps: Capabilities,
    #[allow(dead_code)]
    trust: TrustStore,
    clock: Arc<dyn Clock>,
    remember_buckets: Mutex<HashMap<String, TokenBucket>>,
    ask_buckets: Mutex<HashMap<String, TokenBucket>>,
}

impl LocalGatekeeper {
    pub fn new(caps: Capabilities, trust: TrustStore) -> Self {
        Self::with_clock(caps, trust, Arc::new(SystemClock))
    }

    fn with_clock(caps: Capabilities, trust: TrustStore, clock: Arc<dyn Clock>) -> Self {
        Self {
            caps,
            trust,
            clock,
            remember_buckets: Mutex::new(HashMap::new()),
            ask_buckets: Mutex::new(HashMap::new()),
        }
    }

    fn ensure_world_can_write(&self, world: &WorldKey) -> Result<()> {
        if !self.caps.write_worlds.is_empty() && !self.caps.write_worlds.contains(world) {
            return Err(SecurityError::Capability(format!(
                "write not allowed for world {}",
                world.0
            ))
            .into());
        }
        Ok(())
    }

    fn ensure_world_can_read(&self, world: &WorldKey) -> Result<()> {
        if !self.caps.read_worlds.is_empty() && !self.caps.read_worlds.contains(world) {
            return Err(SecurityError::Capability(format!(
                "read not allowed for world {}",
                world.0
            ))
            .into());
        }
        Ok(())
    }

    fn check_importance(&self, importance: Importance) -> Result<()> {
        if let Some(cap) = self.caps.importance_cap {
            if importance.get() > cap {
                return Err(SecurityError::Capability(format!(
                    "importance {} exceeds cap {}",
                    importance.get(),
                    cap
                ))
                .into());
            }
        }
        Ok(())
    }

    fn check_labels(&self, labels: &[String]) -> Result<()> {
        if self.caps.labels_write.is_empty() {
            return Ok(());
        }
        if labels.iter().all(|l| self.caps.labels_write.contains(l)) {
            Ok(())
        } else {
            Err(SecurityError::Capability("label not permitted".into()).into())
        }
    }

    fn check_kind(&self, kind: &AtomKind) -> Result<()> {
        if self.caps.kinds_write.is_empty() || self.caps.kinds_write.contains(kind) {
            Ok(())
        } else {
            Err(SecurityError::Capability(format!("kind {kind:?} not permitted")).into())
        }
    }

    fn rate_limit(
        bucket_map: &Mutex<HashMap<String, TokenBucket>>,
        key: &str,
        rate_per_sec: Option<f64>,
        clock: &Arc<dyn Clock>,
    ) -> Result<()> {
        if let Some(rate) = rate_per_sec {
            let now = clock.now();
            let mut buckets = bucket_map.lock().expect("bucket lock poisoned");
            let bucket = buckets
                .entry(key.to_string())
                .or_insert_with(|| TokenBucket::new(rate, now));
            if !bucket.allow(now) {
                return Err(
                    SecurityError::RateLimited(format!("rate limited for key {key}")).into(),
                );
            }
        }
        Ok(())
    }
}

impl Gatekeeper for LocalGatekeeper {
    fn check_remember(&self, new_atom: &NewAtom) -> Result<()> {
        self.ensure_world_can_write(&new_atom.world)?;
        self.check_kind(&new_atom.kind)?;
        self.check_labels(&new_atom.labels)?;
        self.check_importance(new_atom.importance)?;
        Self::rate_limit(
            &self.remember_buckets,
            &new_atom.worker.0,
            self.caps.rate_limits.remember_per_sec,
            &self.clock,
        )?;
        Ok(())
    }

    fn check_ask(&self, question: &Question) -> Result<()> {
        self.ensure_world_can_read(&question.world)?;
        Self::rate_limit(
            &self.ask_buckets,
            "ask-global",
            self.caps.rate_limits.ask_per_min.map(|v| v / 60.0),
            &self.clock,
        )?;
        Ok(())
    }

    fn check_world_action(&self, action: &WorldAction) -> Result<()> {
        match action {
            WorldAction::Create(meta) => self.ensure_world_can_write(&meta.world),
            WorldAction::Archive(world) | WorldAction::Resume(world) => {
                self.ensure_world_can_write(world)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dwbase_core::{Timestamp, WorkerKey};
    use std::time::Duration;

    struct MockClock {
        now: Mutex<Instant>,
    }

    impl MockClock {
        fn advance(&self, dur: Duration) {
            let mut guard = self.now.lock().unwrap();
            *guard += dur;
        }
    }

    impl Clock for MockClock {
        fn now(&self) -> Instant {
            *self.now.lock().unwrap()
        }
    }

    fn gatekeeper_with_clock(clock: Arc<dyn Clock>, caps: Capabilities) -> LocalGatekeeper {
        LocalGatekeeper::with_clock(caps, TrustStore::default(), clock)
    }

    fn sample_atom(world: &str, importance: f32, labels: &[&str], kind: AtomKind) -> NewAtom {
        let mut labels_vec = Vec::new();
        for l in labels {
            labels_vec.push((*l).to_string());
        }
        NewAtom {
            world: WorldKey::new(world),
            worker: WorkerKey::new("worker-1"),
            kind,
            timestamp: Timestamp::new("2024-01-01T00:00:00Z"),
            importance: Importance::new(importance).unwrap(),
            payload_json: "{}".into(),
            vector: None,
            flags: Vec::new(),
            labels: labels_vec,
            links: Vec::new(),
        }
    }

    #[test]
    fn capability_denied_on_world_and_kind_and_importance() {
        let caps = Capabilities {
            write_worlds: vec![WorldKey::new("allowed")],
            kinds_write: vec![AtomKind::Observation],
            importance_cap: Some(0.5),
            ..Default::default()
        };
        let gk = LocalGatekeeper::new(caps, TrustStore::default());
        let bad_world = sample_atom("blocked", 0.4, &[], AtomKind::Observation);
        assert!(gk.check_remember(&bad_world).is_err());
        let bad_kind = sample_atom("allowed", 0.4, &[], AtomKind::Action);
        assert!(gk.check_remember(&bad_kind).is_err());
        let bad_importance = sample_atom("allowed", 0.9, &[], AtomKind::Observation);
        assert!(gk.check_remember(&bad_importance).is_err());
    }

    #[test]
    fn rate_limiting_enforced_deterministically() {
        let clock = Arc::new(MockClock {
            now: Mutex::new(Instant::now()),
        });
        let caps = Capabilities {
            write_worlds: vec![WorldKey::new("w")],
            rate_limits: RateLimits {
                remember_per_sec: Some(1.0),
                ask_per_min: Some(60.0),
            },
            ..Default::default()
        };
        let gk = gatekeeper_with_clock(clock.clone(), caps);
        let atom = sample_atom("w", 0.1, &[], AtomKind::Observation);
        // First allowed
        assert!(gk.check_remember(&atom).is_ok());
        // Second within same second denied
        assert!(gk.check_remember(&atom).is_err());
        // Advance 1s, allowed again
        clock.advance(Duration::from_secs(1));
        assert!(gk.check_remember(&atom).is_ok());

        let question = Question {
            world: WorldKey::new("w"),
            text: "q".into(),
            filter: None,
        };
        assert!(gk.check_ask(&question).is_ok());
        // ask_per_min=60 => 1/sec tokens; next immediate call should fail
        assert!(gk.check_ask(&question).is_err());
        clock.advance(Duration::from_secs(1));
        assert!(gk.check_ask(&question).is_ok());
    }
}
