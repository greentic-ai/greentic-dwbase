//! NATS-backed presence for DWBase components (hello/presence).
//! This module provides a trait-based client with an in-memory mock bus suitable for tests or local runs.

use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "nats")]
use futures::StreamExt;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

pub mod replication;
pub mod swarm;
pub mod world_events;

type HelloHandler = dyn Fn(NodeHello) + Send + Sync;

/// Presence announcement payload.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NodeHello {
    pub node_id: String,
    pub endpoint: String,
    pub worlds_served: Vec<String>,
    pub trust_score: f32,
    pub started_at: String,
    pub version: String,
}

/// Peer record with expiry.
#[derive(Clone, Debug)]
pub struct PeerInfo {
    pub hello: NodeHello,
    pub last_seen: OffsetDateTime,
    pub ttl: Duration,
}

/// Simple peer table with TTL expiry.
#[derive(Default, Clone)]
pub struct PeerTable {
    inner: Arc<RwLock<Vec<PeerInfo>>>,
}

impl PeerTable {
    pub fn upsert(&self, hello: NodeHello, ttl: Duration) {
        let mut guard = self.inner.write();
        let now = OffsetDateTime::now_utc();
        if let Some(existing) = guard.iter_mut().find(|p| p.hello.node_id == hello.node_id) {
            existing.hello = hello;
            existing.last_seen = now;
            existing.ttl = ttl;
        } else {
            guard.push(PeerInfo {
                hello,
                last_seen: now,
                ttl,
            });
        }
    }

    pub fn peers(&self) -> Vec<NodeHello> {
        self.prune();
        self.inner.read().iter().map(|p| p.hello.clone()).collect()
    }

    fn prune(&self) {
        let now = OffsetDateTime::now_utc();
        let mut guard = self.inner.write();
        guard.retain(|p| now - p.last_seen <= p.ttl);
    }
}

/// Client trait so real or mock NATS can be plugged in.
pub trait NatsClient: Send + Sync {
    fn publish_hello(&self, hello: &NodeHello) -> anyhow::Result<()>;
    fn subscribe_hello(&self, handler: Box<HelloHandler>) -> anyhow::Result<()>;
}

/// In-memory broadcast bus for tests/local dev (not real NATS).
#[derive(Default, Clone)]
pub struct MockNats {
    subs: Arc<RwLock<Vec<Arc<HelloHandler>>>>,
}

impl NatsClient for MockNats {
    fn publish_hello(&self, hello: &NodeHello) -> anyhow::Result<()> {
        for sub in self.subs.read().iter() {
            sub(hello.clone());
        }
        Ok(())
    }

    fn subscribe_hello(&self, handler: Box<HelloHandler>) -> anyhow::Result<()> {
        self.subs.write().push(handler.into());
        Ok(())
    }
}

/// Async NATS client adapter.
#[cfg(feature = "nats")]
pub struct AsyncNats {
    client: async_nats::Client,
    runtime: tokio::runtime::Runtime,
}

#[cfg(feature = "nats")]
impl AsyncNats {
    pub fn connect(url: &str) -> anyhow::Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let client = rt.block_on(async { async_nats::connect(url).await })?;
        Ok(Self {
            client,
            runtime: rt,
        })
    }
}

#[cfg(feature = "nats")]
impl NatsClient for AsyncNats {
    fn publish_hello(&self, hello: &NodeHello) -> anyhow::Result<()> {
        let payload = serde_json::to_vec(hello)?;
        self.runtime.block_on(async {
            self.client
                .publish("dwbase.node.hello", payload.into())
                .await
        })?;
        Ok(())
    }

    fn subscribe_hello(&self, handler: Box<HelloHandler>) -> anyhow::Result<()> {
        let client = self.client.clone();
        let handler = Arc::new(handler);
        self.runtime.spawn(async move {
            if let Ok(mut sub) = client.subscribe("dwbase.node.hello").await {
                while let Some(msg) = sub.next().await {
                    if let Ok(hello) = serde_json::from_slice::<NodeHello>(&msg.payload) {
                        handler(hello);
                    }
                }
            }
        });
        Ok(())
    }
}

/// Periodically publish hello and feed the peer table.
pub fn start_presence_loop(
    client: Arc<dyn NatsClient>,
    hello: NodeHello,
    table: PeerTable,
    ttl: Duration,
) -> std::thread::JoinHandle<()> {
    let send_interval = ttl / 2;
    // Feed local table immediately
    table.upsert(hello.clone(), ttl);
    let c = client.clone();
    let h = hello.clone();
    std::thread::spawn(move || loop {
        let _ = c.publish_hello(&h);
        std::thread::sleep(send_interval);
    });

    let handler_table = table.clone();
    let _ = client.subscribe_hello(Box::new(move |msg| {
        handler_table.upsert(msg, ttl);
    }));

    // Return dummy handle for caller to hold; the publishing loop is separate.
    std::thread::spawn(|| {})
}

pub fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn peer_table_expires() {
        let table = PeerTable::default();
        table.upsert(
            NodeHello {
                node_id: "a".into(),
                endpoint: "http://a".into(),
                worlds_served: vec!["w1".into()],
                trust_score: 1.0,
                started_at: now_rfc3339(),
                version: "v1".into(),
            },
            Duration::from_millis(10),
        );
        assert_eq!(table.peers().len(), 1);
        std::thread::sleep(Duration::from_millis(30));
        assert_eq!(table.peers().len(), 0);
    }

    #[test]
    fn mock_bus_propagates_presence() {
        let client = Arc::new(MockNats::default());
        let table_a = PeerTable::default();
        let table_b = PeerTable::default();
        let hello_a = NodeHello {
            node_id: "a".into(),
            endpoint: "http://a".into(),
            worlds_served: vec!["w1".into()],
            trust_score: 0.9,
            started_at: now_rfc3339(),
            version: "v1".into(),
        };
        let hello_b = NodeHello {
            node_id: "b".into(),
            endpoint: "http://b".into(),
            worlds_served: vec!["w2".into()],
            trust_score: 0.8,
            started_at: now_rfc3339(),
            version: "v1".into(),
        };
        let ttl = Duration::from_millis(100);
        start_presence_loop(client.clone(), hello_a.clone(), table_a.clone(), ttl);
        start_presence_loop(client.clone(), hello_b.clone(), table_b.clone(), ttl);
        std::thread::sleep(Duration::from_millis(100));
        let peers_a = table_a.peers();
        let peers_b = table_b.peers();
        assert!(peers_a.iter().any(|p| p.node_id == "b"));
        assert!(peers_b.iter().any(|p| p.node_id == "a"));
    }
}
