use std::sync::Arc;

use dwbase_core::{AtomId, AtomKind, Importance, Timestamp, WorkerKey, WorldKey};
use dwbase_embedder_dummy::DummyEmbedder;
use dwbase_engine::{AtomFilter, DWBaseEngine, StreamEngine};
use dwbase_security::{Capabilities, LocalGatekeeper, TrustStore};
use dwbase_storage_sled::{DummyKeyProvider, SledConfig, SledStorage};
use dwbase_stream_local::LocalStreamEngine;
use dwbase_vector_hnsw::HnswVectorEngine;
use tempfile::TempDir;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let tmp = TempDir::new()?;
    let world = WorldKey::new("stream-demo");

    let storage = SledStorage::open(
        SledConfig::new(tmp.path()),
        Arc::new(DummyKeyProvider::default()),
    )?;
    let vector = HnswVectorEngine::new();
    let stream = LocalStreamEngine::new();
    let gatekeeper = LocalGatekeeper::new(Capabilities::default(), TrustStore::default());
    let engine = Arc::new(DWBaseEngine::new(
        storage,
        vector,
        stream,
        gatekeeper,
        DummyEmbedder::new(),
    ));

    let handle = engine
        .stream
        .subscribe(&world, AtomFilter::default())
        .expect("subscribe");

    // Producer
    let producer = {
        let engine = engine.clone();
        let world = world.clone();
        tokio::spawn(async move {
            for i in 0..3 {
                let atom = dwbase_core::Atom::builder(
                    AtomId::new(format!("obs-{i}")),
                    world.clone(),
                    WorkerKey::new("worker-1"),
                    AtomKind::Observation,
                    Timestamp::new(""),
                    Importance::clamped(0.5),
                    format!(r#"{{"event":"obs-{i}"}}"#),
                )
                .build();
                engine.observe(atom).await.unwrap();
            }
        })
    };

    // Consumer
    let consumer = {
        let engine = engine.clone();
        tokio::spawn(async move {
            loop {
                match engine.stream.poll(&handle).unwrap() {
                    Some(atom) => println!("received {}", atom.payload_json()),
                    None => {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                }
                if engine.stream.poll(&handle).unwrap().is_none() {
                    break;
                }
            }
        })
    };

    let _ = tokio::join!(producer, consumer);
    Ok(())
}
