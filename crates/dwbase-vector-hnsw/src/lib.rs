//! HNSW-backed VectorEngine implementation for DWBase.
//!
//! Vectors are stored per world in an in-memory HNSW index. Persistence is out of scope for v1.
//! The index assumes a consistent dimension per world (inferred from the first insert).

use std::collections::HashMap;
use std::sync::RwLock;

use dwbase_core::{AtomId, AtomKind, WorldKey};
use dwbase_engine::{AtomFilter, DwbaseError, Result, VectorEngine};
use hnsw_rs::prelude::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VectorMetadata {
    pub kind: Option<AtomKind>,
    pub labels: Vec<String>,
    pub flags: Vec<String>,
}

#[derive(Debug, Error)]
enum VectorError {
    #[error("dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },
}

impl From<VectorError> for DwbaseError {
    fn from(err: VectorError) -> Self {
        DwbaseError::Vector(err.to_string())
    }
}

struct WorldIndex {
    hnsw: Hnsw<'static, f32, DistL2>,
    dim: usize,
    meta: HashMap<usize, (AtomId, VectorMetadata)>,
    next_point: usize,
}

impl WorldIndex {
    fn new(dim: usize) -> Self {
        // Parameters chosen for small in-memory indices; tweak as needed later.
        let max_nb_connection = 16;
        let max_elements = 10_000;
        let max_layers = 12;
        let ef_c = 100;
        let hnsw =
            Hnsw::<f32, DistL2>::new(max_nb_connection, max_elements, max_layers, ef_c, DistL2 {});
        Self {
            hnsw,
            dim,
            meta: HashMap::new(),
            next_point: 0,
        }
    }
}

/// HNSW-based vector search engine.
pub struct HnswVectorEngine {
    worlds: RwLock<HashMap<WorldKey, WorldIndex>>,
}

impl HnswVectorEngine {
    pub fn new() -> Self {
        Self {
            worlds: RwLock::new(HashMap::new()),
        }
    }

    fn metadata_matches(meta: &VectorMetadata, filter: &AtomFilter) -> bool {
        if !filter.kinds.is_empty() {
            if let Some(kind) = &meta.kind {
                if !filter.kinds.contains(kind) {
                    return false;
                }
            } else {
                return false;
            }
        }
        if !filter.labels.is_empty() && !filter.labels.iter().all(|l| meta.labels.contains(l)) {
            return false;
        }
        if !filter.flags.is_empty() && !filter.flags.iter().all(|f| meta.flags.contains(f)) {
            return false;
        }
        true
    }
}

impl Default for HnswVectorEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl VectorEngine for HnswVectorEngine {
    fn upsert(&self, world: &WorldKey, atom_id: &AtomId, vector: &[f32]) -> Result<()> {
        self.upsert_with_metadata(world, atom_id, vector, VectorMetadata::default())
    }

    fn search(
        &self,
        world: &WorldKey,
        query: &[f32],
        k: usize,
        filter: &AtomFilter,
    ) -> Result<Vec<AtomId>> {
        if let Some(filter_world) = &filter.world {
            if filter_world != world {
                return Ok(Vec::new());
            }
        }
        let guard = self.worlds.read().expect("poisoned world index lock");
        let world_idx = match guard.get(world) {
            Some(idx) => idx,
            None => return Ok(Vec::new()),
        };
        if world_idx.dim != query.len() {
            return Err(VectorError::DimensionMismatch {
                expected: world_idx.dim,
                got: query.len(),
            }
            .into());
        }

        let ef_search = 200;
        let results = world_idx.hnsw.search(query, k, ef_search);

        let mut out = Vec::new();
        for point in results {
            if let Some((atom_id, meta)) = world_idx.meta.get(&point.d_id) {
                if Self::metadata_matches(meta, filter) {
                    out.push(atom_id.clone());
                }
            }
        }
        // Fallback: if filtered results are empty, return the first matching items regardless of ANN score.
        if out.is_empty() {
            for (_pid, (atom_id, meta)) in world_idx.meta.iter() {
                if Self::metadata_matches(meta, filter) {
                    out.push(atom_id.clone());
                    if out.len() >= k {
                        break;
                    }
                }
            }
        }
        Ok(out)
    }

    fn rebuild(&self, _world: &WorldKey) -> Result<()> {
        Ok(())
    }
}

impl HnswVectorEngine {
    pub fn upsert_with_metadata(
        &self,
        world: &WorldKey,
        atom_id: &AtomId,
        vector: &[f32],
        metadata: VectorMetadata,
    ) -> Result<()> {
        let dim = vector.len();
        let mut guard = self.worlds.write().expect("poisoned world index lock");
        if let Some(existing) = guard.get(world) {
            if existing.dim != dim {
                return Err(VectorError::DimensionMismatch {
                    expected: existing.dim,
                    got: dim,
                }
                .into());
            }
        }
        let world_idx = guard
            .entry(world.clone())
            .or_insert_with(|| WorldIndex::new(dim));

        let point_id = world_idx.next_point;
        world_idx.next_point += 1;
        world_idx.hnsw.insert((vector, point_id));
        world_idx.meta.insert(point_id, (atom_id.clone(), metadata));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_engine() -> HnswVectorEngine {
        HnswVectorEngine::new()
    }

    #[test]
    fn insert_and_search_returns_nearest_ids() {
        let engine = test_engine();
        let world = WorldKey::new("w1");
        engine
            .upsert_with_metadata(
                &world,
                &AtomId::new("a1"),
                &[0.0, 0.0],
                VectorMetadata {
                    kind: Some(AtomKind::Observation),
                    labels: vec!["foo".into()],
                    flags: vec![],
                },
            )
            .unwrap();
        engine
            .upsert_with_metadata(
                &world,
                &AtomId::new("a2"),
                &[10.0, 10.0],
                VectorMetadata {
                    kind: Some(AtomKind::Observation),
                    labels: vec!["bar".into()],
                    flags: vec![],
                },
            )
            .unwrap();

        let filter = AtomFilter::default();
        let hits = engine.search(&world, &[0.1, 0.1], 1, &filter).unwrap();
        assert_eq!(hits, vec![AtomId::new("a1")]);
    }

    #[test]
    fn filter_by_labels_and_kinds() {
        let engine = test_engine();
        let world = WorldKey::new("w1");
        engine
            .upsert_with_metadata(
                &world,
                &AtomId::new("a1"),
                &[1.0, 1.0],
                VectorMetadata {
                    kind: Some(AtomKind::Observation),
                    labels: vec!["x".into()],
                    flags: vec![],
                },
            )
            .unwrap();
        engine
            .upsert_with_metadata(
                &world,
                &AtomId::new("a2"),
                &[1.1, 1.1],
                VectorMetadata {
                    kind: Some(AtomKind::Reflection),
                    labels: vec!["y".into()],
                    flags: vec!["skip".into()],
                },
            )
            .unwrap();

        let filter = AtomFilter {
            world: Some(world.clone()),
            kinds: vec![AtomKind::Reflection],
            labels: vec!["y".into()],
            flags: vec!["skip".into()],
            since: None,
            until: None,
            limit: Some(5),
        };
        let hits = engine.search(&world, &[1.0, 1.0], 2, &filter).unwrap();
        assert_eq!(hits, vec![AtomId::new("a2")]);
    }
}
