//! Dummy embedder implementation for DWBase; returns no embedding and is intended for local
//! development when vector search is optional.

use std::{future::Future, pin::Pin};

use dwbase_engine::{Embedder, Result};

/// No-op embedder that always yields `None`.
#[derive(Clone, Debug, Default)]
pub struct DummyEmbedder;

impl DummyEmbedder {
    pub fn new() -> Self {
        Self
    }
}

impl Embedder for DummyEmbedder {
    #[allow(clippy::type_complexity)]
    fn embed<'a>(
        &'a self,
        _payload_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<f32>>>> + Send + 'a>> {
        Box::pin(std::future::ready(Ok(None)))
    }

    fn model_version(&self) -> &'static str {
        "dummy-v1"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn returns_none() {
        let embedder = DummyEmbedder::new();
        let res = embedder.embed(r#"{"foo":"bar"}"#).await;
        assert!(res.unwrap().is_none());
    }
}
