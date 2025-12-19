//! Metrics and tracing facade for DWBase.
//!
//! The helpers here emit both metrics (via the `metrics` crate) and lightweight tracing events.

use std::time::Duration;

use metrics::{counter, gauge, histogram};
use tracing::trace;

/// Record latency for `remember` requests in milliseconds.
pub fn record_remember_latency(latency: Duration) {
    let ms = latency.as_secs_f64() * 1_000.0;
    histogram!("dwbase.remember.latency_ms").record(ms);
    trace!(latency_ms = ms, "remember latency observed");
}

/// Record latency for `ask` requests in milliseconds.
pub fn record_ask_latency(latency: Duration) {
    let ms = latency.as_secs_f64() * 1_000.0;
    histogram!("dwbase.ask.latency_ms").record(ms);
    trace!(latency_ms = ms, "ask latency observed");
}

/// Track how stale the reflex/vector index is (age of newest atom seen).
pub fn record_index_freshness(age: Duration) {
    let ms = age.as_secs_f64() * 1_000.0;
    gauge!("dwbase.index.freshness_ms").set(ms);
    trace!(freshness_ms = ms, "index freshness recorded");
}

/// Count GC/eviction actions (placeholder for future GC loops).
pub fn record_gc_evictions(evicted: u64) {
    if evicted == 0 {
        return;
    }
    counter!("dwbase.gc.evictions").increment(evicted);
    trace!(evicted, "gc evictions recorded");
}

/// Snapshot trust scores across entities (worker/node).
pub fn record_trust_distribution<'a, I>(entries: I)
where
    I: IntoIterator<Item = (&'a str, f32)>,
{
    let mut count = 0u64;
    for (entity, score) in entries {
        count += 1;
        let labels = [("entity", entity.to_string())];
        gauge!("dwbase.trust.score", &labels).set(score as f64);
    }
    trace!(entries = count, "trust distribution snapshot recorded");
}

/// Track sync lag (swarm/replication) in milliseconds; set to 0 when idle.
pub fn record_sync_lag(lag: Duration) {
    let ms = lag.as_secs_f64() * 1_000.0;
    gauge!("dwbase.sync.lag_ms").set(ms);
    trace!(lag_ms = ms, "sync lag recorded");
}

/// Track quarantine counts (e.g., excluded peers/messages); set to 0 when unused.
pub fn record_quarantine_count(count: u64) {
    gauge!("dwbase.quarantine.count").set(count as f64);
    trace!(count, "quarantine count recorded");
}

/// Track disk usage for the storage path.
pub fn record_disk_usage(used_bytes: u64, total_bytes: u64) {
    if total_bytes == 0 {
        return;
    }
    let used = used_bytes as f64;
    let total = total_bytes as f64;
    let pct = (used / total) * 100.0;
    gauge!("dwbase.disk.used_bytes").set(used);
    gauge!("dwbase.disk.total_bytes").set(total);
    gauge!("dwbase.disk.used_percent").set(pct);
    trace!(
        used_bytes,
        total_bytes,
        used_percent = pct,
        "disk usage recorded"
    );
}

/// Count observe drops when streams apply backpressure and drop events.
pub fn record_observe_dropped(dropped: u64) {
    if dropped == 0 {
        return;
    }
    counter!("dwbase.observe.dropped_total").increment(dropped);
    trace!(dropped, "observe events dropped");
}

/// Track current observe queue depth (best-effort gauge).
pub fn record_observe_queue_depth(depth: u64) {
    gauge!("dwbase.observe.queue_depth").set(depth as f64);
    trace!(depth, "observe queue depth recorded");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emits_without_recorder() {
        record_remember_latency(Duration::from_millis(5));
        record_ask_latency(Duration::from_millis(7));
        record_index_freshness(Duration::from_millis(2));
        record_gc_evictions(0);
        record_gc_evictions(3);
        record_trust_distribution([("worker-a", 0.9), ("node-1", 0.5)]);
        record_observe_dropped(2);
        record_observe_queue_depth(7);
    }
}
