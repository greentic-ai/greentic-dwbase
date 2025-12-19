# Production Deployment Bundle (PR-30)

This guide covers building the container image, deploying to Kubernetes, and rolling upgrades for dwbase-node.

## Container build
```bash
docker build -t dwbase:latest .
```
- Multi-stage Rust build, Debian-slim runtime.
- Runs as uid/gid 10001 (non-root), rootfs read-only, writes under `/data`.
- Entry: `dwbase-node --config /config/config.toml`.

Suggested publishing:
```bash
docker tag dwbase:latest <registry>/dwbase:<tag>
docker push <registry>/dwbase:<tag>
```

## Kubernetes manifests
Directory: `deploy/k8s/`
- `configmap.yaml` — base `config.toml` mounted at `/config/config.toml`.
- `statefulset.yaml` — 3 replicas, PVC per pod for `/data`, readiness/liveness on `/readyz` and `/healthz`, non-root, read-only rootfs, resource requests/limits.
- `service.yaml` — primary Service on port 80 → 8080; metrics Service on port 9090 scraping `/metrics`.

Apply:
```bash
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/service.yaml
kubectl apply -f deploy/k8s/statefulset.yaml
```

Secret hooks:
- Set `DWBASE_KEY_<KEY_ID>` via a Secret + env if storage encryption is enabled.
- Mount TLS certs separately if fronting with an ingress/sidecar.

## Rolling upgrades
- Compatibility: storage format is append-only with checksum/repair; upgrading within the same minor release keeps data compatible. Roll out one replica at a time.
- Draining a pod:
  1) `kubectl rollout pause statefulset/dwbase` (optional).
  2) `kubectl delete pod dwbase-0` (or scale replicas down/up) to force restart; readiness probe (`/readyz`) keeps the pod out of service until storage/index are ready.
- Safe rollout:
```bash
kubectl rollout restart statefulset/dwbase
kubectl rollout status statefulset/dwbase
```
- If you need manual gating, set the pod not-ready before killing: `kubectl patch pod dwbase-0 -p '{"status":{"conditions":[{"type":"Ready","status":"False"}]}}'` then delete it.

## Security posture
- Non-root user (uid 10001), read-only rootfs, writable `/data`.
- `allowPrivilegeEscalation: false`, `runAsNonRoot: true`, `fsGroup: 10001`.
- Example resource requests/limits in StatefulSet; tune for your workload.

## Observability
- Metrics: scrape `http://<pod>:8080/metrics` (exposed via `dwbase-metrics` service on port 9090).
- Health: `/healthz` (liveness), `/readyz` (readiness: storage/disk/index).
- Correlation IDs: `x-request-id` set per request for tracing.
