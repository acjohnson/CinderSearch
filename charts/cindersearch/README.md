# CinderSearch Helm Chart

Production Kubernetes Helm chart for **CinderSearch** — an S3-first, NVMe-cached, drop-in Elasticsearch / OpenSearch-compatible search engine written in pure Go.

## Supported Topologies & Multi-Master Architecture

CinderSearch uses a **leaderless multi-master consistent hash ring** across `data` nodes (no dedicated `master`-only quorum or single-point-of-failure master election; index metadata syncs peer-to-peer and shard state is persisted authoritatively to S3):

| Mode | Example Values File | Pods Deployed | Best For |
| --- | --- | --- | --- |
| **Single-Node Dev** | `examples/values-single-dev.yaml` | 1 `data` (embedded S3) | Local `kind` / `minikube` / `k3d` evaluation |
| **Multi-Node Multi-Master + MinIO** | `examples/values-multimaster-minio.yaml` | 3 `data` + 4 distributed MinIO | Self-contained on-prem / private K8s HA cluster |
| **Disaggregated + AWS S3** | `examples/values-disaggregated-aws-s3.yaml` | 3 `data` + 2 `coordinator` + 3 `reader` | Cloud production with independent ingest & search scaling |

### Node Role Pools

- **`data` (StatefulSet + NVMe PVC)**: Leaderless multi-master shard owners. Hold local NVMe WALs, replicate asynchronously to replica peers, and flush immutable segments to S3.
- **`coordinator` (Optional StatefulSet, zero WAL)**: Stateless HTTP gateway handling Elasticsearch REST requests, Query DSL scatter-gather, and global aggregation/sort merges. When `coordinator.enabled=true`, the main `<release>-cindersearch` Service automatically routes client traffic to coordinators.
- **`reader` (Optional StatefulSet, zero WAL)**: Stateless S3 + NVMe read-cache search nodes for horizontally scaling read QPS via `?preference=reader` (or `X-Cinder-Preference: reader`).

## S3 Storage Backends (`s3.mode`)

1. **`minio` (default)**: Deploys an open-source MinIO S3-compatible server inside Kubernetes (`s3.minio.mode=standalone` for 1 pod or `distributed` for 4+ erasure-coded pods) plus a `post-install,post-upgrade` hook Job that automatically creates `s3.bucket`.
2. **`external`**: Points directly to AWS S3 (`https://s3.<region>.amazonaws.com` when `s3.external.endpoint` is empty), Cloudflare R2, Ceph RGW, or an external MinIO cluster using `s3.external.existingSecret` (recommended) or inline credentials.
3. **`embedded`**: Uses CinderSearch's built-in ephemeral in-memory S3 mock (strictly validated for single-node dev only).

## Quickstart

### 1. Build & Push Container Image (from repo root)

```bash
docker build -t ghcr.io/cindersearch/cindersearch:8.11.0 .
```

### 2. Install on Kubernetes

```bash
# A) Single-node local dev (embedded S3)
helm upgrade --install cindersearch ./charts/cindersearch \
  -n cindersearch --create-namespace \
  -f ./charts/cindersearch/examples/values-single-dev.yaml

# B) 3-node multi-master cluster + 4-node distributed MinIO S3
helm upgrade --install cindersearch ./charts/cindersearch \
  -n cindersearch --create-namespace \
  -f ./charts/cindersearch/examples/values-multimaster-minio.yaml

# C) Disaggregated (data + coordinator + reader) backed by real AWS S3
kubectl create namespace cindersearch
kubectl -n cindersearch create secret generic cindersearch-aws-creds \
  --from-literal=AWS_ACCESS_KEY_ID="AKIA..." \
  --from-literal=AWS_SECRET_ACCESS_KEY="..."

helm upgrade --install cindersearch ./charts/cindersearch \
  -n cindersearch \
  -f ./charts/cindersearch/examples/values-disaggregated-aws-s3.yaml \
  --set s3.bucket=my-prod-cindersearch-bucket \
  --set s3.region=us-east-1
```

### 3. Verify Deployment

```bash
helm test cindersearch -n cindersearch
kubectl -n cindersearch port-forward svc/cindersearch 9200:9200
curl http://127.0.0.1:9200/_cluster/health
curl http://127.0.0.1:9200/_cat/nodes
```

## Validation & Linting

```bash
helm lint ./charts/cindersearch
helm template cindersearch ./charts/cindersearch -f ./charts/cindersearch/examples/values-single-dev.yaml >/dev/null
helm template cindersearch ./charts/cindersearch -f ./charts/cindersearch/examples/values-multimaster-minio.yaml >/dev/null
helm template cindersearch ./charts/cindersearch -f ./charts/cindersearch/examples/values-disaggregated-aws-s3.yaml >/dev/null
```

## Key Configuration Parameters

| Key | Default | Description |
| --- | --- | --- |
| `cluster.topology` | `multi-node` | Topology mode (`single`, `multi-node`, `disaggregated`) |
| `s3.mode` | `minio` | S3 backend (`minio`, `external`, `embedded`) |
| `s3.bucket` | `cindersearch` | S3 bucket name for immutable segments & manifests |
| `s3.region` | `us-east-1` | AWS / S3 signing region |
| `s3.minio.mode` | `standalone` | MinIO deployment mode (`standalone` or `distributed` with `>= 4` pods) |
| `s3.external.endpoint` | `""` | Custom S3 endpoint (empty resolves to `https://s3.<region>.amazonaws.com`) |
| `s3.external.existingSecret` | `""` | Existing K8s Secret with `AWS_ACCESS_KEY_ID` & `AWS_SECRET_ACCESS_KEY` |
| `data.replicaCount` | `3` | Number of multi-master `data` StatefulSet pods |
| `data.nvmeMaxMB` | `1024` | Bounded local NVMe LRU read cache budget per data pod (MB) |
| `data.persistence.storageClass` | `""` | StorageClass for NVMe WAL + cache PVCs |
| `coordinator.enabled` | `false` | Deploy stateless `coordinator` gateway pool |
| `coordinator.replicaCount` | `2` | Number of `coordinator` pods when enabled |
| `reader.enabled` | `false` | Deploy stateless S3+NVMe `reader` search pool |
| `reader.replicaCount` | `2` | Number of `reader` pods when enabled |
| `engine.flushSec` | `5` | Background WAL-to-S3 segment flush interval (seconds) |
| `engine.pprof.enabled` | `false` | Enable Go pprof debug listener on port `6060` |
| `ingress.enabled` | `false` | Create `networking.k8s.io/v1` Ingress for client API |
| `networkPolicy.enabled` | `false` | Create Kubernetes NetworkPolicy restricting pod traffic |
