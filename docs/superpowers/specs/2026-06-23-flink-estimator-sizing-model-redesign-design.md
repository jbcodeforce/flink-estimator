# Flink Estimator — Sizing Model Redesign

**Date:** 2026-06-23
**Status:** Approved for implementation
**Files:** `src/flink_estimator/estimation.py`, `src/flink_estimator/models.py`, tests

## Problem

For a representative input the estimator reports internally contradictory results:

```json
{ "messages_per_second": 150000, "avg_record_size_bytes": 2000,
  "simple_statements": 1, "medium_statements": 2, "complex_statements": 3,
  "num_distinct_keys": 50000000, "expected_latency_seconds": 60,
  "worker_node_type": "VM", "worker_node_t_size": "M" }
```

→ `total_cpus: 1168` but `total_worker_node_needed: 20` (20 × 16 cores = 320 cores of capacity). The CPU figure and the node count cannot both be true.

## Root cause

Three distinct concepts are pinned to the single constant `TM_vCPUs = 4`, and CPU/memory are sized in two independent passes that never reconcile.

### 1. Per-statement core cap silently ceilings throughput CPU (`estimation.py:635-641`)

```python
simple_statement_cpu_needs = min(TM_vCPUs, total_throughput / rate) * simple_statements
```

`min(TM_vCPUs, …)` caps each statement type at 4 cores regardless of volume. Effect: throughput CPU can never exceed `4 × total_statements`, independent of MB/s.

Proof (286.1 MB/s, this workload):

| statement | per-core rate | cores to keep up | capped | uncapped |
|---|---|---|---|---|
| simple ×1 | 45.78 MB/s | 6.25 | 4.00 | 6.25 |
| medium ×2 | 20.98 MB/s | 13.64 | 8.00 | 27.27 |
| complex ×3 | 4.77 MB/s | 60.00 | 12.00 | 180.00 |
| **total** | | | **24** | **213.52** |

The cap models nothing real — Flink parallelizes a statement across many TMs; the true ceiling is distinct-key count / `maxParallelism`, not cores-per-TM. It only fires when throughput is non-trivial, i.e. exactly when it does harm.

### 2. State sized as RAM, then grossed up (`estimation.py:141-160`)

```python
total_managed_memory_mb = keys × (medium+complex) × apps × record_bytes / 1MB   # ≈ 466 GB
return total_managed_memory_mb / pct                                            # ÷0.4 ⇒ 1.19 TB
```

Two errors:
- The ~466 GB is **total state size**. Under the recommended `rocksdb` backend (`docs-cp-flink/jobs/configure/checkpointing.rst:61-63`) state lives on **local disk**, not RAM. RocksDB's in-memory footprint (block cache + write buffers) is bounded by Flink managed memory (`state.backend.rocksdb.memory.managed=true`, default) and **does not scale with state size**.
- `÷0.4` misuses `taskmanager.memory.managed.fraction` (the RAM cap for RocksDB, default 0.4) as a state→RAM multiplier. It inflates a disk number 2.5× into phantom RAM.

Result: 1.19 TB RAM ÷ 4 GB/TM = 292 TMs.

### 3. CPU and node count never reconcile (`estimation.py:376-383`)

```python
total_cpu_needs = max(total_cpu_need_for_throughput, nb_task_managers * TM_vCPUs)  # max(26, 292×4)=1168
... node count = memory-only bin-pack of 292 × 4 GB TMs = 20
```

`total_cpus` = memory-driven TM count × 4 cores/TM. The node packer considers only RAM and never sees 1168. The throughput node loop (`:652`) only sees the capped 26. Nothing reconciles the three.

## Corrected model

Three independent requirements derive from workload telemetry; the primary buy unit is the **CP Flink node = 8 cores**. CPU is fractional and freely allocatable to TaskManagers; whole VM nodes bound only RAM and disk.

```
tm_cores       = Σ(throughput / rate × stmts) × latency_factor          # fractional
jm_cores       = jm_cpu × number_flink_applications                     # JM runs CP Flink too
total_cores    = tm_cores + jm_cores                                    # all cores running CP Flink
CP_FLINK_NODES = ⌈ total_cores / 8 ⌉                                     # PRIMARY OUTPUT

state_size     = num_distinct_keys × (medium+complex) × record_size × apps
required_disk  = state_size × 1.5                                        # RocksDB compaction/WAL headroom
ram_total      = nb_taskmanagers × MEM_PER_TM (+ network/buffer headroom)
worker_nodes   = ⌈ max( ram_total / ram_per_node, required_disk / disk_per_node ) ⌉   # SECONDARY
```

- **CPU** is throughput-only **plus JM cores** — both TaskManagers and JobManagers run CP Flink workloads, so both count toward `total_cores` and the CP Flink node total. State does **not** add cores.
- **State → local NVMe disk** (rocksdb assumed; hashmap deferred). RAM is the small working-set/buffer figure, not grossed-up state.
- **CP Flink node count is VM-shape-independent**; VM/worker node count is shape-dependent packing only.
- **`provisioned_cores` = worker_nodes × cores_per_node** is reported alongside, exposing the gap between
  compute demand (`total_cpus`) and the cores physically dragged in when a RAM/disk-light shape forces
  extra nodes (e.g. an S fleet provisions 576 cores for a 216-core workload). It is informational and
  does not change the shape-independent `cp_flink_nodes`.

### TM shape

Default per module docstring (`estimation.py:43-44`): `CORES_PER_TM = 1`, `MEM_PER_TM = 4096 MB`. Both **configurable** (inputs / constants). `nb_taskmanagers = ⌈tm_cores / CORES_PER_TM⌉`.

**Packing-efficiency constraint:** `MEM_PER_TM` should be an **even divisor of per-node usable RAM** so TMs don't strand memory. E.g. 4 GB TMs on a 16 GB node → 4 TMs, 0 waste; 6 GB TMs → 2 TMs, 4 GB stranded. The default 4 GB divides S (16 GB) and M (64 GB) cleanly; L (94 GB) does not — implementation must either snap `MEM_PER_TM` to a divisor of usable node RAM or report stranded RAM in diagnostics.

### Disk defaults

Local NVMe SSD added to VM t-shirts; `worker_node_disk_gb` override for custom/bare-metal.

| T-shirt | Cores | RAM | NVMe SSD |
|---|---|---|---|
| S | 8 | 16 GB | 512 GB |
| M | 16 | 64 GB | 2048 GB |
| L | 48 | 94 GB | 6144 GB |

## Changes

| # | File / location | Change |
|---|---|---|
| 1 | `estimation.py:635-641` | **Done.** Remove `min(TM_vCPUs, …)` cap; throughput CPU = `Σ(throughput/rate × stmts)`. |
| 2 | `estimation.py` + `models.py` | Add `CP_FLINK_NODE_CORES = 8`; new `ResourceEstimates.cp_flink_nodes = ⌈total_cores/8⌉` as primary output; add derived `provisioned_cores = worker_nodes × cores_per_node`. |
| 3 | `estimation.py:141-160` | Replace `_state_flink_process_memory_mb` (RAM gross-up) with `_state_disk_gb` = `state_size × 1.5`; drop the `÷pct` path for state. |
| 4 | `models.py:12-17` | Extend `VM_TSHIRT_MB_CPU` with disk GB (512/2048/6144); add `worker_node_disk_gb` field. |
| 5 | `estimation.py` | RAM per TM = baseline + network/buffer heuristic only (no state). `nb_taskmanagers = ⌈total_cores/CORES_PER_TM⌉`. |
| 6 | `estimation.py:206-246` | Bin-pack worker nodes on `max(RAM, disk)`; demote node count to secondary metric. |
| 7 | `estimation.py:376` | `total_cpus = total_cores` (throughput); remove `max(…, nb_tm × TM_vCPUs)`. |
| 8 | `estimation.py:119` | Remove dead `TM_JVM_OVERHEAD_MB`. |
| 9 | `estimation.py:285-337` | Rework `SizingDiagnostics`: bounding factors over {cpu, ram, disk}; drop circular `tm_slots` comparison. |
| 10 | `estimation.py:39-55` | Update docstring: CP Flink node = 8 cores; state on disk; remove "1168/20"-era heuristics. |
| 11 | tests | Update 5 cap-era assertions; add disk-dimension and CP-flink-node coverage. |

## Proof: before → after (representative input)

| metric | before | after |
|---|---|---|
| throughput CPU | 24 (capped) | 213.5 |
| `total_cpus` (incl. JM) | 1168 | ~216 (213.5 + 2 JM) |
| **CP Flink nodes** | — | **27** (`⌈216/8⌉`) |
| state treated as | 1.19 TB RAM | 466 GB disk → 699 GB w/ headroom |
| disk nodes (M=2 TB) | — | 1 |
| VM/worker nodes | 20 (RAM-only, inconsistent) | `max(RAM, disk)` pack, consistent |

The 1168-vs-20 contradiction dissolves: CPU (→ CP Flink nodes) and VM-node packing (RAM/disk) are answers to two different questions, no longer conflated through `TM_vCPUs`.

## Out of scope

- `hashmap` backend (state→heap RAM) — assume `rocksdb`; revisit later.
- Key/skew-based parallelism ceiling (replacement for the removed cap) — not modeled; noted as the correct future bound.
- Disk **throughput/IOPS** sizing — only capacity is modeled.
