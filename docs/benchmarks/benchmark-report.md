# Benchmark Report

## Goal

Measure Helios at production-shaped workflow volumes instead of the original tiny smoke-style run. The benchmark now supports 100, 500, 1000, or larger workflow batches and records scheduler, worker, retry, recovery, and correctness signals for each run.

The workload uses the production-style fraud/risk workflow from [workflow.json](../../examples/workflow.json), which includes validation, feature enrichment, fraud scoring, aggregation, deterministic embedding generation, artifact persistence, and webhook notification. The scoring stage intentionally exercises retry behavior.

## How To Run

Run the default benchmark matrix:

```bash
make benchmark
```

By default this executes:

- 100 workflows
- 500 workflows
- 1000 workflows

Run a custom matrix:

```bash
BENCHMARK_COUNTS="100 500 1000 2500" make benchmark
```

Run a quick local sanity benchmark:

```bash
make benchmark-quick
```

For large runs, raise the API submission limiter before starting the control plane:

```bash
HELIOS_SUBMISSION_RATE_PER_MINUTE=5000 make run-control-plane
```

Raw JSON results are written to [results](results) as `benchmark-<run-id>-<count>w.json`.

## Metrics Captured

Each result file includes:

| Metric | Meaning |
| --- | --- |
| `workflow_latency_ms.p50/p95/p99` | Time from benchmark submission to observed terminal workflow state |
| `scheduler_lease_latency_ms.p50/p95/p99` | Time from a task entering `ready` to the scheduler leasing it |
| `task_throughput_tasks_per_sec` | Terminal task events processed per second |
| `attempt_throughput_attempts_per_sec` | Task attempts processed per second, including retries |
| `recovery_time_ms.p50/p95/p99` | Time from recovery timeout detection to recovery policy application |
| `stale_result_count` | Observed stale or late-result signals in workflow event history |
| `duplicate_assignment_count` | Duplicate scheduler lease events for the same attempt ID |
| `worker_utilization.utilization_ratio` | Worker running-task utilization sampled during polling |
| `recovery_event_count` | Number of lease/worker-loss recovery events observed |
| `scheduler_lease_event_count` | Number of scheduler lease events observed |

## Workload

Each workflow contains 6 DAG tasks:

- `validate_payload`
- `transform_records`
- `model_inference`
- `aggregate_metrics`
- `write_artifact`
- `notify_webhook`

The `model_inference` task intentionally fails on its first attempt with a retryable error. This means the benchmark exercises retry/backoff and attempt-aware result handling, not just happy-path task execution.

## Interpreting Results

Healthy large-run results should show:

- All submitted workflows reach a terminal state before timeout.
- `succeeded_workflows` equals `submitted_workflows` for the default workflow.
- `duplicate_assignment_count` remains `0`.
- `stale_result_count` remains `0`.
- `scheduler_lease_latency_ms.p95` stays close to the scheduler tick plus worker availability delay.
- `worker_utilization.utilization_ratio.p95` rises under load instead of staying near zero.
- `recovery_event_count` is normally `0` for the default benchmark unless workers are killed or leases expire during the run.

## Production Signals

The benchmark validates:

- API submission under sustained workflow volume
- DAG persistence and task readiness transitions
- scheduler leasing behavior under load
- NATS assignment dispatch
- worker execution throughput and utilization
- retry/backoff correctness
- attempt-aware task completion
- duplicate assignment detection
- stale-result signal detection
- recovery timing when timeout or worker-loss recovery occurs
- reproducible JSON output for comparing commits and environments

## Notes

The benchmark is intentionally API-driven. It observes the system through the same REST endpoints operators and smoke checks use, then derives latency and correctness metrics from workflow, task, worker, and event records.

For stable comparisons, keep worker count, worker capacity, scheduler tick, retry policy, and Docker/Kubernetes resource limits fixed between runs.
