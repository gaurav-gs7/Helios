# Benchmark Report

## Goal

Measure Helios workflow submission throughput, end-to-end workflow completion rate, task execution throughput, retry behavior, and local demo viability on a constrained developer machine.

This benchmark is intentionally practical rather than synthetic. It runs the production-style fraud/risk workflow from [workflow.json](/Users/gauravgs7/Documents/Projects/Helios-AI/Test_Helios/examples/workflow.json), which includes validation, feature enrichment, fraud scoring, aggregation, deterministic embedding generation, artifact persistence, and a retryable model-serving failure in the scoring stage.

## Environment

- Machine: MacBook Air M2, 8 GB RAM, 256 GB storage
- OS: macOS arm64, Darwin 25.4.0
- Runtime: Docker Compose
- Services running:
  - PostgreSQL 16
  - NATS 2.11
  - Helios control plane
  - Helios worker
  - Helios Gemini planner
  - Prometheus
  - Grafana
- Worker count: 1 active worker
- Worker capacity: 2
- Benchmark script: [run_benchmark.sh](/Users/gauravgs7/Documents/Projects/Helios-AI/Test_Helios/scripts/benchmark/run_benchmark.sh)
- Raw result: [benchmark-20260418T072628Z.json](/Users/gauravgs7/Documents/Projects/Helios-AI/Test_Helios/docs/benchmarks/results/benchmark-20260418T072628Z.json)

## Workload

The benchmark submitted 5 workflow instances using the default example workflow.

Each workflow contains 6 DAG tasks:

- `validate_payload`
- `transform_records`
- `model_inference`
- `aggregate_metrics`
- `write_artifact`
- `notify_webhook`

The `model_inference` task intentionally fails on its first attempt with a retryable error. This means the benchmark exercises retry/backoff and attempt-aware result handling, not just happy-path task execution.

## Results

| Metric | Value |
| --- | ---: |
| Submitted workflows | 5 |
| Succeeded workflows | 5 |
| Failed workflows | 0 |
| Total tasks | 30 |
| Total task attempts | 36 |
| Retry tasks observed | 6 |
| Submission elapsed | 475 ms |
| Total completion elapsed | 29,032 ms |
| Submission rate | 631.58 workflows/min |
| Completion rate | 10.33 workflows/min |
| Task completion rate | 1.03 tasks/sec |
| Attempt processing rate | 1.24 attempts/sec |
| Average workflow latency | 28,792.6 ms |
| p50 workflow latency | 28,773 ms |
| p95 workflow latency | 28,975 ms |
| Max workflow latency | 28,975 ms |

## Interpretation

All submitted workflows completed successfully, which validates the full local execution path:

- API submission
- DAG persistence
- scheduler leasing
- worker execution
- retry/backoff
- dependency unlock
- final workflow success reconciliation

The completion rate is intentionally lower than raw submission throughput because the workload includes retryable model-serving failure simulation and retry backoff. This is useful for production-readiness validation because it proves Helios handles degraded task execution rather than only measuring a happy path.

The submission path is fast relative to execution, completing 5 submissions in under half a second. End-to-end completion is bounded by one active worker, worker capacity, scheduler tick interval, workflow dependency structure, and intentional retry backoff.

## Production Signals

This benchmark demonstrates:

- Helios can run multiple DAG workflows concurrently on a small laptop.
- The scheduler can continue dispatching work while retryable failures are present.
- Retry attempts do not prevent workflows from reaching terminal success.
- The control plane can persist and inspect completed workflow histories.
- The benchmark script produces reproducible JSON output that can be tracked across commits.

## Bottlenecks Observed

- Single active worker limits task throughput.
- The sample DAG has a long critical path, so not every task can run in parallel.
- Retry backoff in `model_inference` intentionally increases workflow latency.
- Poll-based benchmark measurement adds up to 2 seconds of observation delay.
- Docker Desktop overhead is visible on an 8 GB MacBook Air when Prometheus and Grafana are also running.

## Next Benchmark Improvements

- Run matrix benchmarks with 1, 2, and 4 workers.
- Add a no-retry workload to measure raw happy-path scheduler throughput.
- Add p95 scheduler lease latency once scheduler histograms are available.
- Add Prometheus query snapshots before and after each benchmark run.
- Add Kubernetes/kind benchmark results after deploying the Kustomize dev overlay.
- Add worker-death recovery timing using the chaos script and compare lease duration settings.
