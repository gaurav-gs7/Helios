#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import statistics
from datetime import datetime
from pathlib import Path
from typing import Any

TERMINAL_WORKFLOW_STATES = {"succeeded", "failed", "cancelled"}
TERMINAL_TASK_STATES = {"succeeded", "failed", "timed_out", "cancelled"}


def load_json(path: str | Path) -> Any:
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def parse_time(raw: str | None) -> datetime | None:
    if not raw:
        return None
    normalized = raw.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


def elapsed_ms(start: datetime | None, end: datetime | None) -> int | None:
    if start is None or end is None:
        return None
    return max(0, round((end - start).total_seconds() * 1000))


def percentile(values: list[float], pct: int) -> float:
    if not values:
        return 0
    sorted_values = sorted(values)
    index = math.ceil((pct / 100) * len(sorted_values)) - 1
    return sorted_values[max(0, min(index, len(sorted_values) - 1))]


def distribution(values: list[float]) -> dict[str, float]:
    return {
        "count": len(values),
        "avg": round(statistics.mean(values), 2) if values else 0,
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "max": max(values) if values else 0,
    }


def text_contains_stale_signal(value: Any) -> bool:
    corpus = json.dumps(value, sort_keys=True).lower()
    return any(
        needle in corpus
        for needle in (
            "stale",
            "does not match active attempt",
            "active attempt",
            "late result",
            "old attempt",
        )
    )


def read_manifest(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            (
                workflow_id,
                submitted_ms,
                terminal_ms,
                summary_path,
                tasks_path,
                events_path,
            ) = line.rstrip("\n").split("\t")
            rows.append(
                {
                    "workflow_id": workflow_id,
                    "submitted_ms": submitted_ms,
                    "terminal_ms": terminal_ms,
                    "summary_path": summary_path,
                    "tasks_path": tasks_path,
                    "events_path": events_path,
                }
            )
    return rows


def worker_utilization(worker_sample_dir: Path) -> dict[str, Any]:
    utilizations: list[float] = []
    healthy_samples = 0
    worker_samples = 0
    for path in sorted(worker_sample_dir.glob("workers-*.json")):
        workers = load_json(path)
        for worker in workers:
            capacity = int(worker.get("capacity") or 0)
            if capacity <= 0:
                continue
            worker_samples += 1
            if worker.get("health") == "healthy":
                healthy_samples += 1
            running = int(worker.get("running_task_count") or 0)
            utilizations.append(min(1.0, max(0.0, running / capacity)))
    return {
        "worker_sample_count": worker_samples,
        "healthy_worker_sample_count": healthy_samples,
        "utilization_ratio": distribution(utilizations),
    }


def summarize(args: argparse.Namespace) -> dict[str, Any]:
    rows = read_manifest(Path(args.manifest_file))
    workflow_latencies: list[float] = []
    scheduler_lease_latencies: list[float] = []
    recovery_times: list[float] = []
    states: dict[str, int] = {}
    total_tasks = 0
    total_task_attempts = 0
    retry_tasks = 0
    terminal_task_events = 0
    succeeded_workflows = 0
    failed_workflows = 0
    stale_result_count = 0
    duplicate_assignment_count = 0
    lease_event_count = 0
    recovery_event_count = 0
    seen_attempt_ids: set[str] = set()

    for row in rows:
        submitted_ms = int(row["submitted_ms"])
        terminal_ms = int(row["terminal_ms"])
        workflow_latencies.append(max(0, terminal_ms - submitted_ms))

        summary = load_json(row["summary_path"])
        tasks_body = load_json(row["tasks_path"])
        events_body = load_json(row["events_path"])
        tasks = tasks_body.get("tasks", [])
        events = events_body.get("events", [])

        state = summary.get("state", "unknown")
        states[state] = states.get(state, 0) + 1
        if state == "succeeded":
            succeeded_workflows += 1
        if state in TERMINAL_WORKFLOW_STATES - {"succeeded"}:
            failed_workflows += 1

        total_tasks += len(tasks)
        for task in tasks:
            attempts = int(task.get("attempt", 0) or 0)
            total_task_attempts += attempts
            if attempts > 1:
                retry_tasks += 1

        ready_at_by_task: dict[str, datetime] = {}
        timed_out_at_by_task: dict[str, datetime] = {}
        for event in sorted(events, key=lambda item: item.get("occurred_at", "")):
            task_id = event.get("task_id") or ""
            new_state = event.get("new_state") or ""
            old_state = event.get("old_state") or ""
            actor = event.get("actor") or ""
            occurred_at = parse_time(event.get("occurred_at"))

            if text_contains_stale_signal(event):
                stale_result_count += 1
            if new_state in TERMINAL_TASK_STATES:
                terminal_task_events += 1
            if task_id and new_state == "ready":
                ready_at_by_task[task_id] = occurred_at
            if task_id and new_state == "leased" and actor == "scheduler":
                lease_event_count += 1
                attempt_id = event.get("attempt_id") or ""
                if attempt_id:
                    if attempt_id in seen_attempt_ids:
                        duplicate_assignment_count += 1
                    seen_attempt_ids.add(attempt_id)
                lease_latency = elapsed_ms(ready_at_by_task.get(task_id), occurred_at)
                if lease_latency is not None:
                    scheduler_lease_latencies.append(lease_latency)
            if task_id and new_state == "timed_out" and actor == "recovery":
                timed_out_at_by_task[task_id] = occurred_at
                recovery_event_count += 1
            if task_id and old_state == "timed_out" and actor == "recovery":
                recovery_time = elapsed_ms(timed_out_at_by_task.get(task_id), occurred_at)
                if recovery_time is not None:
                    recovery_times.append(recovery_time)

    submit_elapsed = max(1, int(args.submit_finished_ms) - int(args.submit_started_ms))
    total_elapsed = max(1, int(args.finished_ms) - int(args.submit_started_ms))
    elapsed_seconds = total_elapsed / 1000
    count = int(args.workflow_count)

    summary = {
        "run_id": Path(args.result_file).name.removeprefix("benchmark-").removesuffix(".json"),
        "api_url": args.api_url,
        "workflow_file": args.workflow_file,
        "submitted_workflows": count,
        "observed_terminal_workflows": len(rows),
        "succeeded_workflows": succeeded_workflows,
        "failed_workflows": failed_workflows,
        "workflow_states": states,
        "total_tasks": total_tasks,
        "total_task_attempts": total_task_attempts,
        "retry_tasks": retry_tasks,
        "submission_elapsed_ms": submit_elapsed,
        "total_elapsed_ms": total_elapsed,
        "submission_rate_workflows_per_min": round(count * 60000 / submit_elapsed, 2),
        "completion_rate_workflows_per_min": round(succeeded_workflows * 60000 / total_elapsed, 2),
        "task_throughput_tasks_per_sec": round(terminal_task_events / elapsed_seconds, 2),
        "attempt_throughput_attempts_per_sec": round(total_task_attempts / elapsed_seconds, 2),
        "workflow_latency_ms": distribution(workflow_latencies),
        "scheduler_lease_latency_ms": distribution(scheduler_lease_latencies),
        "recovery_time_ms": distribution(recovery_times),
        "recovery_event_count": recovery_event_count,
        "stale_result_count": stale_result_count,
        "duplicate_assignment_count": duplicate_assignment_count,
        "scheduler_lease_event_count": lease_event_count,
        "worker_utilization": worker_utilization(Path(args.worker_sample_dir)),
    }
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize a Helios benchmark run.")
    parser.add_argument("--result-file", required=True)
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--workflow-file", required=True)
    parser.add_argument("--workflow-count", required=True)
    parser.add_argument("--submit-started-ms", required=True)
    parser.add_argument("--submit-finished-ms", required=True)
    parser.add_argument("--finished-ms", required=True)
    parser.add_argument("--manifest-file", required=True)
    parser.add_argument("--worker-sample-dir", required=True)
    args = parser.parse_args()

    summary = summarize(args)
    result_path = Path(args.result_file)
    result_path.parent.mkdir(parents=True, exist_ok=True)
    with open(result_path, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")

    for key, value in summary.items():
        if isinstance(value, (dict, list)):
            print(f"{key}={json.dumps(value, sort_keys=True)}")
        else:
            print(f"{key}={value}")


if __name__ == "__main__":
    main()
