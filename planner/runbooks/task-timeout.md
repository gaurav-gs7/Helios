# Task Timeout

## Signal

A task enters timed_out or retry_wait when execution exceeds timeout_seconds or when the active lease expires before completion.

## Impact

Timeouts can indicate payloads that are too large, downstream dependency latency, worker saturation, or an overly aggressive timeout policy.

## Recovery

Inspect the affected task type, attempt count, last_error, and p95 latency. If the error is transient, let exponential backoff retry. If timeouts repeat, reduce batch size or increase timeout_seconds.

## Prevention

Set task timeouts using historical p95 or p99 task latency and keep retry budgets bounded.
