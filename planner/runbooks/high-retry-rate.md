# High Retry Rate

## Signal

Retry_wait tasks, repeated retryable handler errors, or elevated retry metrics show that work is not completing on the first attempt.

## Impact

Retries protect transient failures but can amplify load on downstream systems if concurrency remains too high.

## Recovery

Identify the shared task type or dependency behind retrying attempts. Temporarily reduce concurrency or batch size for that task type and confirm retry_wait tasks promote back to ready.

## Prevention

Use exponential backoff, cap max_attempts, and surface retry rate by task_type in Prometheus and Grafana.
