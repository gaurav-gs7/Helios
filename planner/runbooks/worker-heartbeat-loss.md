# Worker Heartbeat Loss

## Signal

A worker is stale or dead when heartbeats stop arriving within the configured stale/dead thresholds. Tasks assigned to that worker may remain leased or running until the lease expires.

## Impact

Helios provides at-least-once execution, so abandoned work is recovered by lease expiry and retry policy. Duplicate execution is possible if the original worker later reports a stale result.

## Recovery

Check worker health, last heartbeat age, and active task leases. Verify expired attempts are requeued with a new attempt ID and reject stale results from old attempts.

## Prevention

Keep heartbeat intervals significantly shorter than lease duration. Alert on stale workers before tasks hit terminal retry exhaustion.
