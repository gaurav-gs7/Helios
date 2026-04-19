# Pause Heartbeats

For the native worker process, stop the worker or disconnect it from the control plane and observe:

- worker state transitions from `healthy` to `stale` to `dead`
- active leases eventually recover
- retry or failure behavior is visible through workflow inspection and `/metrics`
