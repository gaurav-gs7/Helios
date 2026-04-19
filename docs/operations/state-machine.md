# State Machine

## Task States

- `pending`: dependencies not yet satisfied
- `ready`: scheduler may lease the task
- `leased`: assignment created and sent to a worker
- `running`: worker acknowledged execution start
- `retry_wait`: backoff in progress before another attempt
- `succeeded`: terminal success
- `failed`: terminal failure
- `timed_out`: active attempt exceeded lease/timeout budget
- `cancelled`: workflow or operator cancelled the task

## Workflow States

- `submitted`
- `running`
- `succeeded`
- `failed`
- `cancelled`

## Transition Rules

- Root tasks start as `ready`
- Dependent tasks start as `pending`
- Success unlocks downstream tasks whose dependencies have all succeeded
- Retryable failure or timeout moves to `retry_wait`
- Backoff expiry moves `retry_wait -> ready`
- Terminal failure drives workflow failure and cancels outstanding work

The canonical transition guard lives in [state_machine.go](/Users/gauravgs7/Documents/Projects/Helios-AI/Test_Helios/internal/domain/state_machine.go).
