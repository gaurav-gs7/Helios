# AI Planner Invalid DAG

## Signal

The AI planner returns a DAG with duplicate task IDs, unknown dependencies, cycles, unsupported task types, or unsafe retry/timeout settings.

## Impact

The workflow must not be submitted. AI output is untrusted until it passes schema validation, DAG validation, and policy checks.

## Recovery

Use dry-run output to identify the invalid task or dependency. Regenerate with a more explicit intent or provide stages manually.

## Prevention

Keep LLM output schema-constrained, enforce task type allowlists, validate dependencies in the Go control plane, and return clear errors to operators.
