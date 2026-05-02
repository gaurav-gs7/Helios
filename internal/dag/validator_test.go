package dag

import (
	"encoding/json"
	"testing"

	"github.com/gauravgs7/helios/internal/domain"
)

func TestValidateAcceptsValidDAG(t *testing.T) {
	spec := domain.WorkflowSpec{
		Name: "etl",
		Tasks: []domain.TaskSpec{
			{
				TaskID:         "extract",
				TaskType:       "validate_payload",
				InputPayload:   json.RawMessage(`{"records":[{"id":"txn-1","amount":10,"currency":"usd","merchant_id":"m1","country":"us"}]}`),
				TimeoutSeconds: 10,
				RetryPolicy:    domain.RetryPolicy{MaxAttempts: 3},
			},
			{
				TaskID:         "transform",
				TaskType:       "transform_records",
				Dependencies:   []string{"extract"},
				InputPayload:   json.RawMessage(`{"records":[{"id":"txn-1","amount":10,"currency":"usd","merchant_id":"m1","country":"us"}]}`),
				TimeoutSeconds: 10,
				RetryPolicy:    domain.RetryPolicy{MaxAttempts: 3},
			},
		},
	}
	if err := Validate(spec); err != nil {
		t.Fatalf("expected valid DAG, got %v", err)
	}
}

func TestValidateRejectsCycle(t *testing.T) {
	spec := domain.WorkflowSpec{
		Name: "bad",
		Tasks: []domain.TaskSpec{
			{
				TaskID:         "a",
				TaskType:       "validate_payload",
				Dependencies:   []string{"b"},
				InputPayload:   json.RawMessage(`{}`),
				TimeoutSeconds: 10,
				RetryPolicy:    domain.RetryPolicy{MaxAttempts: 3},
			},
			{
				TaskID:         "b",
				TaskType:       "transform_records",
				Dependencies:   []string{"a"},
				InputPayload:   json.RawMessage(`{}`),
				TimeoutSeconds: 10,
				RetryPolicy:    domain.RetryPolicy{MaxAttempts: 3},
			},
		},
	}
	if err := Validate(spec); err == nil {
		t.Fatal("expected cycle rejection")
	}
}
