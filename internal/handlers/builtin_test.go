package handlers

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/gauravgs7/helios/internal/domain"
)

func TestFailureProbeRetriesThenSucceeds(t *testing.T) {
	handler := Builtins()["failure_probe"]
	assignment := domain.Assignment{
		Attempt:      1,
		InputPayload: json.RawMessage(`{"fail_until_attempt":1,"retryable":true}`),
	}
	if _, err := handler(context.Background(), ExecutionContext{Assignment: assignment}); err == nil {
		t.Fatal("expected simulated failure")
	}
	assignment.Attempt = 2
	if _, err := handler(context.Background(), ExecutionContext{Assignment: assignment}); err != nil {
		t.Fatalf("expected recovery on second attempt, got %v", err)
	}
}

func TestValidateRecordsReturnsValidAndRejectedCounts(t *testing.T) {
	handler := Builtins()["validate_records"]
	output, err := handler(context.Background(), ExecutionContext{Assignment: domain.Assignment{
		InputPayload: json.RawMessage(`{
			"records": [
				{"id":"txn-1","amount":10,"currency":"usd","merchant_id":"m1","country":"us"},
				{"id":"txn-2","amount":0,"currency":"usd","merchant_id":"m2","country":"us"}
			]
		}`),
	}})
	if err != nil {
		t.Fatalf("validate_records failed: %v", err)
	}
	var decoded struct {
		ValidCount   int `json:"valid_count"`
		InvalidCount int `json:"invalid_count"`
	}
	if err := json.Unmarshal(output, &decoded); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if decoded.ValidCount != 1 || decoded.InvalidCount != 1 {
		t.Fatalf("unexpected counts: %+v", decoded)
	}
}

func TestScoreFraudRiskRetriesTransientModelFailure(t *testing.T) {
	handler := Builtins()["score_fraud_risk"]
	assignment := domain.Assignment{
		Attempt: 1,
		InputPayload: json.RawMessage(`{
			"fail_until_attempt": 1,
			"retryable_failure": true,
			"records": [
				{"id":"txn-1","amount":1200,"currency":"usd","merchant_id":"m1","country":"ng","channel":"web"}
			]
		}`),
	}
	if _, err := handler(context.Background(), ExecutionContext{Assignment: assignment}); err == nil || !IsRetryable(err) {
		t.Fatalf("expected retryable transient scoring error, got %v", err)
	}
	assignment.Attempt = 2
	output, err := handler(context.Background(), ExecutionContext{Assignment: assignment})
	if err != nil {
		t.Fatalf("expected retry success, got %v", err)
	}
	var decoded struct {
		Status string `json:"status"`
		Scores []struct {
			Decision string  `json:"decision"`
			Risk     float64 `json:"risk_score"`
		} `json:"scores"`
	}
	if err := json.Unmarshal(output, &decoded); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if decoded.Status != "scored" || decoded.Scores[0].Decision != "block" || decoded.Scores[0].Risk < 0.8 {
		t.Fatalf("unexpected scoring output: %+v", decoded)
	}
}

func TestEmbedTextBatchIsDeterministic(t *testing.T) {
	handler := Builtins()["embed_text_batch"]
	assignment := domain.Assignment{
		InputPayload: json.RawMessage(`{
			"dimensions": 4,
			"documents": [{"id":"doc-1","text":"hello helios"}]
		}`),
	}
	first, err := handler(context.Background(), ExecutionContext{Assignment: assignment})
	if err != nil {
		t.Fatalf("first embedding failed: %v", err)
	}
	second, err := handler(context.Background(), ExecutionContext{Assignment: assignment})
	if err != nil {
		t.Fatalf("second embedding failed: %v", err)
	}
	if string(first) != string(second) {
		t.Fatalf("expected deterministic output\nfirst=%s\nsecond=%s", first, second)
	}
}
