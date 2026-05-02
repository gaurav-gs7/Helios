package handlers

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/gauravgs7/helios/internal/domain"
)

func TestValidatePayloadReturnsValidAndRejectedCounts(t *testing.T) {
	handler := Builtins()["validate_payload"]
	output, err := handler(context.Background(), ExecutionContext{Assignment: domain.Assignment{
		InputPayload: json.RawMessage(`{
			"records": [
				{"id":"ord-1","amount":10,"country":"us","vip":true},
				{"id":"ord-2","amount":"bad","country":"us","vip":false}
			],
			"required_fields": ["id", "amount", "country"],
			"field_types": {"id":"string", "amount":"number", "vip":"bool"},
			"unique_key": "id"
		}`),
	}})
	if err != nil {
		t.Fatalf("validate_payload failed: %v", err)
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

func TestTransformRecordsNormalizesAndRenamesFields(t *testing.T) {
	handler := Builtins()["transform_records"]
	output, err := handler(context.Background(), ExecutionContext{Assignment: domain.Assignment{
		InputPayload: json.RawMessage(`{
			"records": [{"id":"ord-1","country":"us","amount":10.235,"extra":"drop"}],
			"select_fields": ["id", "country", "amount"],
			"rename_fields": {"amount":"order_amount"},
			"uppercase_fields": ["country"],
			"round_fields": {"order_amount": 2},
			"add_fields": {"pipeline":"orders-v1"}
		}`),
	}})
	if err != nil {
		t.Fatalf("transform_records failed: %v", err)
	}
	var decoded struct {
		Records []map[string]any `json:"records"`
	}
	if err := json.Unmarshal(output, &decoded); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	record := decoded.Records[0]
	if record["country"] != "US" || record["order_amount"] != 10.24 || record["pipeline"] != "orders-v1" {
		t.Fatalf("unexpected transformed record: %+v", record)
	}
	if _, exists := record["extra"]; exists {
		t.Fatalf("select_fields should have removed extra field: %+v", record)
	}
}

func TestModelInferenceRetriesTransientFailureThenScores(t *testing.T) {
	handler := Builtins()["model_inference"]
	assignment := domain.Assignment{
		Attempt: 1,
		InputPayload: json.RawMessage(`{
			"model_name": "risk-rules-v1",
			"fail_until_attempt": 1,
			"retryable_failure": true,
			"records": [{"id":"ord-1","amount":1200,"country":"NG","channel":"web"}],
			"rules": [
				{"field":"amount","operator":"gte","value":1000,"score":0.5,"contributor":"high_amount"},
				{"field":"country","operator":"in","values":["NG","IR"],"score":0.3,"contributor":"high_risk_geo"}
			]
		}`),
	}
	if _, err := handler(context.Background(), ExecutionContext{Assignment: assignment}); err == nil || !IsRetryable(err) {
		t.Fatalf("expected retryable inference error, got %v", err)
	}
	assignment.Attempt = 2
	output, err := handler(context.Background(), ExecutionContext{Assignment: assignment})
	if err != nil {
		t.Fatalf("expected retry success, got %v", err)
	}
	var decoded struct {
		Status      string `json:"status"`
		Predictions []struct {
			Decision string  `json:"decision"`
			Score    float64 `json:"score"`
		} `json:"predictions"`
	}
	if err := json.Unmarshal(output, &decoded); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if decoded.Status != "inferred" || decoded.Predictions[0].Decision != "block" || decoded.Predictions[0].Score < 0.8 {
		t.Fatalf("unexpected inference output: %+v", decoded)
	}
}

func TestWriteArtifactWritesIdempotentLocalFile(t *testing.T) {
	handler := Builtins()["write_artifact"]
	dir := t.TempDir()
	t.Setenv("HELIOS_ARTIFACT_BASE_PATH", dir)
	assignment := domain.Assignment{
		IdempotencyKey: "artifact-test-v1",
		InputPayload: json.RawMessage(`{
			"sink": "local",
			"dataset": "orders",
			"base_path": "` + filepath.ToSlash(dir) + `",
			"artifact": {"count": 2, "status": "ready"}
		}`),
	}
	output, err := handler(context.Background(), ExecutionContext{Assignment: assignment})
	if err != nil {
		t.Fatalf("write_artifact failed: %v", err)
	}
	var decoded struct {
		Status string `json:"status"`
		Path   string `json:"path"`
	}
	if err := json.Unmarshal(output, &decoded); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if decoded.Status != "written" {
		t.Fatalf("unexpected artifact status: %+v", decoded)
	}
	if _, err := os.Stat(decoded.Path); err != nil {
		t.Fatalf("expected artifact file to exist: %v", err)
	}
}

func TestWriteArtifactRejectsBasePathOutsideArtifactRoot(t *testing.T) {
	handler := Builtins()["write_artifact"]
	root := t.TempDir()
	outside := t.TempDir()
	t.Setenv("HELIOS_ARTIFACT_BASE_PATH", root)
	_, err := handler(context.Background(), ExecutionContext{Assignment: domain.Assignment{
		IdempotencyKey: "artifact-test-v1",
		InputPayload: json.RawMessage(`{
			"sink": "local",
			"dataset": "orders",
			"base_path": "` + filepath.ToSlash(outside) + `",
			"artifact": {"count": 2, "status": "ready"}
		}`),
	}})
	if err == nil {
		t.Fatal("expected write_artifact to reject base_path outside artifact root")
	}
	if IsRetryable(err) {
		t.Fatalf("base_path validation should be non-retryable, got %v", err)
	}
}

func TestNotifyWebhookDryRunDoesNotRequireURL(t *testing.T) {
	handler := Builtins()["notify_webhook"]
	output, err := handler(context.Background(), ExecutionContext{Assignment: domain.Assignment{
		IdempotencyKey: "notify-test-v1",
		InputPayload:   json.RawMessage(`{"dry_run":true,"body":{"status":"ok"}}`),
	}})
	if err != nil {
		t.Fatalf("notify_webhook dry run failed: %v", err)
	}
	var decoded struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(output, &decoded); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if decoded.Status != "dry_run" {
		t.Fatalf("unexpected notification output: %+v", decoded)
	}
}
