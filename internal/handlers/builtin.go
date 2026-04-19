package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/gauravgs7/helios/internal/domain"
)

type ExecutionContext struct {
	Assignment domain.Assignment
}

type Handler func(ctx context.Context, exec ExecutionContext) (json.RawMessage, error)

type ExecutionError struct {
	Err       error
	Retryable bool
}

type transactionRecord struct {
	ID         string  `json:"id"`
	Amount     float64 `json:"amount"`
	Currency   string  `json:"currency"`
	MerchantID string  `json:"merchant_id"`
	Country    string  `json:"country"`
	Channel    string  `json:"channel,omitempty"`
}

type riskScore struct {
	ID           string   `json:"id"`
	RiskScore    float64  `json:"risk_score"`
	Decision     string   `json:"decision"`
	Contributors []string `json:"contributors"`
}

func (e *ExecutionError) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func Builtins() map[string]Handler {
	return map[string]Handler{
		"failure_probe":          failureProbeHandler,
		"validate_records":       validateRecordsHandler,
		"enrich_risk_features":   enrichRiskFeaturesHandler,
		"score_fraud_risk":       scoreFraudRiskHandler,
		"aggregate_risk_results": aggregateRiskResultsHandler,
		"persist_artifact":       persistArtifactHandler,
		"embed_text_batch":       embedTextBatchHandler,
	}
}

func SupportedTaskTypes() []string {
	types := make([]string, 0, len(Builtins()))
	for taskType := range Builtins() {
		types = append(types, taskType)
	}
	sort.Strings(types)
	return types
}

func IsRetryable(err error) bool {
	var execErr *ExecutionError
	if err != nil && errorAs(err, &execErr) {
		return execErr.Retryable
	}
	return false
}

func errorAs(err error, target **ExecutionError) bool {
	execErr, ok := err.(*ExecutionError)
	if ok {
		*target = execErr
		return true
	}
	return false
}

func failureProbeHandler(_ context.Context, exec ExecutionContext) (json.RawMessage, error) {
	var input struct {
		FailUntilAttempt int  `json:"fail_until_attempt"`
		Retryable        bool `json:"retryable"`
	}
	if err := json.Unmarshal(exec.Assignment.InputPayload, &input); err != nil {
		return nil, nonRetryable("decode failure_probe payload: %w", err)
	}
	if exec.Assignment.Attempt <= input.FailUntilAttempt {
		if input.Retryable {
			return nil, retryable("simulated failure at attempt %d", exec.Assignment.Attempt)
		}
		return nil, nonRetryable("simulated failure at attempt %d", exec.Assignment.Attempt)
	}
	return mustMarshal(map[string]any{
		"status":  "recovered",
		"attempt": exec.Assignment.Attempt,
	}), nil
}

func validateRecordsHandler(_ context.Context, exec ExecutionContext) (json.RawMessage, error) {
	var input struct {
		Records []transactionRecord `json:"records"`
	}
	if err := json.Unmarshal(exec.Assignment.InputPayload, &input); err != nil {
		return nil, nonRetryable("decode validate_records payload: %w", err)
	}
	if len(input.Records) == 0 {
		return nil, nonRetryable("validate_records requires at least one record")
	}
	valid := make([]transactionRecord, 0, len(input.Records))
	rejected := make([]map[string]string, 0)
	seen := make(map[string]struct{}, len(input.Records))
	for _, record := range input.Records {
		reason := validateTransaction(record, seen)
		if reason != "" {
			rejected = append(rejected, map[string]string{"id": record.ID, "reason": reason})
			continue
		}
		seen[record.ID] = struct{}{}
		valid = append(valid, normalizeTransaction(record))
	}
	return mustMarshal(map[string]any{
		"status":        "validated",
		"valid_count":   len(valid),
		"invalid_count": len(rejected),
		"records":       valid,
		"rejected":      rejected,
		"checksum":      checksum(valid),
	}), nil
}

func enrichRiskFeaturesHandler(_ context.Context, exec ExecutionContext) (json.RawMessage, error) {
	var input struct {
		Records []transactionRecord `json:"records"`
	}
	if err := json.Unmarshal(exec.Assignment.InputPayload, &input); err != nil {
		return nil, nonRetryable("decode enrich_risk_features payload: %w", err)
	}
	if len(input.Records) == 0 {
		return nil, nonRetryable("enrich_risk_features requires records")
	}
	features := make([]map[string]any, 0, len(input.Records))
	for _, record := range input.Records {
		normalized := normalizeTransaction(record)
		features = append(features, map[string]any{
			"id":                 normalized.ID,
			"amount_bucket":      amountBucket(normalized.Amount),
			"is_cross_border":    normalized.Country != "US",
			"is_high_risk_geo":   isHighRiskCountry(normalized.Country),
			"merchant_hash":      shortHash(normalized.MerchantID),
			"channel":            normalized.Channel,
			"normalized_amount":  math.Round(normalized.Amount*100) / 100,
			"feature_version":    "risk-features-v1",
			"idempotency_key":    exec.Assignment.IdempotencyKey,
			"source_attempt":     exec.Assignment.Attempt,
			"source_workflow_id": exec.Assignment.WorkflowID,
		})
	}
	return mustMarshal(map[string]any{
		"status":    "features_built",
		"features":  features,
		"count":     len(features),
		"checksum":  checksum(features),
		"generated": time.Now().Format(time.RFC3339),
	}), nil
}

func scoreFraudRiskHandler(_ context.Context, exec ExecutionContext) (json.RawMessage, error) {
	var input struct {
		Records          []transactionRecord `json:"records"`
		FailUntilAttempt int                 `json:"fail_until_attempt,omitempty"`
		RetryableFailure bool                `json:"retryable_failure,omitempty"`
	}
	if err := json.Unmarshal(exec.Assignment.InputPayload, &input); err != nil {
		return nil, nonRetryable("decode score_fraud_risk payload: %w", err)
	}
	if exec.Assignment.Attempt <= input.FailUntilAttempt {
		if input.RetryableFailure {
			return nil, retryable("transient model-serving error at attempt %d", exec.Assignment.Attempt)
		}
		return nil, nonRetryable("non-retryable model input error at attempt %d", exec.Assignment.Attempt)
	}
	if len(input.Records) == 0 {
		return nil, nonRetryable("score_fraud_risk requires records")
	}
	scores := make([]riskScore, 0, len(input.Records))
	for _, record := range input.Records {
		scores = append(scores, scoreTransaction(normalizeTransaction(record)))
	}
	return mustMarshal(map[string]any{
		"status":        "scored",
		"model":         "fraud-risk-rules-v1",
		"attempt":       exec.Assignment.Attempt,
		"scores":        scores,
		"score_count":   len(scores),
		"max_risk":      maxRisk(scores),
		"decision_hash": checksum(scores),
	}), nil
}

func aggregateRiskResultsHandler(_ context.Context, exec ExecutionContext) (json.RawMessage, error) {
	var input struct {
		Scores []riskScore `json:"scores"`
	}
	if err := json.Unmarshal(exec.Assignment.InputPayload, &input); err != nil {
		return nil, nonRetryable("decode aggregate_risk_results payload: %w", err)
	}
	if len(input.Scores) == 0 {
		return nil, nonRetryable("aggregate_risk_results requires scores")
	}
	var sum, max float64
	decisionCounts := map[string]int{"approve": 0, "review": 0, "block": 0}
	for _, score := range input.Scores {
		sum += score.RiskScore
		if score.RiskScore > max {
			max = score.RiskScore
		}
		decisionCounts[score.Decision]++
	}
	average := math.Round((sum/float64(len(input.Scores)))*1000) / 1000
	return mustMarshal(map[string]any{
		"status":          "aggregated",
		"score_count":     len(input.Scores),
		"avg_risk":        average,
		"max_risk":        max,
		"decision_counts": decisionCounts,
		"checksum":        checksum(input.Scores),
	}), nil
}

func persistArtifactHandler(_ context.Context, exec ExecutionContext) (json.RawMessage, error) {
	var input struct {
		Sink     string          `json:"sink"`
		Dataset  string          `json:"dataset"`
		Artifact json.RawMessage `json:"artifact"`
	}
	if err := json.Unmarshal(exec.Assignment.InputPayload, &input); err != nil {
		return nil, nonRetryable("decode persist_artifact payload: %w", err)
	}
	if strings.TrimSpace(input.Sink) == "" || strings.TrimSpace(input.Dataset) == "" {
		return nil, nonRetryable("persist_artifact requires sink and dataset")
	}
	if len(input.Artifact) == 0 || string(input.Artifact) == "null" {
		return nil, nonRetryable("persist_artifact requires artifact")
	}
	artifactID := shortHash(fmt.Sprintf("%s:%s:%s:%s", input.Sink, input.Dataset, exec.Assignment.IdempotencyKey, checksum(input.Artifact)))
	return mustMarshal(map[string]any{
		"status":          "persisted",
		"sink":            input.Sink,
		"dataset":         input.Dataset,
		"artifact_id":     artifactID,
		"idempotency_key": exec.Assignment.IdempotencyKey,
		"checksum":        checksum(input.Artifact),
		"recorded_at":     time.Now().Format(time.RFC3339),
	}), nil
}

func embedTextBatchHandler(_ context.Context, exec ExecutionContext) (json.RawMessage, error) {
	var input struct {
		Documents []struct {
			ID   string `json:"id"`
			Text string `json:"text"`
		} `json:"documents"`
		Dimensions int `json:"dimensions"`
	}
	if err := json.Unmarshal(exec.Assignment.InputPayload, &input); err != nil {
		return nil, nonRetryable("decode embed_text_batch payload: %w", err)
	}
	if len(input.Documents) == 0 {
		return nil, nonRetryable("embed_text_batch requires documents")
	}
	if input.Dimensions <= 0 {
		input.Dimensions = 8
	}
	if input.Dimensions > 64 {
		return nil, nonRetryable("embed_text_batch dimensions must be <= 64 for local trusted handler")
	}
	embeddings := make([]map[string]any, 0, len(input.Documents))
	for _, document := range input.Documents {
		if strings.TrimSpace(document.ID) == "" || strings.TrimSpace(document.Text) == "" {
			return nil, nonRetryable("embed_text_batch documents require id and text")
		}
		embeddings = append(embeddings, map[string]any{
			"id":        document.ID,
			"dimension": input.Dimensions,
			"vector":    deterministicVector(document.Text, input.Dimensions),
			"text_hash": shortHash(document.Text),
		})
	}
	return mustMarshal(map[string]any{
		"status":          "embedded",
		"embedding_model": "deterministic-local-v1",
		"count":           len(embeddings),
		"embeddings":      embeddings,
		"checksum":        checksum(embeddings),
	}), nil
}

func validateTransaction(record transactionRecord, seen map[string]struct{}) string {
	if strings.TrimSpace(record.ID) == "" {
		return "missing id"
	}
	if _, exists := seen[record.ID]; exists {
		return "duplicate id"
	}
	if record.Amount <= 0 {
		return "amount must be positive"
	}
	if strings.TrimSpace(record.Currency) == "" {
		return "missing currency"
	}
	if strings.TrimSpace(record.MerchantID) == "" {
		return "missing merchant_id"
	}
	if strings.TrimSpace(record.Country) == "" {
		return "missing country"
	}
	return ""
}

func normalizeTransaction(record transactionRecord) transactionRecord {
	record.ID = strings.TrimSpace(record.ID)
	record.Currency = strings.ToUpper(strings.TrimSpace(record.Currency))
	record.Country = strings.ToUpper(strings.TrimSpace(record.Country))
	record.MerchantID = strings.TrimSpace(record.MerchantID)
	record.Channel = strings.ToLower(strings.TrimSpace(record.Channel))
	if record.Channel == "" {
		record.Channel = "unknown"
	}
	record.Amount = math.Round(record.Amount*100) / 100
	return record
}

func amountBucket(amount float64) string {
	switch {
	case amount >= 1000:
		return "very_high"
	case amount >= 500:
		return "high"
	case amount >= 100:
		return "medium"
	default:
		return "low"
	}
}

func isHighRiskCountry(country string) bool {
	switch strings.ToUpper(country) {
	case "NG", "RU", "KP", "IR":
		return true
	default:
		return false
	}
}

func scoreTransaction(record transactionRecord) riskScore {
	score := 0.05
	contributors := []string{"base"}
	if record.Amount >= 1000 {
		score += 0.45
		contributors = append(contributors, "very_high_amount")
	} else if record.Amount >= 500 {
		score += 0.25
		contributors = append(contributors, "high_amount")
	}
	if record.Country != "US" {
		score += 0.15
		contributors = append(contributors, "cross_border")
	}
	if isHighRiskCountry(record.Country) {
		score += 0.30
		contributors = append(contributors, "high_risk_geo")
	}
	if record.Channel == "card_not_present" || record.Channel == "web" {
		score += 0.10
		contributors = append(contributors, "remote_channel")
	}
	score = math.Min(0.99, math.Round(score*1000)/1000)
	decision := "approve"
	if score >= 0.80 {
		decision = "block"
	} else if score >= 0.45 {
		decision = "review"
	}
	return riskScore{
		ID:           record.ID,
		RiskScore:    score,
		Decision:     decision,
		Contributors: contributors,
	}
}

func maxRisk(scores []riskScore) float64 {
	var max float64
	for _, score := range scores {
		if score.RiskScore > max {
			max = score.RiskScore
		}
	}
	return max
}

func deterministicVector(text string, dimensions int) []float64 {
	vector := make([]float64, dimensions)
	seed := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(text))))
	for i := range dimensions {
		value := int(seed[i%len(seed)])
		vector[i] = math.Round(((float64(value)/255.0)*2-1)*10000) / 10000
	}
	return vector
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:16]
}

func checksum(value any) string {
	body, _ := json.Marshal(value)
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func retryable(format string, args ...any) *ExecutionError {
	return &ExecutionError{Err: fmt.Errorf(format, args...), Retryable: true}
}

func nonRetryable(format string, args ...any) *ExecutionError {
	return &ExecutionError{Err: fmt.Errorf(format, args...), Retryable: false}
}

func mustMarshal(value any) json.RawMessage {
	body, _ := json.Marshal(value)
	return body
}
