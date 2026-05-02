package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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

type genericRecord map[string]any

type inferencePrediction struct {
	ID           string   `json:"id"`
	Score        float64  `json:"score"`
	Decision     string   `json:"decision"`
	Contributors []string `json:"contributors"`
}

const defaultArtifactBasePath = "/tmp/helios-artifacts"

func (e *ExecutionError) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func Builtins() map[string]Handler {
	return map[string]Handler{
		"validate_payload":  validatePayloadHandler,
		"transform_records": transformRecordsHandler,
		"model_inference":   modelInferenceHandler,
		"aggregate_metrics": aggregateMetricsHandler,
		"write_artifact":    writeArtifactHandler,
		"notify_webhook":    notifyWebhookHandler,
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

func validatePayloadHandler(_ context.Context, exec ExecutionContext) (json.RawMessage, error) {
	var input struct {
		Records         []genericRecord   `json:"records"`
		RequiredFields  []string          `json:"required_fields"`
		FieldTypes      map[string]string `json:"field_types"`
		UniqueKey       string            `json:"unique_key"`
		RejectOnInvalid bool              `json:"reject_on_invalid"`
	}
	if err := json.Unmarshal(exec.Assignment.InputPayload, &input); err != nil {
		return nil, nonRetryable("decode validate_payload payload: %w", err)
	}
	if len(input.Records) == 0 {
		return nil, nonRetryable("validate_payload requires at least one record")
	}
	seen := map[string]struct{}{}
	valid := make([]genericRecord, 0, len(input.Records))
	rejected := make([]map[string]any, 0)
	for index, record := range input.Records {
		reasons := validateRecord(record, input.RequiredFields, input.FieldTypes)
		if input.UniqueKey != "" {
			key := strings.TrimSpace(fmt.Sprint(record[input.UniqueKey]))
			if key == "" {
				reasons = append(reasons, fmt.Sprintf("missing unique key %s", input.UniqueKey))
			} else if _, ok := seen[key]; ok {
				reasons = append(reasons, fmt.Sprintf("duplicate unique key %s=%s", input.UniqueKey, key))
			}
			seen[key] = struct{}{}
		}
		if len(reasons) > 0 {
			rejected = append(rejected, map[string]any{
				"index":   index,
				"record":  record,
				"reasons": reasons,
			})
			continue
		}
		valid = append(valid, cloneRecord(record))
	}
	if input.RejectOnInvalid && len(rejected) > 0 {
		return nil, nonRetryable("validate_payload rejected %d invalid records", len(rejected))
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

func transformRecordsHandler(_ context.Context, exec ExecutionContext) (json.RawMessage, error) {
	var input struct {
		Records         []genericRecord   `json:"records"`
		SelectFields    []string          `json:"select_fields"`
		RenameFields    map[string]string `json:"rename_fields"`
		AddFields       map[string]any    `json:"add_fields"`
		UppercaseFields []string          `json:"uppercase_fields"`
		LowercaseFields []string          `json:"lowercase_fields"`
		RoundFields     map[string]int    `json:"round_fields"`
	}
	if err := json.Unmarshal(exec.Assignment.InputPayload, &input); err != nil {
		return nil, nonRetryable("decode transform_records payload: %w", err)
	}
	if len(input.Records) == 0 {
		return nil, nonRetryable("transform_records requires records")
	}
	transformed := make([]genericRecord, 0, len(input.Records))
	for _, record := range input.Records {
		next := cloneRecord(record)
		if len(input.SelectFields) > 0 {
			next = selectFields(next, input.SelectFields)
		}
		for from, to := range input.RenameFields {
			if value, ok := next[from]; ok {
				next[to] = value
				delete(next, from)
			}
		}
		for key, value := range input.AddFields {
			next[key] = value
		}
		normalizeTextFields(next, input.UppercaseFields, strings.ToUpper)
		normalizeTextFields(next, input.LowercaseFields, strings.ToLower)
		for field, places := range input.RoundFields {
			if value, ok := numberValue(next[field]); ok {
				next[field] = round(value, places)
			}
		}
		transformed = append(transformed, next)
	}
	return mustMarshal(map[string]any{
		"status":   "transformed",
		"count":    len(transformed),
		"records":  transformed,
		"checksum": checksum(transformed),
	}), nil
}

func modelInferenceHandler(_ context.Context, exec ExecutionContext) (json.RawMessage, error) {
	var input struct {
		Records          []genericRecord `json:"records"`
		ModelName        string          `json:"model_name"`
		IDField          string          `json:"id_field"`
		FailUntilAttempt int             `json:"fail_until_attempt"`
		RetryableFailure bool            `json:"retryable_failure"`
		Rules            []struct {
			Field       string  `json:"field"`
			Operator    string  `json:"operator"`
			Value       any     `json:"value"`
			Values      []any   `json:"values"`
			Score       float64 `json:"score"`
			Contributor string  `json:"contributor"`
		} `json:"rules"`
		DecisionThresholds map[string]float64 `json:"decision_thresholds"`
	}
	if err := json.Unmarshal(exec.Assignment.InputPayload, &input); err != nil {
		return nil, nonRetryable("decode model_inference payload: %w", err)
	}
	if exec.Assignment.Attempt <= input.FailUntilAttempt {
		if input.RetryableFailure {
			return nil, retryable("transient model inference failure at attempt %d", exec.Assignment.Attempt)
		}
		return nil, nonRetryable("non-retryable model inference failure at attempt %d", exec.Assignment.Attempt)
	}
	if len(input.Records) == 0 {
		return nil, nonRetryable("model_inference requires records")
	}
	if input.ModelName == "" {
		input.ModelName = "rules-v1"
	}
	if input.IDField == "" {
		input.IDField = "id"
	}
	predictions := make([]inferencePrediction, 0, len(input.Records))
	for index, record := range input.Records {
		score := 0.05
		contributors := []string{"base"}
		for _, rule := range input.Rules {
			if ruleMatches(record[rule.Field], rule.Operator, rule.Value, rule.Values) {
				score += rule.Score
				if strings.TrimSpace(rule.Contributor) != "" {
					contributors = append(contributors, rule.Contributor)
				}
			}
		}
		score = math.Min(0.99, round(score, 3))
		predictions = append(predictions, inferencePrediction{
			ID:           recordID(record, input.IDField, index),
			Score:        score,
			Decision:     decisionForScore(score, input.DecisionThresholds),
			Contributors: contributors,
		})
	}
	return mustMarshal(map[string]any{
		"status":           "inferred",
		"model":            input.ModelName,
		"attempt":          exec.Assignment.Attempt,
		"prediction_count": len(predictions),
		"predictions":      predictions,
		"checksum":         checksum(predictions),
	}), nil
}

func aggregateMetricsHandler(_ context.Context, exec ExecutionContext) (json.RawMessage, error) {
	var input struct {
		Predictions []inferencePrediction `json:"predictions"`
	}
	if err := json.Unmarshal(exec.Assignment.InputPayload, &input); err != nil {
		return nil, nonRetryable("decode aggregate_metrics payload: %w", err)
	}
	if len(input.Predictions) == 0 {
		return nil, nonRetryable("aggregate_metrics requires predictions")
	}
	decisionCounts := map[string]int{}
	var total float64
	var maxScore float64
	for _, prediction := range input.Predictions {
		decisionCounts[prediction.Decision]++
		total += prediction.Score
		if prediction.Score > maxScore {
			maxScore = prediction.Score
		}
	}
	return mustMarshal(map[string]any{
		"status":           "aggregated",
		"prediction_count": len(input.Predictions),
		"avg_score":        round(total/float64(len(input.Predictions)), 3),
		"max_score":        round(maxScore, 3),
		"decision_counts":  decisionCounts,
		"checksum":         checksum(input.Predictions),
	}), nil
}

func writeArtifactHandler(_ context.Context, exec ExecutionContext) (json.RawMessage, error) {
	var input struct {
		Sink      string          `json:"sink"`
		Dataset   string          `json:"dataset"`
		BasePath  string          `json:"base_path"`
		Artifact  json.RawMessage `json:"artifact"`
		Overwrite bool            `json:"overwrite"`
	}
	if err := json.Unmarshal(exec.Assignment.InputPayload, &input); err != nil {
		return nil, nonRetryable("decode write_artifact payload: %w", err)
	}
	if input.Sink == "" {
		input.Sink = "local"
	}
	if strings.TrimSpace(input.Dataset) == "" {
		return nil, nonRetryable("write_artifact requires dataset")
	}
	if len(input.Artifact) == 0 || string(input.Artifact) == "null" {
		return nil, nonRetryable("write_artifact requires artifact")
	}
	artifactChecksum := checksum(json.RawMessage(input.Artifact))
	artifactID := shortHash(fmt.Sprintf("%s:%s:%s", input.Dataset, exec.Assignment.IdempotencyKey, artifactChecksum))
	out := map[string]any{
		"status":          "recorded",
		"sink":            input.Sink,
		"dataset":         input.Dataset,
		"artifact_id":     artifactID,
		"idempotency_key": exec.Assignment.IdempotencyKey,
		"checksum":        artifactChecksum,
		"recorded_at":     time.Now().Format(time.RFC3339),
	}
	if input.Sink == "manifest" {
		out["manifest_only"] = true
		return mustMarshal(out), nil
	}
	if input.Sink != "local" {
		return nil, nonRetryable("write_artifact unsupported sink %q", input.Sink)
	}
	basePath, err := resolveArtifactBasePath(input.BasePath)
	if err != nil {
		return nil, nonRetryable("write_artifact invalid base_path: %w", err)
	}
	path, err := writeLocalArtifact(basePath, input.Dataset, artifactID, input.Artifact, input.Overwrite)
	if err != nil {
		return nil, retryable("write local artifact: %w", err)
	}
	out["status"] = "written"
	out["path"] = path
	return mustMarshal(out), nil
}

func notifyWebhookHandler(ctx context.Context, exec ExecutionContext) (json.RawMessage, error) {
	var input struct {
		URL            string            `json:"url"`
		Method         string            `json:"method"`
		Headers        map[string]string `json:"headers"`
		Body           json.RawMessage   `json:"body"`
		DryRun         bool              `json:"dry_run"`
		TimeoutSeconds int               `json:"timeout_seconds"`
	}
	if err := json.Unmarshal(exec.Assignment.InputPayload, &input); err != nil {
		return nil, nonRetryable("decode notify_webhook payload: %w", err)
	}
	if input.Method == "" {
		input.Method = http.MethodPost
	}
	if len(input.Body) == 0 {
		input.Body = []byte(`{}`)
	}
	if input.DryRun {
		return mustMarshal(map[string]any{
			"status":          "dry_run",
			"method":          input.Method,
			"url":             input.URL,
			"body_checksum":   checksum(json.RawMessage(input.Body)),
			"idempotency_key": exec.Assignment.IdempotencyKey,
		}), nil
	}
	if strings.TrimSpace(input.URL) == "" {
		return nil, nonRetryable("notify_webhook requires url unless dry_run=true")
	}
	parsed, err := url.Parse(input.URL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, nonRetryable("notify_webhook url is invalid")
	}
	if input.TimeoutSeconds <= 0 {
		input.TimeoutSeconds = 10
	}
	callCtx, cancel := context.WithTimeout(ctx, time.Duration(input.TimeoutSeconds)*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(callCtx, input.Method, input.URL, bytes.NewReader(input.Body))
	if err != nil {
		return nil, nonRetryable("build webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for key, value := range input.Headers {
		req.Header.Set(key, value)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, retryable("send webhook: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return nil, retryable("webhook returned retryable status %d: %s", resp.StatusCode, string(respBody))
	}
	if resp.StatusCode >= 400 {
		return nil, nonRetryable("webhook returned non-retryable status %d: %s", resp.StatusCode, string(respBody))
	}
	return mustMarshal(map[string]any{
		"status":        "delivered",
		"status_code":   resp.StatusCode,
		"body_checksum": checksum(json.RawMessage(input.Body)),
	}), nil
}

func validateRecord(record genericRecord, required []string, fieldTypes map[string]string) []string {
	reasons := []string{}
	for _, field := range required {
		if _, ok := record[field]; !ok || isEmpty(record[field]) {
			reasons = append(reasons, fmt.Sprintf("missing required field %s", field))
		}
	}
	for field, expected := range fieldTypes {
		if value, ok := record[field]; ok && !matchesType(value, expected) {
			reasons = append(reasons, fmt.Sprintf("field %s must be %s", field, expected))
		}
	}
	return reasons
}

func isEmpty(value any) bool {
	switch typed := value.(type) {
	case nil:
		return true
	case string:
		return strings.TrimSpace(typed) == ""
	default:
		return false
	}
}

func matchesType(value any, expected string) bool {
	switch strings.ToLower(strings.TrimSpace(expected)) {
	case "", "any":
		return true
	case "string":
		_, ok := value.(string)
		return ok
	case "number":
		_, ok := numberValue(value)
		return ok
	case "bool", "boolean":
		_, ok := value.(bool)
		return ok
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	default:
		return false
	}
}

func cloneRecord(in genericRecord) genericRecord {
	out := make(genericRecord, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func selectFields(record genericRecord, fields []string) genericRecord {
	selected := make(genericRecord, len(fields))
	for _, field := range fields {
		if value, ok := record[field]; ok {
			selected[field] = value
		}
	}
	return selected
}

func normalizeTextFields(record genericRecord, fields []string, normalize func(string) string) {
	for _, field := range fields {
		if value, ok := record[field].(string); ok {
			record[field] = normalize(strings.TrimSpace(value))
		}
	}
}

func numberValue(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func ruleMatches(actual any, operator string, value any, values []any) bool {
	operator = strings.ToLower(strings.TrimSpace(operator))
	if operator == "" {
		operator = "eq"
	}
	switch operator {
	case "eq":
		return fmt.Sprint(actual) == fmt.Sprint(value)
	case "ne":
		return fmt.Sprint(actual) != fmt.Sprint(value)
	case "in":
		for _, candidate := range values {
			if fmt.Sprint(actual) == fmt.Sprint(candidate) {
				return true
			}
		}
		return false
	case "gt", "gte", "lt", "lte":
		left, leftOK := numberValue(actual)
		right, rightOK := numberValue(value)
		if !leftOK || !rightOK {
			return false
		}
		switch operator {
		case "gt":
			return left > right
		case "gte":
			return left >= right
		case "lt":
			return left < right
		default:
			return left <= right
		}
	default:
		return false
	}
}

func recordID(record genericRecord, field string, index int) string {
	if value := strings.TrimSpace(fmt.Sprint(record[field])); value != "" && value != "<nil>" {
		return value
	}
	return fmt.Sprintf("record-%d", index+1)
}

func decisionForScore(score float64, thresholds map[string]float64) string {
	block := thresholds["block"]
	review := thresholds["review"]
	if block == 0 {
		block = 0.8
	}
	if review == 0 {
		review = 0.45
	}
	switch {
	case score >= block:
		return "block"
	case score >= review:
		return "review"
	default:
		return "approve"
	}
}

func resolveArtifactBasePath(requested string) (string, error) {
	root := strings.TrimSpace(os.Getenv("HELIOS_ARTIFACT_BASE_PATH"))
	if root == "" {
		root = defaultArtifactBasePath
	}
	absRoot, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return "", err
	}
	requested = strings.TrimSpace(requested)
	if requested == "" {
		return absRoot, nil
	}
	candidate := filepath.Clean(requested)
	if !filepath.IsAbs(candidate) {
		candidate = filepath.Join(absRoot, candidate)
	}
	absCandidate, err := filepath.Abs(candidate)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(absRoot, absCandidate)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("path must stay under %s", absRoot)
	}
	return absCandidate, nil
}

func writeLocalArtifact(basePath string, dataset string, artifactID string, body []byte, overwrite bool) (string, error) {
	if err := os.MkdirAll(basePath, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(basePath, fmt.Sprintf("%s-%s.json", safeName(dataset), artifactID))
	if !overwrite {
		if _, err := os.Stat(path); err == nil {
			return path, nil
		}
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", err
	}
	return path, nil
}

func safeName(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var builder strings.Builder
	for _, ch := range value {
		switch {
		case ch >= 'a' && ch <= 'z':
			builder.WriteRune(ch)
		case ch >= '0' && ch <= '9':
			builder.WriteRune(ch)
		case ch == '-' || ch == '_':
			builder.WriteRune(ch)
		default:
			builder.WriteRune('-')
		}
	}
	out := strings.Trim(builder.String(), "-")
	if out == "" {
		return "artifact"
	}
	return out
}

func round(value float64, places int) float64 {
	if places < 0 {
		places = 0
	}
	multiplier := math.Pow10(places)
	return math.Round(value*multiplier) / multiplier
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
