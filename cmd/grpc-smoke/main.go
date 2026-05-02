package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	heliosv1 "github.com/gauravgs7/helios/helios/v1"
	"github.com/gauravgs7/helios/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	defaultGRPCAddr = "localhost:8081"
	defaultTimeout  = 120 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "grpc_smoke=failed err=%v\n", err)
		os.Exit(1)
	}
	fmt.Println("grpc_smoke=passed")
}

func run() error {
	adminToken := strings.TrimSpace(os.Getenv("HELIOS_ADMIN_TOKEN"))
	if adminToken == "" {
		adminToken = "change-me-admin-token"
	}
	grpcAddr := strings.TrimSpace(os.Getenv("HELIOS_GRPC_ADDR"))
	if grpcAddr == "" {
		grpcAddr = defaultGRPCAddr
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("dial grpc: %w", err)
	}
	defer conn.Close()

	client := heliosv1.NewControlPlaneServiceClient(conn)
	adminCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+adminToken)

	if err := expectHealth(ctx, client); err != nil {
		return err
	}
	if err := expectReady(ctx, client); err != nil {
		return err
	}
	if err := expectUnauthorizedList(ctx, client); err != nil {
		return err
	}
	if err := expectWorkers(adminCtx, client); err != nil {
		return err
	}
	intentReq, err := loadPlannerIntentRequest("examples/intent_request.json")
	if err != nil {
		return err
	}
	if err := expectPlanner(adminCtx, client, intentReq); err != nil {
		return err
	}
	cancelSpec := cancelWorkflowSpec()
	cancelledWorkflowID, err := createWorkflow(adminCtx, client, cancelSpec)
	if err != nil {
		return err
	}
	if err := waitForTaskState(adminCtx, client, cancelledWorkflowID, "wait-or-active", 20*time.Second); err != nil {
		return err
	}
	if err := cancelWorkflow(adminCtx, client, cancelledWorkflowID); err != nil {
		return err
	}
	if err := waitForWorkflowState(adminCtx, client, cancelledWorkflowID, "cancelled", 30*time.Second); err != nil {
		return err
	}
	failedWorkflowID, err := createWorkflow(adminCtx, client, failedWorkflowSpec())
	if err != nil {
		return err
	}
	if err := waitForWorkflowState(adminCtx, client, failedWorkflowID, "failed", 30*time.Second); err != nil {
		return err
	}
	if err := expectFailureAnalysis(adminCtx, client, failedWorkflowID); err != nil {
		return err
	}
	successSpec, err := loadWorkflowSpec("examples/workflow.json")
	if err != nil {
		return err
	}
	successWorkflowID, err := createWorkflow(adminCtx, client, successSpec)
	if err != nil {
		return err
	}
	if err := waitForWorkflowState(adminCtx, client, successWorkflowID, "succeeded", 90*time.Second); err != nil {
		return err
	}
	if err := expectSuccessWorkflowArtifacts(adminCtx, client, successWorkflowID); err != nil {
		return err
	}
	return nil
}

func expectHealth(ctx context.Context, client heliosv1.ControlPlaneServiceClient) error {
	resp, err := client.GetHealth(ctx, &heliosv1.HealthRequest{})
	if err != nil {
		return fmt.Errorf("get health: %w", err)
	}
	if resp.GetStatus() != "ok" {
		return fmt.Errorf("unexpected health status %q", resp.GetStatus())
	}
	fmt.Println("grpc_health=ok")
	return nil
}

func expectReady(ctx context.Context, client heliosv1.ControlPlaneServiceClient) error {
	resp, err := client.GetReady(ctx, &heliosv1.ReadyRequest{})
	if err != nil {
		return fmt.Errorf("get ready: %w", err)
	}
	if resp.GetStatus() != "ok" {
		return fmt.Errorf("unexpected ready status %q checks=%v", resp.GetStatus(), resp.GetChecks())
	}
	fmt.Println("grpc_ready=ok")
	return nil
}

func expectUnauthorizedList(ctx context.Context, client heliosv1.ControlPlaneServiceClient) error {
	_, err := client.ListWorkflows(ctx, &heliosv1.ListWorkflowsRequest{Limit: 1})
	if status.Code(err) != codes.Unauthenticated {
		return fmt.Errorf("expected unauthenticated list workflows, got %v", err)
	}
	fmt.Println("grpc_auth=ok")
	return nil
}

func expectWorkers(ctx context.Context, client heliosv1.ControlPlaneServiceClient) error {
	resp, err := client.ListWorkers(ctx, &heliosv1.ListWorkersRequest{})
	if err != nil {
		return fmt.Errorf("list workers: %w", err)
	}
	if len(resp.GetWorkers()) == 0 {
		return errors.New("expected at least one worker")
	}
	healthy := 0
	for _, worker := range resp.GetWorkers() {
		if worker.GetHealth() == "healthy" {
			healthy++
		}
	}
	if healthy == 0 {
		return fmt.Errorf("expected healthy worker, got %+v", resp.GetWorkers())
	}
	fmt.Printf("grpc_workers=healthy:%d\n", healthy)
	return nil
}

func expectPlanner(ctx context.Context, client heliosv1.ControlPlaneServiceClient, req *heliosv1.PlannerIntentRequest) error {
	intentResp, err := client.PlannerIntent(ctx, req)
	if err != nil {
		return fmt.Errorf("planner intent: %w", err)
	}
	if intentResp.GetWorkflow() == nil || len(intentResp.GetWorkflow().GetTasks()) == 0 {
		return errors.New("planner intent returned empty workflow")
	}
	dryRunResp, err := client.PlannerDryRun(ctx, req)
	if err != nil {
		return fmt.Errorf("planner dry-run: %w", err)
	}
	if !dryRunResp.GetValid() {
		return fmt.Errorf("planner dry-run invalid: %v", dryRunResp.GetValidationErrors())
	}
	if dryRunResp.GetAnalysis() == nil || dryRunResp.GetAnalysis().GetTaskCount() == 0 {
		return errors.New("planner dry-run analysis missing")
	}
	fmt.Printf("grpc_planner=tasks:%d\n", len(intentResp.GetWorkflow().GetTasks()))
	return nil
}

func cancelWorkflow(ctx context.Context, client heliosv1.ControlPlaneServiceClient, workflowID string) error {
	resp, err := client.CancelWorkflow(ctx, &heliosv1.CancelWorkflowRequest{WorkflowId: workflowID})
	if err != nil {
		return fmt.Errorf("cancel workflow %s: %w", workflowID, err)
	}
	if resp.GetStatus() != "cancelled" {
		return fmt.Errorf("unexpected cancel status %q", resp.GetStatus())
	}
	fmt.Printf("grpc_cancel=submitted workflow_id=%s\n", workflowID)
	return nil
}

func expectFailureAnalysis(ctx context.Context, client heliosv1.ControlPlaneServiceClient, workflowID string) error {
	resp, err := client.AnalyzeWorkflowFailure(ctx, &heliosv1.AnalyzeWorkflowFailureRequest{
		WorkflowId:         workflowID,
		RunbookQuery:       "model inference retry failure",
		IncludeRawSnapshot: true,
	})
	if err != nil {
		return fmt.Errorf("analyze workflow failure: %w", err)
	}
	if strings.TrimSpace(resp.GetSummary()) == "" {
		return errors.New("failure analysis summary is empty")
	}
	fmt.Printf("grpc_failure_analysis=ok workflow_id=%s backend=%s\n", workflowID, resp.GetBackend())
	return nil
}

func expectSuccessWorkflowArtifacts(ctx context.Context, client heliosv1.ControlPlaneServiceClient, workflowID string) error {
	workflow, err := client.GetWorkflow(ctx, &heliosv1.GetWorkflowRequest{WorkflowId: workflowID})
	if err != nil {
		return fmt.Errorf("get workflow %s: %w", workflowID, err)
	}
	if workflow.GetState() != "succeeded" {
		return fmt.Errorf("expected succeeded workflow, got %s", workflow.GetState())
	}
	tasksResp, err := client.ListWorkflowTasks(ctx, &heliosv1.ListWorkflowTasksRequest{WorkflowId: workflowID})
	if err != nil {
		return fmt.Errorf("list workflow tasks %s: %w", workflowID, err)
	}
	if len(tasksResp.GetTasks()) == 0 {
		return errors.New("success workflow returned no tasks")
	}
	eventsResp, err := client.ListWorkflowEvents(ctx, &heliosv1.ListWorkflowEventsRequest{WorkflowId: workflowID})
	if err != nil {
		return fmt.Errorf("list workflow events %s: %w", workflowID, err)
	}
	if len(eventsResp.GetEvents()) == 0 {
		return errors.New("success workflow returned no events")
	}
	taskByID := make(map[string]*heliosv1.TaskRecord, len(tasksResp.GetTasks()))
	sawRetryFailure := false
	sawSuccess := false
	for _, task := range tasksResp.GetTasks() {
		taskByID[task.GetTaskId()] = task
		if task.GetTaskId() == "run-risk-inference" {
			if task.GetAttempt() < 2 {
				return fmt.Errorf("expected retry attempt on run-risk-inference, got %d", task.GetAttempt())
			}
			if len(task.GetOutputPayload()) == 0 {
				return errors.New("run-risk-inference missing output payload")
			}
		}
	}
	for _, event := range eventsResp.GetEvents() {
		if event.GetTaskId() == "run-risk-inference" && (event.GetNewState() == "failed" || event.GetNewState() == "retry_wait") {
			sawRetryFailure = true
		}
		if event.GetTaskId() == "write-risk-artifact" && event.GetNewState() == "succeeded" {
			sawSuccess = true
		}
	}
	if !sawRetryFailure {
		return errors.New("expected failed retry event for run-risk-inference")
	}
	if !sawSuccess {
		return errors.New("expected write-risk-artifact success event")
	}
	persistTask := taskByID["write-risk-artifact"]
	if persistTask == nil {
		return errors.New("write-risk-artifact task not found")
	}
	getTaskResp, err := client.GetTask(ctx, &heliosv1.GetTaskRequest{TaskId: persistTask.GetTaskId()})
	if err != nil {
		return fmt.Errorf("get task %s: %w", persistTask.GetTaskId(), err)
	}
	if getTaskResp.GetState() != "succeeded" {
		return fmt.Errorf("write-risk-artifact task state = %s", getTaskResp.GetState())
	}
	fmt.Printf("grpc_success=ok workflow_id=%s tasks=%d events=%d\n", workflowID, len(tasksResp.GetTasks()), len(eventsResp.GetEvents()))
	return nil
}

func createWorkflow(ctx context.Context, client heliosv1.ControlPlaneServiceClient, spec *heliosv1.WorkflowSpec) (string, error) {
	resp, err := client.CreateWorkflow(ctx, &heliosv1.CreateWorkflowRequest{Workflow: spec})
	if err != nil {
		return "", fmt.Errorf("create workflow %s: %w", spec.GetName(), err)
	}
	if strings.TrimSpace(resp.GetWorkflowId()) == "" {
		return "", fmt.Errorf("workflow %s returned empty id", spec.GetName())
	}
	fmt.Printf("grpc_submit=ok name=%s workflow_id=%s\n", spec.GetName(), resp.GetWorkflowId())
	return resp.GetWorkflowId(), nil
}

func waitForWorkflowState(ctx context.Context, client heliosv1.ControlPlaneServiceClient, workflowID, expected string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.GetWorkflow(ctx, &heliosv1.GetWorkflowRequest{WorkflowId: workflowID})
		if err != nil {
			return fmt.Errorf("poll workflow %s: %w", workflowID, err)
		}
		if resp.GetState() == expected {
			fmt.Printf("grpc_workflow_state=%s workflow_id=%s\n", expected, workflowID)
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("workflow %s did not reach %s before timeout", workflowID, expected)
}

func waitForTaskState(ctx context.Context, client heliosv1.ControlPlaneServiceClient, workflowID, mode string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.ListWorkflowTasks(ctx, &heliosv1.ListWorkflowTasksRequest{WorkflowId: workflowID})
		if err != nil {
			return fmt.Errorf("list workflow tasks %s: %w", workflowID, err)
		}
		for _, task := range resp.GetTasks() {
			switch mode {
			case "wait-or-active":
				if task.GetState() == "retry_wait" || task.GetState() == "running" || task.GetState() == "leased" {
					fmt.Printf("grpc_task_state=%s workflow_id=%s task_id=%s\n", task.GetState(), workflowID, task.GetTaskId())
					return nil
				}
			}
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("workflow %s did not reach task mode %s before timeout", workflowID, mode)
}

func loadPlannerIntentRequest(path string) (*heliosv1.PlannerIntentRequest, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read planner intent %s: %w", path, err)
	}
	var req struct {
		Name           string             `json:"name"`
		Intent         string             `json:"intent"`
		Stages         []string           `json:"stages"`
		TimeoutSeconds int                `json:"timeout_seconds"`
		RetryPolicy    domain.RetryPolicy `json:"retry_policy"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("decode planner intent %s: %w", path, err)
	}
	return &heliosv1.PlannerIntentRequest{
		Name:           req.Name,
		Intent:         req.Intent,
		Stages:         append([]string(nil), req.Stages...),
		TimeoutSeconds: int32(req.TimeoutSeconds),
		RetryPolicy:    retryPolicyToProto(req.RetryPolicy),
	}, nil
}

func loadWorkflowSpec(path string) (*heliosv1.WorkflowSpec, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read workflow spec %s: %w", path, err)
	}
	var spec domain.WorkflowSpec
	if err := json.Unmarshal(body, &spec); err != nil {
		return nil, fmt.Errorf("decode workflow spec %s: %w", path, err)
	}
	return workflowSpecToProto(spec), nil
}

func cancelWorkflowSpec() *heliosv1.WorkflowSpec {
	return &heliosv1.WorkflowSpec{
		Name: "grpc-cancel-flow",
		Tasks: []*heliosv1.TaskSpec{
			{
				TaskId:         "cancel-after-retry",
				TaskType:       "model_inference",
				InputPayload:   inferencePayload(5, true),
				TimeoutSeconds: 10,
				RetryPolicy: &heliosv1.RetryPolicy{
					MaxAttempts:           6,
					InitialBackoffSeconds: 5,
					MaxBackoffSeconds:     20,
					Multiplier:            2,
				},
				IdempotencyKey: "grpc-cancel-flow-v1",
			},
		},
	}
}

func failedWorkflowSpec() *heliosv1.WorkflowSpec {
	return &heliosv1.WorkflowSpec{
		Name: "grpc-failure-analysis-flow",
		Tasks: []*heliosv1.TaskSpec{
			{
				TaskId:         "terminal-failure",
				TaskType:       "model_inference",
				InputPayload:   inferencePayload(1, false),
				TimeoutSeconds: 10,
				RetryPolicy: &heliosv1.RetryPolicy{
					MaxAttempts:           1,
					InitialBackoffSeconds: 1,
					MaxBackoffSeconds:     1,
					Multiplier:            1,
				},
				IdempotencyKey: "grpc-failure-analysis-v1",
			},
		},
	}
}

func workflowSpecToProto(spec domain.WorkflowSpec) *heliosv1.WorkflowSpec {
	out := &heliosv1.WorkflowSpec{
		Name:     spec.Name,
		Labels:   cloneStringMap(spec.Labels),
		Metadata: cloneStringMap(spec.Metadata),
		Tasks:    make([]*heliosv1.TaskSpec, 0, len(spec.Tasks)),
	}
	for _, task := range spec.Tasks {
		out.Tasks = append(out.Tasks, &heliosv1.TaskSpec{
			TaskId:                  task.TaskID,
			TaskType:                task.TaskType,
			Dependencies:            append([]string(nil), task.Dependencies...),
			InputPayload:            append([]byte(nil), task.InputPayload...),
			TimeoutSeconds:          int32(task.TimeoutSeconds),
			RetryPolicy:             retryPolicyToProto(task.RetryPolicy),
			Labels:                  cloneStringMap(task.Labels),
			Metadata:                cloneStringMap(task.Metadata),
			IdempotencyKey:          task.IdempotencyKey,
			Priority:                int32(task.Priority),
			CpuUnits:                int32(task.CPUUnits),
			MemoryMb:                int32(task.MemoryMB),
			ExpectedDurationSeconds: int32(task.ExpectedDurationSeconds),
		})
	}
	return out
}

func retryPolicyToProto(policy domain.RetryPolicy) *heliosv1.RetryPolicy {
	return &heliosv1.RetryPolicy{
		MaxAttempts:           int32(policy.MaxAttempts),
		InitialBackoffSeconds: int32(policy.InitialBackoffSeconds),
		MaxBackoffSeconds:     int32(policy.MaxBackoffSeconds),
		Multiplier:            policy.Multiplier,
	}
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func mustJSON(value any) []byte {
	body, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return body
}

func inferencePayload(failUntilAttempt int, retryable bool) []byte {
	return mustJSON(map[string]any{
		"model_name":          "smoke-risk-rules-v1",
		"fail_until_attempt":  failUntilAttempt,
		"retryable_failure":   retryable,
		"records":             []map[string]any{{"id": "smoke-1", "amount": 100}},
		"rules":               []map[string]any{{"field": "amount", "operator": "gte", "value": 50, "score": 0.5, "contributor": "amount_threshold"}},
		"decision_thresholds": map[string]any{"review": 0.45, "block": 0.8},
	})
}
