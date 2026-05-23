package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	heliosv1 "github.com/gaurav-gs7/helios/helios/v1"
	"github.com/gaurav-gs7/helios/internal/dag"
	"github.com/gaurav-gs7/helios/internal/domain"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type grpcService struct {
	heliosv1.UnimplementedControlPlaneServiceServer
	server *Server
}

type plannerFailureAnalysisResponse struct {
	Summary          string                `json:"summary"`
	FailureClass     string                `json:"failure_class"`
	LikelyRootCauses []string              `json:"likely_root_causes"`
	RecoveryActions  []string              `json:"recovery_actions"`
	AffectedTasks    []string              `json:"affected_tasks"`
	RunbookMatches   []plannerRunbookMatch `json:"runbook_matches"`
	Confidence       float64               `json:"confidence"`
	Backend          string                `json:"backend"`
	GeneratedAt      time.Time             `json:"generated_at"`
	RawSnapshot      map[string]any        `json:"raw_snapshot"`
}

type plannerRunbookMatch struct {
	Title   string  `json:"title"`
	Path    string  `json:"path"`
	Score   float64 `json:"score"`
	Excerpt string  `json:"excerpt"`
}

func (s *Server) NewGRPCServer() *grpc.Server {
	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(s.grpcUnaryInterceptor()))
	heliosv1.RegisterControlPlaneServiceServer(grpcServer, &grpcService{server: s})
	reflection.Register(grpcServer)
	return grpcServer
}

func (s *Server) grpcUnaryInterceptor() grpc.UnaryServerInterceptor {
	exempt := map[string]struct{}{
		heliosv1.ControlPlaneService_GetHealth_FullMethodName: {},
		heliosv1.ControlPlaneService_GetReady_FullMethodName:  {},
	}
	bootstrap := map[string]struct{}{
		heliosv1.ControlPlaneService_RegisterWorker_FullMethodName: {},
	}
	workerMethods := map[string]struct{}{
		heliosv1.ControlPlaneService_HeartbeatWorker_FullMethodName:       {},
		heliosv1.ControlPlaneService_StartTaskExecution_FullMethodName:    {},
		heliosv1.ControlPlaneService_CompleteTaskExecution_FullMethodName: {},
		heliosv1.ControlPlaneService_FailTaskExecution_FullMethodName:     {},
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		requestID := grpcRequestID(ctx)
		ctx = context.WithValue(ctx, requestIDKey, requestID)
		start := time.Now()

		switch {
		case hasGRPCMethod(exempt, info.FullMethod):
		case hasGRPCMethod(bootstrap, info.FullMethod):
			if err := requireGRPCBearerToken(ctx, s.bootstrapToken); err != nil {
				s.logGRPCRequest(ctx, info.FullMethod, grpcRemoteAddr(ctx), start, status.Code(err))
				return nil, err
			}
		case hasGRPCMethod(workerMethods, info.FullMethod):
			if err := authenticateGRPCWorker(ctx, s.store, req); err != nil {
				s.logGRPCRequest(ctx, info.FullMethod, grpcRemoteAddr(ctx), start, status.Code(err))
				return nil, err
			}
		default:
			if err := requireGRPCAdminToken(ctx, s.adminToken); err != nil {
				s.logGRPCRequest(ctx, info.FullMethod, grpcRemoteAddr(ctx), start, status.Code(err))
				return nil, err
			}
		}

		resp, err := handler(ctx, req)
		s.logGRPCRequest(ctx, info.FullMethod, grpcRemoteAddr(ctx), start, status.Code(err))
		return resp, err
	}
}

func (s *Server) logGRPCRequest(ctx context.Context, method, remote string, start time.Time, code codes.Code) {
	if s.logger == nil {
		return
	}
	s.logger.Info("grpc request",
		"method", method,
		"code", code.String(),
		"duration_ms", time.Since(start).Milliseconds(),
		"remote_addr", remote,
		"request_id", requestIDFromContext(ctx),
	)
}

func hasGRPCMethod(methods map[string]struct{}, method string) bool {
	_, ok := methods[method]
	return ok
}

func requireGRPCAdminToken(ctx context.Context, expected string) error {
	return requireGRPCBearerToken(ctx, expected)
}

func requireGRPCBearerToken(ctx context.Context, expected string) error {
	if expected == "" {
		return status.Error(codes.Unauthenticated, "unauthorized")
	}
	token := grpcBearerToken(ctx)
	if subtle.ConstantTimeCompare([]byte(token), []byte(expected)) != 1 {
		return status.Error(codes.Unauthenticated, "unauthorized")
	}
	return nil
}

func authenticateGRPCWorker(ctx context.Context, auth workerAuthenticator, req any) error {
	workerID := grpcWorkerID(req)
	if workerID == "" {
		return status.Error(codes.Unauthenticated, "worker id is required")
	}
	token := grpcBearerToken(ctx)
	if err := auth.AuthenticateWorker(ctx, workerID, token); err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	return nil
}

func grpcBearerToken(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get("authorization")
	if len(values) == 0 {
		return ""
	}
	return bearerTokenFromHeader(values[0])
}

func grpcWorkerID(req any) string {
	switch typed := req.(type) {
	case *heliosv1.HeartbeatWorkerRequest:
		return typed.GetWorkerId()
	case *heliosv1.StartTaskExecutionRequest:
		return typed.GetWorkerId()
	case *heliosv1.CompleteTaskExecutionRequest:
		return typed.GetWorkerId()
	case *heliosv1.FailTaskExecutionRequest:
		return typed.GetWorkerId()
	default:
		return ""
	}
}

func grpcRemoteAddr(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return ""
	}
	return p.Addr.String()
}

func grpcRequestID(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		values := md.Get("x-request-id")
		if len(values) > 0 && strings.TrimSpace(values[0]) != "" {
			return strings.TrimSpace(values[0])
		}
	}
	return uuid.NewString()
}

func (g *grpcService) GetHealth(_ context.Context, _ *heliosv1.HealthRequest) (*heliosv1.HealthResponse, error) {
	return &heliosv1.HealthResponse{
		Status:    "ok",
		Timestamp: timestamppb.New(time.Now()),
		Version:   g.server.version,
	}, nil
}

func (g *grpcService) GetReady(ctx context.Context, _ *heliosv1.ReadyRequest) (*heliosv1.ReadyResponse, error) {
	checks := map[string]string{
		"postgres": "ok",
	}
	if err := g.server.store.Ping(ctx); err != nil {
		checks["postgres"] = err.Error()
		return &heliosv1.ReadyResponse{
			Status:    "degraded",
			Timestamp: timestamppb.New(time.Now()),
			Checks:    checks,
		}, nil
	}
	if g.server.plannerURL != "" {
		if err := WaitForPlanner(ctx, g.server.plannerURL); err != nil {
			checks["planner"] = err.Error()
			return &heliosv1.ReadyResponse{
				Status:    "degraded",
				Timestamp: timestamppb.New(time.Now()),
				Checks:    checks,
			}, nil
		}
		checks["planner"] = "ok"
	}
	return &heliosv1.ReadyResponse{
		Status:    "ok",
		Timestamp: timestamppb.New(time.Now()),
		Checks:    checks,
	}, nil
}

func (g *grpcService) RegisterWorker(ctx context.Context, req *heliosv1.WorkerRegistration) (*heliosv1.WorkerRegistrationResult, error) {
	registration, err := g.server.store.RegisterWorker(ctx, workerRegistrationFromProto(req))
	if err != nil {
		return nil, grpcError(err)
	}
	return workerRegistrationResultToProto(registration), nil
}

func (g *grpcService) HeartbeatWorker(ctx context.Context, req *heliosv1.HeartbeatWorkerRequest) (*heliosv1.StatusResponse, error) {
	if strings.TrimSpace(req.GetWorkerId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}
	if err := g.server.store.HeartbeatWorker(ctx, req.GetWorkerId(), workerHeartbeatFromProto(req.GetHeartbeat())); err != nil {
		return nil, grpcError(err)
	}
	return &heliosv1.StatusResponse{Status: "heartbeat-recorded"}, nil
}

func (g *grpcService) ListWorkers(ctx context.Context, _ *heliosv1.ListWorkersRequest) (*heliosv1.WorkerListResponse, error) {
	workers, err := g.server.store.ListWorkers(ctx)
	if err != nil {
		return nil, grpcError(err)
	}
	out := &heliosv1.WorkerListResponse{Workers: make([]*heliosv1.WorkerSnapshot, 0, len(workers))}
	for _, worker := range workers {
		out.Workers = append(out.Workers, workerSnapshotToProto(worker))
	}
	return out, nil
}

func (g *grpcService) StartTaskExecution(ctx context.Context, req *heliosv1.StartTaskExecutionRequest) (*heliosv1.StatusResponse, error) {
	if err := g.server.store.StartTask(ctx, req.GetWorkflowId(), req.GetTaskId(), domain.StartTaskRequest{
		WorkerID:   req.GetWorkerId(),
		AttemptID:  req.GetAttemptId(),
		StartedAt:  req.GetStartedAt(),
		TraceID:    req.GetTraceId(),
		ReasonHint: req.GetReasonHint(),
	}); err != nil {
		return nil, grpcError(err)
	}
	return &heliosv1.StatusResponse{Status: "running"}, nil
}

func (g *grpcService) CompleteTaskExecution(ctx context.Context, req *heliosv1.CompleteTaskExecutionRequest) (*heliosv1.StatusResponse, error) {
	if err := g.server.store.CompleteTask(ctx, req.GetWorkflowId(), req.GetTaskId(), domain.CompleteTaskRequest{
		WorkerID:      req.GetWorkerId(),
		AttemptID:     req.GetAttemptId(),
		OutputPayload: append([]byte(nil), req.GetOutputPayload()...),
		TraceID:       req.GetTraceId(),
	}); err != nil {
		return nil, grpcError(err)
	}
	return &heliosv1.StatusResponse{Status: "succeeded"}, nil
}

func (g *grpcService) FailTaskExecution(ctx context.Context, req *heliosv1.FailTaskExecutionRequest) (*heliosv1.StatusResponse, error) {
	if err := g.server.store.FailTask(ctx, req.GetWorkflowId(), req.GetTaskId(), domain.FailTaskRequest{
		WorkerID:  req.GetWorkerId(),
		AttemptID: req.GetAttemptId(),
		Error:     req.GetError(),
		Retryable: req.GetRetryable(),
		TraceID:   req.GetTraceId(),
	}); err != nil {
		return nil, grpcError(err)
	}
	return &heliosv1.StatusResponse{Status: "recorded"}, nil
}

func (g *grpcService) CreateWorkflow(ctx context.Context, req *heliosv1.CreateWorkflowRequest) (*heliosv1.WorkflowSummary, error) {
	if req.GetWorkflow() == nil {
		return nil, status.Error(codes.InvalidArgument, "workflow is required")
	}
	if !g.server.submissions.allow(clientIP(grpcRemoteAddr(ctx)), time.Now().UTC()) {
		return nil, status.Error(codes.ResourceExhausted, "submission rate limit exceeded")
	}
	spec := workflowSpecFromProto(req.GetWorkflow())
	if err := dag.Validate(spec); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	summary, err := g.server.store.CreateWorkflow(ctx, spec)
	if err != nil {
		return nil, grpcError(err)
	}
	return workflowSummaryToProto(summary), nil
}

func (g *grpcService) ListWorkflows(ctx context.Context, req *heliosv1.ListWorkflowsRequest) (*heliosv1.WorkflowListResponse, error) {
	limit := 50
	if req.GetLimit() > 0 {
		limit = int(req.GetLimit())
	}
	workflows, err := g.server.store.ListWorkflows(ctx, strings.TrimSpace(req.GetState()), limit)
	if err != nil {
		return nil, grpcError(err)
	}
	out := &heliosv1.WorkflowListResponse{Workflows: make([]*heliosv1.WorkflowSummary, 0, len(workflows))}
	for _, workflow := range workflows {
		out.Workflows = append(out.Workflows, workflowSummaryToProto(workflow))
	}
	return out, nil
}

func (g *grpcService) GetWorkflow(ctx context.Context, req *heliosv1.GetWorkflowRequest) (*heliosv1.WorkflowSummary, error) {
	summary, err := g.server.store.GetWorkflow(ctx, req.GetWorkflowId())
	if err != nil {
		return nil, grpcError(err)
	}
	return workflowSummaryToProto(summary), nil
}

func (g *grpcService) ListWorkflowTasks(ctx context.Context, req *heliosv1.ListWorkflowTasksRequest) (*heliosv1.TaskListResponse, error) {
	tasks, err := g.server.store.ListWorkflowTasks(ctx, req.GetWorkflowId())
	if err != nil {
		return nil, grpcError(err)
	}
	out := &heliosv1.TaskListResponse{
		WorkflowId: req.GetWorkflowId(),
		Tasks:      make([]*heliosv1.TaskRecord, 0, len(tasks)),
	}
	for _, task := range tasks {
		out.Tasks = append(out.Tasks, taskRecordToProto(task))
	}
	return out, nil
}

func (g *grpcService) ListWorkflowEvents(ctx context.Context, req *heliosv1.ListWorkflowEventsRequest) (*heliosv1.WorkflowEventListResponse, error) {
	events, err := g.server.store.ListWorkflowEvents(ctx, req.GetWorkflowId(), 200)
	if err != nil {
		return nil, grpcError(err)
	}
	out := &heliosv1.WorkflowEventListResponse{
		WorkflowId: req.GetWorkflowId(),
		Events:     make([]*heliosv1.TaskEvent, 0, len(events)),
	}
	for _, event := range events {
		out.Events = append(out.Events, taskEventToProto(event))
	}
	return out, nil
}

func (g *grpcService) GetTask(ctx context.Context, req *heliosv1.GetTaskRequest) (*heliosv1.TaskRecord, error) {
	task, err := g.server.store.GetTask(ctx, req.GetTaskId())
	if err != nil {
		return nil, grpcError(err)
	}
	return taskRecordToProto(task), nil
}

func (g *grpcService) CancelWorkflow(ctx context.Context, req *heliosv1.CancelWorkflowRequest) (*heliosv1.CancelWorkflowResponse, error) {
	if err := g.server.store.CancelWorkflow(ctx, req.GetWorkflowId(), "grpc"); err != nil {
		return nil, grpcError(err)
	}
	return &heliosv1.CancelWorkflowResponse{Status: "cancelled"}, nil
}

func (g *grpcService) PlannerIntent(ctx context.Context, req *heliosv1.PlannerIntentRequest) (*heliosv1.PlannerIntentResponse, error) {
	if g.server.plannerURL == "" {
		return nil, status.Error(codes.FailedPrecondition, "planner service is not configured")
	}
	payload, err := json.Marshal(map[string]any{
		"name":            req.GetName(),
		"intent":          req.GetIntent(),
		"stages":          nonNilStrings(req.GetStages()),
		"timeout_seconds": int(req.GetTimeoutSeconds()),
		"retry_policy": map[string]any{
			"max_attempts":            int(req.GetRetryPolicy().GetMaxAttempts()),
			"initial_backoff_seconds": int(req.GetRetryPolicy().GetInitialBackoffSeconds()),
			"max_backoff_seconds":     int(req.GetRetryPolicy().GetMaxBackoffSeconds()),
			"multiplier":              req.GetRetryPolicy().GetMultiplier(),
		},
	})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	respBody, statusCode, err := g.server.postPlanner(ctx, "/v1/plan/intent", payload)
	if err != nil {
		return nil, grpcError(err)
	}
	if statusCode >= http.StatusMultipleChoices {
		return nil, grpcErrorFromHTTPStatus(statusCode, respBody)
	}
	var planned plannerIntentEnvelope
	if err := json.Unmarshal(respBody, &planned); err != nil {
		return nil, status.Errorf(codes.Internal, "decode planner response: %v", err)
	}
	hints, err := structpb.NewStruct(planned.SchedulerHints)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode scheduler hints: %v", err)
	}
	return &heliosv1.PlannerIntentResponse{
		Workflow:       workflowSpecToProto(planned.Workflow),
		PlanningNotes:  append([]string(nil), planned.PlanningNotes...),
		SchedulerHints: hints,
		CreatedAt:      timestamppb.New(planned.CreatedAt),
	}, nil
}

func (g *grpcService) PlannerDryRun(ctx context.Context, req *heliosv1.PlannerIntentRequest) (*heliosv1.PlannerDryRunResponse, error) {
	if g.server.plannerURL == "" {
		return nil, status.Error(codes.FailedPrecondition, "planner service is not configured")
	}
	payload, err := json.Marshal(map[string]any{
		"name":            req.GetName(),
		"intent":          req.GetIntent(),
		"stages":          nonNilStrings(req.GetStages()),
		"timeout_seconds": int(req.GetTimeoutSeconds()),
		"retry_policy": map[string]any{
			"max_attempts":            int(req.GetRetryPolicy().GetMaxAttempts()),
			"initial_backoff_seconds": int(req.GetRetryPolicy().GetInitialBackoffSeconds()),
			"max_backoff_seconds":     int(req.GetRetryPolicy().GetMaxBackoffSeconds()),
			"multiplier":              req.GetRetryPolicy().GetMultiplier(),
		},
	})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	respBody, statusCode, err := g.server.postPlanner(ctx, "/v1/plan/intent", payload)
	if err != nil {
		return nil, grpcError(err)
	}
	if statusCode >= http.StatusMultipleChoices {
		return nil, grpcErrorFromHTTPStatus(statusCode, respBody)
	}
	var planned plannerIntentEnvelope
	if err := json.Unmarshal(respBody, &planned); err != nil {
		return nil, status.Errorf(codes.Internal, "decode planner response: %v", err)
	}
	analysis := analyzeWorkflowSpec(planned.Workflow)
	hints, err := structpb.NewStruct(planned.SchedulerHints)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode scheduler hints: %v", err)
	}
	return &heliosv1.PlannerDryRunResponse{
		Valid:            dag.Validate(planned.Workflow) == nil,
		Workflow:         workflowSpecToProto(planned.Workflow),
		PlanningNotes:    append([]string(nil), planned.PlanningNotes...),
		SchedulerHints:   hints,
		Analysis:         dryRunAnalysisToProto(analysis),
		CreatedAt:        timestamppb.New(time.Now()),
		ValidationErrors: validationErrorsForWorkflow(planned.Workflow),
	}, nil
}

func (g *grpcService) AnalyzeWorkflowFailure(ctx context.Context, req *heliosv1.AnalyzeWorkflowFailureRequest) (*heliosv1.FailureAnalysisResponse, error) {
	if g.server.plannerURL == "" {
		return nil, status.Error(codes.FailedPrecondition, "planner service is not configured")
	}
	details, err := g.server.store.GetWorkflowDetails(ctx, req.GetWorkflowId())
	if err != nil {
		return nil, grpcError(err)
	}
	payload, err := json.Marshal(map[string]any{
		"workflow":             details,
		"runbook_query":        req.GetRunbookQuery(),
		"include_raw_snapshot": req.GetIncludeRawSnapshot(),
	})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	respBody, statusCode, err := g.server.postPlanner(ctx, "/v1/analyze/failure", payload)
	if err != nil {
		return nil, grpcError(err)
	}
	if statusCode >= http.StatusMultipleChoices {
		return nil, grpcErrorFromHTTPStatus(statusCode, respBody)
	}
	var analysis plannerFailureAnalysisResponse
	if err := json.Unmarshal(respBody, &analysis); err != nil {
		return nil, status.Errorf(codes.Internal, "decode failure analysis: %v", err)
	}
	return failureAnalysisToProto(analysis)
}

func workflowSpecFromProto(in *heliosv1.WorkflowSpec) domain.WorkflowSpec {
	if in == nil {
		return domain.WorkflowSpec{}
	}
	out := domain.WorkflowSpec{
		Name:     in.GetName(),
		Labels:   cloneStringMap(in.GetLabels()),
		Metadata: cloneStringMap(in.GetMetadata()),
		Tasks:    make([]domain.TaskSpec, 0, len(in.GetTasks())),
	}
	for _, task := range in.GetTasks() {
		out.Tasks = append(out.Tasks, taskSpecFromProto(task))
	}
	return out
}

func taskSpecFromProto(in *heliosv1.TaskSpec) domain.TaskSpec {
	if in == nil {
		return domain.TaskSpec{}
	}
	return domain.TaskSpec{
		TaskID:                  in.GetTaskId(),
		TaskType:                in.GetTaskType(),
		Dependencies:            append([]string(nil), in.GetDependencies()...),
		InputPayload:            append([]byte(nil), in.GetInputPayload()...),
		TimeoutSeconds:          int(in.GetTimeoutSeconds()),
		RetryPolicy:             retryPolicyFromProto(in.GetRetryPolicy()),
		Labels:                  cloneStringMap(in.GetLabels()),
		Metadata:                cloneStringMap(in.GetMetadata()),
		IdempotencyKey:          in.GetIdempotencyKey(),
		Priority:                int(in.GetPriority()),
		CPUUnits:                int(in.GetCpuUnits()),
		MemoryMB:                int(in.GetMemoryMb()),
		ExpectedDurationSeconds: int(in.GetExpectedDurationSeconds()),
	}
}

func retryPolicyFromProto(in *heliosv1.RetryPolicy) domain.RetryPolicy {
	if in == nil {
		return domain.RetryPolicy{}
	}
	return domain.RetryPolicy{
		MaxAttempts:           int(in.GetMaxAttempts()),
		InitialBackoffSeconds: int(in.GetInitialBackoffSeconds()),
		MaxBackoffSeconds:     int(in.GetMaxBackoffSeconds()),
		Multiplier:            in.GetMultiplier(),
	}
}

func workflowSpecToProto(in domain.WorkflowSpec) *heliosv1.WorkflowSpec {
	out := &heliosv1.WorkflowSpec{
		Name:     in.Name,
		Labels:   cloneStringMap(in.Labels),
		Metadata: cloneStringMap(in.Metadata),
		Tasks:    make([]*heliosv1.TaskSpec, 0, len(in.Tasks)),
	}
	for _, task := range in.Tasks {
		out.Tasks = append(out.Tasks, taskSpecToProto(task))
	}
	return out
}

func taskSpecToProto(in domain.TaskSpec) *heliosv1.TaskSpec {
	return &heliosv1.TaskSpec{
		TaskId:                  in.TaskID,
		TaskType:                in.TaskType,
		Dependencies:            append([]string(nil), in.Dependencies...),
		InputPayload:            append([]byte(nil), in.InputPayload...),
		TimeoutSeconds:          int32(in.TimeoutSeconds),
		RetryPolicy:             retryPolicyToProto(in.RetryPolicy),
		Labels:                  cloneStringMap(in.Labels),
		Metadata:                cloneStringMap(in.Metadata),
		IdempotencyKey:          in.IdempotencyKey,
		Priority:                int32(in.Priority),
		CpuUnits:                int32(in.CPUUnits),
		MemoryMb:                int32(in.MemoryMB),
		ExpectedDurationSeconds: int32(in.ExpectedDurationSeconds),
	}
}

func retryPolicyToProto(in domain.RetryPolicy) *heliosv1.RetryPolicy {
	return &heliosv1.RetryPolicy{
		MaxAttempts:           int32(in.MaxAttempts),
		InitialBackoffSeconds: int32(in.InitialBackoffSeconds),
		MaxBackoffSeconds:     int32(in.MaxBackoffSeconds),
		Multiplier:            in.Multiplier,
	}
}

func workflowSummaryToProto(in domain.WorkflowSummary) *heliosv1.WorkflowSummary {
	return &heliosv1.WorkflowSummary{
		WorkflowId: in.WorkflowID,
		Name:       in.Name,
		State:      string(in.State),
		CreatedAt:  ts(in.CreatedAt),
		UpdatedAt:  ts(in.UpdatedAt),
		Labels:     cloneStringMap(in.Labels),
		Metadata:   cloneStringMap(in.Metadata),
	}
}

func taskRecordToProto(in domain.TaskRecord) *heliosv1.TaskRecord {
	return &heliosv1.TaskRecord{
		TaskId:                  in.TaskID,
		WorkflowId:              in.WorkflowID,
		TaskType:                in.TaskType,
		State:                   string(in.State),
		Dependencies:            append([]string(nil), in.Dependencies...),
		Attempt:                 int32(in.Attempt),
		MaxAttempts:             int32(in.MaxAttempts),
		AssignedWorker:          in.AssignedWorker,
		TimeoutSeconds:          int32(in.TimeoutSeconds),
		LeaseExpiresAt:          tsp(in.LeaseExpiresAt),
		NextRunAt:               tsp(in.NextRunAt),
		LastError:               in.LastError,
		IdempotencyKey:          in.IdempotencyKey,
		InputPayload:            append([]byte(nil), in.InputPayload...),
		OutputPayload:           append([]byte(nil), in.OutputPayload...),
		RetryPolicy:             retryPolicyToProto(in.RetryPolicy),
		Labels:                  cloneStringMap(in.Labels),
		Metadata:                cloneStringMap(in.Metadata),
		CurrentAttempt:          in.CurrentAttempt,
		CreatedAt:               ts(in.CreatedAt),
		UpdatedAt:               ts(in.UpdatedAt),
		CompletedAt:             tsp(in.CompletedAt),
		Priority:                int32(in.Priority),
		CpuUnits:                int32(in.CPUUnits),
		MemoryMb:                int32(in.MemoryMB),
		ExpectedDurationSeconds: int32(in.ExpectedDurationSeconds),
		DependencyCount:         int32(in.DependencyCount),
	}
}

func taskEventToProto(in domain.TaskEvent) *heliosv1.TaskEvent {
	return &heliosv1.TaskEvent{
		EventId:     in.EventID,
		WorkflowId:  in.WorkflowID,
		TaskId:      in.TaskID,
		AttemptId:   in.AttemptID,
		Actor:       in.Actor,
		OldState:    in.OldState,
		NewState:    in.NewState,
		Reason:      in.Reason,
		OccurredAt:  ts(in.OccurredAt),
		Metadata:    cloneStringMap(in.Metadata),
		Description: in.Description,
	}
}

func workerRegistrationFromProto(in *heliosv1.WorkerRegistration) domain.WorkerRegistration {
	if in == nil {
		return domain.WorkerRegistration{}
	}
	return domain.WorkerRegistration{
		Hostname:           in.GetHostname(),
		Version:            in.GetVersion(),
		SupportedTaskTypes: append([]string(nil), in.GetSupportedTaskTypes()...),
		Capacity:           int(in.GetCapacity()),
		CPUCapacityUnits:   int(in.GetCpuCapacityUnits()),
		MemoryCapacityMB:   int(in.GetMemoryCapacityMb()),
	}
}

func workerHeartbeatFromProto(in *heliosv1.WorkerHeartbeat) domain.WorkerHeartbeat {
	if in == nil {
		return domain.WorkerHeartbeat{}
	}
	return domain.WorkerHeartbeat{
		CPULoad:          in.GetCpuLoad(),
		MemoryUsedMB:     int(in.GetMemoryUsedMb()),
		FreeSlots:        int(in.GetFreeSlots()),
		QueueDepth:       int(in.GetQueueDepth()),
		RunningTaskCount: int(in.GetRunningTaskCount()),
	}
}

func workerSnapshotToProto(in domain.WorkerSnapshot) *heliosv1.WorkerSnapshot {
	return &heliosv1.WorkerSnapshot{
		WorkerId:           in.WorkerID,
		Hostname:           in.Hostname,
		Version:            in.Version,
		SupportedTaskTypes: append([]string(nil), in.SupportedTaskTypes...),
		Capacity:           int32(in.Capacity),
		RunningTaskCount:   int32(in.RunningTaskCount),
		FreeSlots:          int32(in.FreeSlots),
		QueueDepth:         int32(in.QueueDepth),
		CpuLoad:            in.CPULoad,
		CpuCapacityUnits:   int32(in.CPUCapacityUnits),
		AllocatedCpuUnits:  int32(in.AllocatedCPUUnits),
		MemoryUsedMb:       int32(in.MemoryUsedMB),
		MemoryCapacityMb:   int32(in.MemoryCapacityMB),
		AllocatedMemoryMb:  int32(in.AllocatedMemoryMB),
		LastHeartbeatAt:    ts(in.LastHeartbeatAt),
		Health:             string(in.Health),
		RegisteredAt:       ts(in.RegisteredAt),
	}
}

func workerRegistrationResultToProto(in domain.WorkerRegistrationResult) *heliosv1.WorkerRegistrationResult {
	return &heliosv1.WorkerRegistrationResult{
		Worker:      workerSnapshotToProto(in.Worker),
		WorkerToken: in.Token,
	}
}

func dryRunAnalysisToProto(in dryRunWorkflowAnalysis) *heliosv1.DryRunWorkflowAnalysis {
	out := &heliosv1.DryRunWorkflowAnalysis{
		TaskCount:               int32(in.TaskCount),
		EdgeCount:               int32(in.EdgeCount),
		RootTasks:               append([]string(nil), in.RootTasks...),
		TerminalTasks:           append([]string(nil), in.TerminalTasks...),
		MaxParallelismEstimate:  int32(in.MaxParallelismEstimate),
		CriticalPath:            append([]string(nil), in.CriticalPath...),
		CriticalPathLength:      int32(in.CriticalPathLength),
		EstimatedRuntimeSeconds: int32(in.EstimatedRuntimeSeconds),
		TaskTypes:               make(map[string]int32, len(in.TaskTypes)),
		RiskWarnings:            append([]string(nil), in.RiskWarnings...),
	}
	for key, value := range in.TaskTypes {
		out.TaskTypes[key] = int32(value)
	}
	return out
}

func failureAnalysisToProto(in plannerFailureAnalysisResponse) (*heliosv1.FailureAnalysisResponse, error) {
	rawSnapshot, err := toStructPB(in.RawSnapshot)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode raw snapshot: %v", err)
	}
	out := &heliosv1.FailureAnalysisResponse{
		Summary:          in.Summary,
		FailureClass:     in.FailureClass,
		LikelyRootCauses: append([]string(nil), in.LikelyRootCauses...),
		RecoveryActions:  append([]string(nil), in.RecoveryActions...),
		AffectedTasks:    append([]string(nil), in.AffectedTasks...),
		Confidence:       in.Confidence,
		Backend:          in.Backend,
		GeneratedAt:      ts(in.GeneratedAt),
		RawSnapshot:      rawSnapshot,
		RunbookMatches:   make([]*heliosv1.RunbookMatch, 0, len(in.RunbookMatches)),
	}
	for _, match := range in.RunbookMatches {
		out.RunbookMatches = append(out.RunbookMatches, &heliosv1.RunbookMatch{
			Title:   match.Title,
			Path:    match.Path,
			Score:   match.Score,
			Excerpt: match.Excerpt,
		})
	}
	return out, nil
}

func validationErrorsForWorkflow(spec domain.WorkflowSpec) []string {
	if err := dag.Validate(spec); err != nil {
		return []string{err.Error()}
	}
	return nil
}

func toStructPB(value map[string]any) (*structpb.Struct, error) {
	if len(value) == 0 {
		return nil, nil
	}
	return structpb.NewStruct(value)
}

func ts(value time.Time) *timestamppb.Timestamp {
	if value.IsZero() {
		return nil
	}
	return timestamppb.New(value)
}

func tsp(value *time.Time) *timestamppb.Timestamp {
	if value == nil || value.IsZero() {
		return nil
	}
	return timestamppb.New(*value)
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

func nonNilStrings(in []string) []string {
	if len(in) == 0 {
		return []string{}
	}
	return append([]string(nil), in...)
}

func grpcError(err error) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok {
		return st.Err()
	}
	message := err.Error()
	lower := strings.ToLower(message)
	switch {
	case strings.Contains(lower, "not found"):
		return status.Error(codes.NotFound, message)
	case strings.Contains(lower, "rate limit"):
		return status.Error(codes.ResourceExhausted, message)
	case strings.Contains(lower, "limit must"), strings.Contains(lower, "decode"), strings.Contains(lower, "cycle detected"),
		strings.Contains(lower, "invalid"), strings.Contains(lower, "required"), strings.Contains(lower, "does not match"):
		return status.Error(codes.InvalidArgument, message)
	case strings.Contains(lower, "planner service is not configured"):
		return status.Error(codes.FailedPrecondition, message)
	case strings.Contains(lower, "unauthorized"):
		return status.Error(codes.Unauthenticated, message)
	default:
		return status.Error(codes.Internal, message)
	}
}

func grpcErrorFromHTTPStatus(statusCode int, body []byte) error {
	message := fmt.Sprintf("planner request failed with %d", statusCode)
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err == nil {
		if detail, ok := payload["detail"].(string); ok && detail != "" {
			message = detail
		}
	}
	switch {
	case statusCode == http.StatusUnauthorized:
		return status.Error(codes.Unauthenticated, message)
	case statusCode == http.StatusForbidden:
		return status.Error(codes.PermissionDenied, message)
	case statusCode == http.StatusNotFound:
		return status.Error(codes.NotFound, message)
	case statusCode == http.StatusTooManyRequests:
		return status.Error(codes.ResourceExhausted, message)
	case statusCode == http.StatusBadRequest || statusCode == http.StatusUnprocessableEntity:
		return status.Error(codes.InvalidArgument, message)
	case statusCode == http.StatusServiceUnavailable || statusCode == http.StatusBadGateway || statusCode == http.StatusGatewayTimeout:
		return status.Error(codes.Unavailable, message)
	default:
		return status.Error(codes.Internal, message)
	}
}
