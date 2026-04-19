package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gauravgs7/helios/internal/dag"
	"github.com/gauravgs7/helios/internal/domain"
	"github.com/gauravgs7/helios/internal/store/postgres"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	store          *postgres.Store
	logger         *slog.Logger
	version        string
	plannerURL     string
	adminToken     string
	bootstrapToken string
	maxBodyBytes   int64
	httpClient     *http.Client
	metricsHandler http.Handler
	submissions    *submissionLimiter
}

func NewServer(store *postgres.Store, logger *slog.Logger, version string, plannerURL string, adminToken string, bootstrapToken string, maxBodyBytes int64, registry *prometheus.Registry, submissionRatePerMinute int) *Server {
	return &Server{
		store:          store,
		logger:         logger,
		version:        version,
		plannerURL:     strings.TrimRight(plannerURL, "/"),
		adminToken:     adminToken,
		bootstrapToken: bootstrapToken,
		maxBodyBytes:   maxBodyBytes,
		httpClient:     &http.Client{Timeout: 10 * time.Second},
		metricsHandler: promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
		submissions:    newSubmissionLimiter(submissionRatePerMinute, time.Minute),
	}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.Handle("GET /metrics", s.metricsHandler)
	mux.Handle("POST /api/v1/workflows", chain(http.HandlerFunc(s.handleCreateWorkflow),
		bearerTokenAuthMiddleware(s.adminToken),
		submissionRateLimitMiddleware(s.submissions),
	))
	mux.Handle("GET /api/v1/workflows", chain(http.HandlerFunc(s.handleListWorkflows),
		bearerTokenAuthMiddleware(s.adminToken),
	))
	mux.Handle("GET /api/v1/workflows/{workflowID}", chain(http.HandlerFunc(s.handleGetWorkflow),
		bearerTokenAuthMiddleware(s.adminToken),
	))
	mux.Handle("GET /api/v1/workflows/{workflowID}/tasks", chain(http.HandlerFunc(s.handleListWorkflowTasks),
		bearerTokenAuthMiddleware(s.adminToken),
	))
	mux.Handle("GET /api/v1/workflows/{workflowID}/events", chain(http.HandlerFunc(s.handleListWorkflowEvents),
		bearerTokenAuthMiddleware(s.adminToken),
	))
	mux.Handle("GET /api/v1/tasks/{taskID}", chain(http.HandlerFunc(s.handleGetTask),
		bearerTokenAuthMiddleware(s.adminToken),
	))
	mux.Handle("POST /api/v1/workflows/{workflowID}/cancel", chain(http.HandlerFunc(s.handleCancelWorkflow),
		bearerTokenAuthMiddleware(s.adminToken),
	))
	mux.Handle("POST /api/v1/workflows/{workflowID}/tasks/{taskID}/start", chain(http.HandlerFunc(s.handleStartTask),
		workerAuthMiddleware(s.store),
	))
	mux.Handle("POST /api/v1/workflows/{workflowID}/tasks/{taskID}/complete", chain(http.HandlerFunc(s.handleCompleteTask),
		workerAuthMiddleware(s.store),
	))
	mux.Handle("POST /api/v1/workflows/{workflowID}/tasks/{taskID}/fail", chain(http.HandlerFunc(s.handleFailTask),
		workerAuthMiddleware(s.store),
	))
	mux.Handle("POST /api/v1/workers/register", chain(http.HandlerFunc(s.handleRegisterWorker),
		bearerTokenAuthMiddleware(s.bootstrapToken),
	))
	mux.Handle("POST /api/v1/workers/{workerID}/heartbeat", chain(http.HandlerFunc(s.handleHeartbeatWorker),
		workerAuthMiddleware(s.store),
	))
	mux.Handle("GET /api/v1/workers", chain(http.HandlerFunc(s.handleListWorkers),
		bearerTokenAuthMiddleware(s.adminToken),
	))
	mux.Handle("POST /api/v1/planner/intent", chain(http.HandlerFunc(s.handlePlannerIntent),
		bearerTokenAuthMiddleware(s.adminToken),
	))
	mux.Handle("POST /api/v1/planner/dry-run", chain(http.HandlerFunc(s.handlePlannerDryRun),
		bearerTokenAuthMiddleware(s.adminToken),
	))
	mux.Handle("POST /api/v1/workflows/{workflowID}/ai/failure-analysis", chain(http.HandlerFunc(s.handleWorkflowFailureAnalysis),
		bearerTokenAuthMiddleware(s.adminToken),
	))
	return chain(mux,
		requestContextMiddleware(),
		bodyLimitMiddleware(s.maxBodyBytes),
		loggingMiddleware(s.logger),
		recoveryMiddleware(s.logger),
	)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, domain.HealthResponse{
		Status:    "ok",
		Timestamp: time.Now(),
		Version:   s.version,
	})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	checks := map[string]string{
		"postgres": "ok",
	}
	if err := s.store.Ping(r.Context()); err != nil {
		checks["postgres"] = err.Error()
		writeJSON(w, http.StatusServiceUnavailable, domain.ReadyResponse{
			Status:    "degraded",
			Timestamp: time.Now(),
			Checks:    checks,
		})
		return
	}
	if s.plannerURL != "" {
		if err := WaitForPlanner(r.Context(), s.plannerURL); err != nil {
			checks["planner"] = err.Error()
			writeJSON(w, http.StatusServiceUnavailable, domain.ReadyResponse{
				Status:    "degraded",
				Timestamp: time.Now(),
				Checks:    checks,
			})
			return
		}
		checks["planner"] = "ok"
	}
	writeJSON(w, http.StatusOK, domain.ReadyResponse{
		Status:    "ok",
		Timestamp: time.Now(),
		Checks:    checks,
	})
}

func (s *Server) handleCreateWorkflow(w http.ResponseWriter, r *http.Request) {
	var spec domain.WorkflowSpec
	if err := decodeJSON(r.Body, &spec); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := dag.Validate(spec); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	summary, err := s.store.CreateWorkflow(r.Context(), spec)
	if err != nil {
		s.logger.Error("create workflow", "err", err)
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusAccepted, summary)
}

func (s *Server) handleListWorkflows(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("limit must be an integer"))
			return
		}
		limit = parsed
	}
	workflows, err := s.store.ListWorkflows(r.Context(), strings.TrimSpace(r.URL.Query().Get("state")), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, domain.WorkflowListResponse{Workflows: workflows})
}

func (s *Server) handleGetWorkflow(w http.ResponseWriter, r *http.Request) {
	summary, err := s.store.GetWorkflow(r.Context(), r.PathValue("workflowID"))
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, summary)
}

func (s *Server) handleListWorkflowTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := s.store.ListWorkflowTasks(r.Context(), r.PathValue("workflowID"))
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, domain.TaskListResponse{
		WorkflowID: r.PathValue("workflowID"),
		Tasks:      tasks,
	})
}

func (s *Server) handleListWorkflowEvents(w http.ResponseWriter, r *http.Request) {
	events, err := s.store.ListWorkflowEvents(r.Context(), r.PathValue("workflowID"), 200)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, domain.WorkflowEventListResponse{
		WorkflowID: r.PathValue("workflowID"),
		Events:     events,
	})
}

func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	task, err := s.store.GetTask(r.Context(), r.PathValue("taskID"))
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (s *Server) handleCancelWorkflow(w http.ResponseWriter, r *http.Request) {
	if err := s.store.CancelWorkflow(r.Context(), r.PathValue("workflowID"), "api"); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "cancelled"})
}

func (s *Server) handleStartTask(w http.ResponseWriter, r *http.Request) {
	var req domain.StartTaskRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.store.StartTask(r.Context(), r.PathValue("workflowID"), r.PathValue("taskID"), req); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "running"})
}

func (s *Server) handleCompleteTask(w http.ResponseWriter, r *http.Request) {
	var req domain.CompleteTaskRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.store.CompleteTask(r.Context(), r.PathValue("workflowID"), r.PathValue("taskID"), req); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "succeeded"})
}

func (s *Server) handleFailTask(w http.ResponseWriter, r *http.Request) {
	var req domain.FailTaskRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.store.FailTask(r.Context(), r.PathValue("workflowID"), r.PathValue("taskID"), req); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "recorded"})
}

func (s *Server) handleRegisterWorker(w http.ResponseWriter, r *http.Request) {
	var req domain.WorkerRegistration
	if err := decodeJSON(r.Body, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	worker, err := s.store.RegisterWorker(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, worker)
}

func (s *Server) handleHeartbeatWorker(w http.ResponseWriter, r *http.Request) {
	var heartbeat domain.WorkerHeartbeat
	if r.Body != nil {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if len(strings.TrimSpace(string(body))) > 0 {
			if err := json.Unmarshal(body, &heartbeat); err != nil {
				writeError(w, http.StatusBadRequest, fmt.Errorf("decode heartbeat: %w", err))
				return
			}
		}
	}
	if err := s.store.HeartbeatWorker(r.Context(), r.PathValue("workerID"), heartbeat); err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "heartbeat-recorded"})
}

func (s *Server) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	workers, err := s.store.ListWorkers(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, workers)
}

func (s *Server) handlePlannerIntent(w http.ResponseWriter, r *http.Request) {
	if s.plannerURL == "" {
		writeError(w, http.StatusServiceUnavailable, fmt.Errorf("planner service is not configured"))
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, s.plannerURL+"/v1/plan/intent", bytes.NewReader(body))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(respBody)
}

type plannerIntentEnvelope struct {
	Workflow       domain.WorkflowSpec `json:"workflow"`
	PlanningNotes  []string            `json:"planning_notes"`
	SchedulerHints map[string]any      `json:"scheduler_hints"`
	CreatedAt      time.Time           `json:"created_at"`
}

type dryRunResponse struct {
	Valid            bool                   `json:"valid"`
	ValidationErrors []string               `json:"validation_errors,omitempty"`
	Workflow         domain.WorkflowSpec    `json:"workflow"`
	PlanningNotes    []string               `json:"planning_notes,omitempty"`
	SchedulerHints   map[string]any         `json:"scheduler_hints,omitempty"`
	Analysis         dryRunWorkflowAnalysis `json:"analysis"`
	CreatedAt        time.Time              `json:"created_at"`
}

type dryRunWorkflowAnalysis struct {
	TaskCount               int            `json:"task_count"`
	EdgeCount               int            `json:"edge_count"`
	RootTasks               []string       `json:"root_tasks"`
	TerminalTasks           []string       `json:"terminal_tasks"`
	MaxParallelismEstimate  int            `json:"max_parallelism_estimate"`
	CriticalPath            []string       `json:"critical_path"`
	CriticalPathLength      int            `json:"critical_path_length"`
	EstimatedRuntimeSeconds int            `json:"estimated_runtime_seconds"`
	TaskTypes               map[string]int `json:"task_types"`
	RiskWarnings            []string       `json:"risk_warnings,omitempty"`
}

func (s *Server) handlePlannerDryRun(w http.ResponseWriter, r *http.Request) {
	if s.plannerURL == "" {
		writeError(w, http.StatusServiceUnavailable, fmt.Errorf("planner service is not configured"))
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	respBody, statusCode, err := s.postPlanner(r.Context(), "/v1/plan/intent", body)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	if statusCode >= http.StatusMultipleChoices {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		_, _ = w.Write(respBody)
		return
	}

	var planned plannerIntentEnvelope
	if err := json.Unmarshal(respBody, &planned); err != nil {
		writeError(w, http.StatusBadGateway, fmt.Errorf("decode planner response: %w", err))
		return
	}
	out := dryRunResponse{
		Workflow:       planned.Workflow,
		PlanningNotes:  planned.PlanningNotes,
		SchedulerHints: planned.SchedulerHints,
		Analysis:       analyzeWorkflowSpec(planned.Workflow),
		CreatedAt:      time.Now(),
	}
	if err := dag.Validate(planned.Workflow); err != nil {
		out.Valid = false
		out.ValidationErrors = []string{err.Error()}
		writeJSON(w, http.StatusOK, out)
		return
	}
	out.Valid = true
	writeJSON(w, http.StatusOK, out)
}

type failureAnalysisOptions struct {
	RunbookQuery       string `json:"runbook_query,omitempty"`
	IncludeRawSnapshot bool   `json:"include_raw_snapshot,omitempty"`
}

func (s *Server) handleWorkflowFailureAnalysis(w http.ResponseWriter, r *http.Request) {
	if s.plannerURL == "" {
		writeError(w, http.StatusServiceUnavailable, fmt.Errorf("planner service is not configured"))
		return
	}
	details, err := s.store.GetWorkflowDetails(r.Context(), r.PathValue("workflowID"))
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	var opts failureAnalysisOptions
	if r.Body != nil {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if len(strings.TrimSpace(string(body))) > 0 {
			if err := json.Unmarshal(body, &opts); err != nil {
				writeError(w, http.StatusBadRequest, fmt.Errorf("decode analysis options: %w", err))
				return
			}
		}
	}
	payload, err := json.Marshal(map[string]any{
		"workflow":             details,
		"runbook_query":        opts.RunbookQuery,
		"include_raw_snapshot": opts.IncludeRawSnapshot,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	respBody, statusCode, err := s.postPlanner(r.Context(), "/v1/analyze/failure", payload)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_, _ = w.Write(respBody)
}

func (s *Server) postPlanner(ctx context.Context, path string, body []byte) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.plannerURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}
	return respBody, resp.StatusCode, nil
}

func analyzeWorkflowSpec(spec domain.WorkflowSpec) dryRunWorkflowAnalysis {
	analysis := dryRunWorkflowAnalysis{
		TaskCount: len(spec.Tasks),
		TaskTypes: make(map[string]int),
	}
	dependents := make(map[string][]string, len(spec.Tasks))
	timeoutByTask := make(map[string]int, len(spec.Tasks))
	dependencyCount := make(map[string]int, len(spec.Tasks))
	for _, task := range spec.Tasks {
		analysis.EdgeCount += len(task.Dependencies)
		analysis.TaskTypes[task.TaskType]++
		timeoutByTask[task.TaskID] = task.TimeoutSeconds
		dependencyCount[task.TaskID] = len(task.Dependencies)
		if len(task.Dependencies) == 0 {
			analysis.RootTasks = append(analysis.RootTasks, task.TaskID)
		}
		for _, dep := range task.Dependencies {
			dependents[dep] = append(dependents[dep], task.TaskID)
		}
		if task.IdempotencyKey == "" && task.TaskType == "persist_artifact" {
			analysis.RiskWarnings = append(analysis.RiskWarnings, "persist_artifact should include an idempotency_key for side-effect safety")
		}
		if task.RetryPolicy.MaxAttempts > 5 {
			analysis.RiskWarnings = append(analysis.RiskWarnings, fmt.Sprintf("%s has high max_attempts=%d", task.TaskID, task.RetryPolicy.MaxAttempts))
		}
	}
	for _, task := range spec.Tasks {
		if len(dependents[task.TaskID]) == 0 {
			analysis.TerminalTasks = append(analysis.TerminalTasks, task.TaskID)
		}
	}
	sort.Strings(analysis.RootTasks)
	sort.Strings(analysis.TerminalTasks)
	analysis.MaxParallelismEstimate = estimateMaxParallelism(spec.Tasks, dependencyCount, dependents)
	analysis.CriticalPath, analysis.EstimatedRuntimeSeconds = estimateCriticalPath(spec.Tasks, timeoutByTask)
	analysis.CriticalPathLength = len(analysis.CriticalPath)
	return analysis
}

func estimateMaxParallelism(tasks []domain.TaskSpec, dependencyCount map[string]int, dependents map[string][]string) int {
	remaining := make(map[string]int, len(dependencyCount))
	for taskID, count := range dependencyCount {
		remaining[taskID] = count
	}
	ready := make([]string, 0)
	for _, task := range tasks {
		if remaining[task.TaskID] == 0 {
			ready = append(ready, task.TaskID)
		}
	}
	maxParallelism := len(ready)
	for len(ready) > 0 {
		next := make([]string, 0)
		for _, taskID := range ready {
			for _, dependent := range dependents[taskID] {
				remaining[dependent]--
				if remaining[dependent] == 0 {
					next = append(next, dependent)
				}
			}
		}
		if len(next) > maxParallelism {
			maxParallelism = len(next)
		}
		ready = next
	}
	if maxParallelism == 0 && len(tasks) > 0 {
		return 1
	}
	return maxParallelism
}

func estimateCriticalPath(tasks []domain.TaskSpec, timeoutByTask map[string]int) ([]string, int) {
	taskByID := make(map[string]domain.TaskSpec, len(tasks))
	memoDuration := make(map[string]int, len(tasks))
	memoPath := make(map[string][]string, len(tasks))
	for _, task := range tasks {
		taskByID[task.TaskID] = task
	}
	var visit func(taskID string) (int, []string)
	visit = func(taskID string) (int, []string) {
		if duration, ok := memoDuration[taskID]; ok {
			return duration, append([]string(nil), memoPath[taskID]...)
		}
		task := taskByID[taskID]
		bestDuration := 0
		var bestPath []string
		for _, dep := range task.Dependencies {
			duration, path := visit(dep)
			if duration > bestDuration {
				bestDuration = duration
				bestPath = path
			}
		}
		total := bestDuration + max(1, timeoutByTask[taskID])
		path := append(append([]string(nil), bestPath...), taskID)
		memoDuration[taskID] = total
		memoPath[taskID] = path
		return total, append([]string(nil), path...)
	}
	bestDuration := 0
	var bestPath []string
	for _, task := range tasks {
		duration, path := visit(task.TaskID)
		if duration > bestDuration {
			bestDuration = duration
			bestPath = path
		}
	}
	return bestPath, bestDuration
}

func decodeJSON(r io.Reader, target any) error {
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode json body: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("decode json body: multiple JSON documents are not allowed")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{
		"error": err.Error(),
	})
}

func WaitForPlanner(ctx context.Context, plannerURL string) error {
	if plannerURL == "" {
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(plannerURL, "/")+"/healthz", nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("planner health returned %d", resp.StatusCode)
	}
	return nil
}
