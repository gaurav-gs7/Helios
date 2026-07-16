package postgres

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gaurav-gs7/helios/internal/domain"
	"github.com/gaurav-gs7/helios/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

func TestStoreRecoversExpiredAttempts(t *testing.T) {
	databaseURL := os.Getenv("HELIOS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("HELIOS_TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := New(ctx, databaseURL, logger, metrics.New(prometheus.NewRegistry()))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if _, err := store.pool.Exec(ctx, `
		TRUNCATE task_results, task_state_history, task_events, task_attempts,
		         task_dependencies, tasks, workers, workflows CASCADE
	`); err != nil {
		t.Fatalf("reset test state: %v", err)
	}

	workflow, err := store.CreateWorkflow(ctx, domain.WorkflowSpec{
		Name: "expired-attempt-recovery",
		Tasks: []domain.TaskSpec{{
			TaskID:         "recover-me",
			TaskType:       "validate_payload",
			InputPayload:   json.RawMessage(`{"value":1}`),
			TimeoutSeconds: 30,
			RetryPolicy: domain.RetryPolicy{
				MaxAttempts:           2,
				InitialBackoffSeconds: 1,
				MaxBackoffSeconds:     1,
				Multiplier:            1,
			},
		}},
	})
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	registration, err := store.RegisterWorker(ctx, domain.WorkerRegistration{
		Hostname:           "integration-worker",
		Version:            "test",
		SupportedTaskTypes: []string{"validate_payload"},
		Capacity:           1,
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	ready := requireSingleReadyTask(t, ctx, store)
	first, err := store.LeaseTask(ctx, ready, registration.Worker, 50*time.Millisecond, "least-loaded")
	if err != nil {
		t.Fatalf("lease first attempt: %v", err)
	}
	if err := store.StartTask(ctx, workflow.WorkflowID, ready.TaskID, domain.StartTaskRequest{
		WorkerID:  registration.Worker.WorkerID,
		AttemptID: first.AttemptID,
	}); err != nil {
		t.Fatalf("start first attempt: %v", err)
	}

	waitForLeaseExpiry(first.LeaseExpiresAt)
	if err := store.RecoverActiveTasks(ctx); err != nil {
		t.Fatalf("recover running attempt: %v", err)
	}
	recovered, err := store.GetTask(ctx, ready.TaskID)
	if err != nil {
		t.Fatalf("get recovered task: %v", err)
	}
	if recovered.State != domain.TaskStateRetryWait {
		t.Fatalf("expected retry_wait after first expiry, got %s", recovered.State)
	}
	if !strings.Contains(recovered.LastError, "lease expired") {
		t.Fatalf("expected recovery reason, got %q", recovered.LastError)
	}

	err = store.CompleteTask(ctx, workflow.WorkflowID, ready.TaskID, domain.CompleteTaskRequest{
		WorkerID:      registration.Worker.WorkerID,
		AttemptID:     first.AttemptID,
		OutputPayload: json.RawMessage(`{"stale":true}`),
	})
	if err == nil || !strings.Contains(err.Error(), "does not match active attempt") {
		t.Fatalf("expected stale completion rejection, got %v", err)
	}

	if _, err := store.pool.Exec(ctx, `UPDATE tasks SET next_run_at = NOW() WHERE workflow_id = $1 AND task_id = $2`, workflow.WorkflowID, ready.TaskID); err != nil {
		t.Fatalf("make retry runnable: %v", err)
	}
	if err := store.PromoteRetryableTasks(ctx); err != nil {
		t.Fatalf("promote retry: %v", err)
	}

	ready = requireSingleReadyTask(t, ctx, store)
	second, err := store.LeaseTask(ctx, ready, registration.Worker, 50*time.Millisecond, "least-loaded")
	if err != nil {
		t.Fatalf("lease second attempt: %v", err)
	}
	if second.Attempt != 2 {
		t.Fatalf("expected second attempt number, got %d", second.Attempt)
	}
	waitForLeaseExpiry(second.LeaseExpiresAt)
	if err := store.RecoverActiveTasks(ctx); err != nil {
		t.Fatalf("recover terminal attempt: %v", err)
	}

	failed, err := store.GetTask(ctx, ready.TaskID)
	if err != nil {
		t.Fatalf("get terminal task: %v", err)
	}
	if failed.State != domain.TaskStateFailed {
		t.Fatalf("expected failed task after retry exhaustion, got %s", failed.State)
	}
	updatedWorkflow, err := store.GetWorkflow(ctx, workflow.WorkflowID)
	if err != nil {
		t.Fatalf("get terminal workflow: %v", err)
	}
	if updatedWorkflow.State != domain.WorkflowStateFailed {
		t.Fatalf("expected failed workflow, got %s", updatedWorkflow.State)
	}

	var timedOutResults int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM task_results WHERE workflow_id = $1 AND status = $2`, workflow.WorkflowID, domain.TaskStateTimedOut).Scan(&timedOutResults); err != nil {
		t.Fatalf("count timeout results: %v", err)
	}
	if timedOutResults != 2 {
		t.Fatalf("expected two durable timeout results, got %d", timedOutResults)
	}
}

func waitForLeaseExpiry(expiresAt time.Time) {
	if wait := time.Until(expiresAt) + 25*time.Millisecond; wait > 0 {
		time.Sleep(wait)
	}
}

func requireSingleReadyTask(t *testing.T, ctx context.Context, store *Store) domain.TaskRecord {
	t.Helper()
	tasks, err := store.ListReadyTasks(ctx, 10)
	if err != nil {
		t.Fatalf("list ready tasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected one ready task, got %d", len(tasks))
	}
	return tasks[0]
}
