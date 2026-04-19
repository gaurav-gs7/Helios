package postgres

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/gauravgs7/helios/internal/domain"
	"github.com/gauravgs7/helios/internal/metrics"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type Store struct {
	pool    *pgxpool.Pool
	logger  *slog.Logger
	metrics *metrics.Metrics
}

func New(ctx context.Context, databaseURL string, logger *slog.Logger, m *metrics.Metrics) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}
	return &Store{pool: pool, logger: logger, metrics: m}, nil
}

func (s *Store) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *Store) ApplyMigrations(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	entries, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read embedded migrations: %w", err)
	}
	var files []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		files = append(files, entry.Name())
	}
	sort.Strings(files)
	for _, file := range files {
		version := strings.TrimSuffix(filepath.Base(file), filepath.Ext(file))
		var applied bool
		if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, version).Scan(&applied); err != nil {
			return fmt.Errorf("check migration %s: %w", version, err)
		}
		if applied {
			continue
		}
		body, err := migrationFiles.ReadFile("migrations/" + file)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", file, err)
		}
		tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return fmt.Errorf("begin migration tx %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx, string(body)); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("record migration %s: %w", version, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", version, err)
		}
	}
	return nil
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *Store) RecordSchedulerBackpressure(reason string) {
	if reason == "" {
		reason = "unknown"
	}
	s.metrics.Backpressure.WithLabelValues(reason).Inc()
}

func (s *Store) CreateWorkflow(ctx context.Context, spec domain.WorkflowSpec) (domain.WorkflowSummary, error) {
	workflowID := uuid.NewString()
	now := time.Now().UTC()
	summary := domain.WorkflowSummary{
		WorkflowID: workflowID,
		Name:       spec.Name,
		State:      domain.WorkflowStateRunning,
		CreatedAt:  now,
		UpdatedAt:  now,
		Labels:     cloneMap(spec.Labels),
		Metadata:   cloneMap(spec.Metadata),
	}
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return domain.WorkflowSummary{}, fmt.Errorf("marshal workflow spec: %w", err)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.WorkflowSummary{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	labelsJSON, _ := json.Marshal(summary.Labels)
	metadataJSON, _ := json.Marshal(summary.Metadata)
	if _, err := tx.Exec(ctx, `
		INSERT INTO workflows (workflow_id, name, state, labels, metadata, spec, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, workflowID, spec.Name, summary.State, labelsJSON, metadataJSON, specJSON, now, now); err != nil {
		return domain.WorkflowSummary{}, fmt.Errorf("insert workflow: %w", err)
	}

	for _, task := range spec.Tasks {
		policy := task.RetryPolicy.Normalized()
		retryJSON, _ := json.Marshal(policy)
		taskLabelsJSON, _ := json.Marshal(cloneMap(task.Labels))
		taskMetadataJSON, _ := json.Marshal(cloneMap(task.Metadata))
		state := domain.TaskStatePending
		if len(task.Dependencies) == 0 {
			state = domain.TaskStateReady
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO tasks (
				workflow_id, task_id, task_type, state, input_payload, timeout_seconds, retry_policy, max_attempts,
				labels, metadata, idempotency_key, priority, cpu_units, memory_mb, expected_duration_seconds, created_at, updated_at
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
		`, workflowID, task.TaskID, task.TaskType, state, []byte(task.InputPayload), task.TimeoutSeconds, retryJSON, policy.MaxAttempts,
			taskLabelsJSON, taskMetadataJSON, task.IdempotencyKey, task.Priority, task.CPUUnits, task.MemoryMB, task.ExpectedDurationSeconds, now, now); err != nil {
			return domain.WorkflowSummary{}, fmt.Errorf("insert task %s: %w", task.TaskID, err)
		}
		for _, dep := range task.Dependencies {
			if _, err := tx.Exec(ctx, `
				INSERT INTO task_dependencies (workflow_id, task_id, dependency_task_id)
				VALUES ($1, $2, $3)
			`, workflowID, task.TaskID, dep); err != nil {
				return domain.WorkflowSummary{}, fmt.Errorf("insert dependency %s->%s: %w", dep, task.TaskID, err)
			}
		}
		if err := s.insertEventTx(ctx, tx, domain.TaskEvent{
			EventID:     uuid.NewString(),
			WorkflowID:  workflowID,
			TaskID:      task.TaskID,
			Actor:       "api",
			NewState:    string(state),
			Reason:      "workflow submitted",
			OccurredAt:  now,
			Metadata:    map[string]string{"task_type": task.TaskType},
			Description: "task persisted during workflow submission",
		}); err != nil {
			return domain.WorkflowSummary{}, err
		}
		s.metrics.TaskTransitions.WithLabelValues("", string(state), "api").Inc()
	}

	if err := s.insertEventTx(ctx, tx, domain.TaskEvent{
		EventID:     uuid.NewString(),
		WorkflowID:  workflowID,
		Actor:       "api",
		NewState:    string(summary.State),
		Reason:      "workflow accepted",
		OccurredAt:  now,
		Description: "workflow stored durably and root tasks are ready",
	}); err != nil {
		return domain.WorkflowSummary{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.WorkflowSummary{}, fmt.Errorf("commit workflow tx: %w", err)
	}
	s.metrics.WorkflowsSubmitted.Inc()
	return summary, nil
}

func (s *Store) GetWorkflow(ctx context.Context, workflowID string) (domain.WorkflowSummary, error) {
	var summary domain.WorkflowSummary
	var labelsRaw, metadataRaw []byte
	err := s.pool.QueryRow(ctx, `
		SELECT workflow_id, name, state, created_at, updated_at, labels, metadata
		FROM workflows
		WHERE workflow_id = $1
	`, workflowID).Scan(&summary.WorkflowID, &summary.Name, &summary.State, &summary.CreatedAt, &summary.UpdatedAt, &labelsRaw, &metadataRaw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.WorkflowSummary{}, fmt.Errorf("workflow %s not found", workflowID)
		}
		return domain.WorkflowSummary{}, fmt.Errorf("query workflow: %w", err)
	}
	_ = json.Unmarshal(labelsRaw, &summary.Labels)
	_ = json.Unmarshal(metadataRaw, &summary.Metadata)
	return summary, nil
}

func (s *Store) ListWorkflows(ctx context.Context, state string, limit int) ([]domain.WorkflowSummary, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	args := []any{limit}
	where := ""
	if state != "" {
		where = "WHERE state = $2"
		args = append(args, state)
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT workflow_id, name, state, created_at, updated_at, labels, metadata
		FROM workflows
		%s
		ORDER BY created_at DESC
		LIMIT $1
	`, where), args...)
	if err != nil {
		return nil, fmt.Errorf("query workflows: %w", err)
	}
	defer rows.Close()
	workflows := make([]domain.WorkflowSummary, 0)
	for rows.Next() {
		var summary domain.WorkflowSummary
		var labelsRaw, metadataRaw []byte
		if err := rows.Scan(&summary.WorkflowID, &summary.Name, &summary.State, &summary.CreatedAt, &summary.UpdatedAt, &labelsRaw, &metadataRaw); err != nil {
			return nil, fmt.Errorf("scan workflow: %w", err)
		}
		_ = json.Unmarshal(labelsRaw, &summary.Labels)
		_ = json.Unmarshal(metadataRaw, &summary.Metadata)
		workflows = append(workflows, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workflows: %w", err)
	}
	return workflows, nil
}

func (s *Store) GetWorkflowDetails(ctx context.Context, workflowID string) (domain.WorkflowDetails, error) {
	summary, err := s.GetWorkflow(ctx, workflowID)
	if err != nil {
		return domain.WorkflowDetails{}, err
	}
	tasks, err := s.ListWorkflowTasks(ctx, workflowID)
	if err != nil {
		return domain.WorkflowDetails{}, err
	}
	events, err := s.ListWorkflowEvents(ctx, workflowID, 200)
	if err != nil {
		return domain.WorkflowDetails{}, err
	}
	return domain.WorkflowDetails{
		WorkflowSummary: summary,
		Tasks:           tasks,
		Events:          events,
	}, nil
}

func (s *Store) ListWorkflowTasks(ctx context.Context, workflowID string) ([]domain.TaskRecord, error) {
	taskRows, err := s.pool.Query(ctx, `
		SELECT
			t.workflow_id, t.task_id, t.task_type, t.state, t.input_payload, t.output_payload,
			t.timeout_seconds, t.retry_policy, t.max_attempts, t.attempt_count, COALESCE(t.current_attempt_id, ''),
			COALESCE(t.assigned_worker, ''), t.lease_expires_at, t.next_run_at, COALESCE(t.last_error, ''),
			COALESCE(t.idempotency_key, ''), t.labels, t.metadata, t.created_at, t.updated_at, t.completed_at, t.priority,
			t.cpu_units, t.memory_mb, t.expected_duration_seconds
		FROM tasks t
		WHERE t.workflow_id = $1
		ORDER BY t.task_id
	`, workflowID)
	if err != nil {
		return nil, fmt.Errorf("query tasks: %w", err)
	}
	defer taskRows.Close()
	tasks := make([]domain.TaskRecord, 0)
	for taskRows.Next() {
		var task domain.TaskRecord
		var retryRaw, labelsRaw, metadataRaw []byte
		var leaseExpiresAt, nextRunAt, completedAt *time.Time
		if err := taskRows.Scan(
			&task.WorkflowID, &task.TaskID, &task.TaskType, &task.State, &task.InputPayload, &task.OutputPayload,
			&task.TimeoutSeconds, &retryRaw, &task.MaxAttempts, &task.Attempt, &task.CurrentAttempt,
			&task.AssignedWorker, &leaseExpiresAt, &nextRunAt, &task.LastError,
			&task.IdempotencyKey, &labelsRaw, &metadataRaw, &task.CreatedAt, &task.UpdatedAt, &completedAt, &task.Priority,
			&task.CPUUnits, &task.MemoryMB, &task.ExpectedDurationSeconds,
		); err != nil {
			return nil, fmt.Errorf("scan task: %w", err)
		}
		task.LeaseExpiresAt = leaseExpiresAt
		task.NextRunAt = nextRunAt
		task.CompletedAt = completedAt
		_ = json.Unmarshal(retryRaw, &task.RetryPolicy)
		_ = json.Unmarshal(labelsRaw, &task.Labels)
		_ = json.Unmarshal(metadataRaw, &task.Metadata)
		deps, err := s.taskDependencies(ctx, workflowID, task.TaskID)
		if err != nil {
			return nil, err
		}
		task.Dependencies = deps
		task.DependencyCount = len(deps)
		tasks = append(tasks, task)
	}
	if err := taskRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tasks: %w", err)
	}
	return tasks, nil
}

func (s *Store) ListWorkflowEvents(ctx context.Context, workflowID string, limit int) ([]domain.TaskEvent, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	eventRows, err := s.pool.Query(ctx, `
		SELECT event_id, workflow_id, COALESCE(task_id, ''), COALESCE(attempt_id, ''), actor,
		       COALESCE(old_state, ''), COALESCE(new_state, ''), COALESCE(reason, ''), metadata, description, occurred_at
		FROM task_events
		WHERE workflow_id = $1
		ORDER BY occurred_at DESC
		LIMIT $2
	`, workflowID, limit)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer eventRows.Close()
	events := make([]domain.TaskEvent, 0)
	for eventRows.Next() {
		var evt domain.TaskEvent
		var metadataRaw []byte
		if err := eventRows.Scan(&evt.EventID, &evt.WorkflowID, &evt.TaskID, &evt.AttemptID, &evt.Actor,
			&evt.OldState, &evt.NewState, &evt.Reason, &metadataRaw, &evt.Description, &evt.OccurredAt); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		_ = json.Unmarshal(metadataRaw, &evt.Metadata)
		events = append(events, evt)
	}
	if err := eventRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return events, nil
}

func (s *Store) GetTask(ctx context.Context, taskID string) (domain.TaskRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			t.workflow_id, t.task_id, t.task_type, t.state, t.input_payload, t.output_payload,
			t.timeout_seconds, t.retry_policy, t.max_attempts, t.attempt_count, COALESCE(t.current_attempt_id, ''),
			COALESCE(t.assigned_worker, ''), t.lease_expires_at, t.next_run_at, COALESCE(t.last_error, ''),
			COALESCE(t.idempotency_key, ''), t.labels, t.metadata, t.created_at, t.updated_at, t.completed_at, t.priority,
			t.cpu_units, t.memory_mb, t.expected_duration_seconds
		FROM tasks t
		WHERE t.task_id = $1
		ORDER BY t.updated_at DESC
		LIMIT 1
	`, taskID)
	if err != nil {
		return domain.TaskRecord{}, fmt.Errorf("query task: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return domain.TaskRecord{}, fmt.Errorf("task %s not found", taskID)
	}
	var task domain.TaskRecord
	var retryRaw, labelsRaw, metadataRaw []byte
	var leaseExpiresAt, nextRunAt, completedAt *time.Time
	if err := rows.Scan(
		&task.WorkflowID, &task.TaskID, &task.TaskType, &task.State, &task.InputPayload, &task.OutputPayload,
		&task.TimeoutSeconds, &retryRaw, &task.MaxAttempts, &task.Attempt, &task.CurrentAttempt,
		&task.AssignedWorker, &leaseExpiresAt, &nextRunAt, &task.LastError,
		&task.IdempotencyKey, &labelsRaw, &metadataRaw, &task.CreatedAt, &task.UpdatedAt, &completedAt, &task.Priority,
		&task.CPUUnits, &task.MemoryMB, &task.ExpectedDurationSeconds,
	); err != nil {
		return domain.TaskRecord{}, fmt.Errorf("scan task: %w", err)
	}
	task.LeaseExpiresAt = leaseExpiresAt
	task.NextRunAt = nextRunAt
	task.CompletedAt = completedAt
	_ = json.Unmarshal(retryRaw, &task.RetryPolicy)
	_ = json.Unmarshal(labelsRaw, &task.Labels)
	_ = json.Unmarshal(metadataRaw, &task.Metadata)
	deps, err := s.taskDependencies(ctx, task.WorkflowID, task.TaskID)
	if err != nil {
		return domain.TaskRecord{}, err
	}
	task.Dependencies = deps
	task.DependencyCount = len(deps)
	return task, nil
}

func (s *Store) CancelWorkflow(ctx context.Context, workflowID, actor string) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin cancel tx: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		UPDATE tasks
		SET state = $1, updated_at = NOW(), completed_at = NOW(), assigned_worker = NULL, lease_expires_at = NULL
		WHERE workflow_id = $2 AND state NOT IN ($3, $4, $5)
	`, domain.TaskStateCancelled, workflowID, domain.TaskStateSucceeded, domain.TaskStateFailed, domain.TaskStateCancelled); err != nil {
		return fmt.Errorf("cancel tasks: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE workflows
		SET state = $1, updated_at = NOW()
		WHERE workflow_id = $2
	`, domain.WorkflowStateCancelled, workflowID); err != nil {
		return fmt.Errorf("cancel workflow: %w", err)
	}
	if err := s.insertEventTx(ctx, tx, domain.TaskEvent{
		EventID:     uuid.NewString(),
		WorkflowID:  workflowID,
		Actor:       actor,
		NewState:    string(domain.WorkflowStateCancelled),
		Reason:      "workflow cancelled",
		OccurredAt:  time.Now().UTC(),
		Description: "operator cancelled workflow execution",
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) RegisterWorker(ctx context.Context, registration domain.WorkerRegistration) (domain.WorkerRegistrationResult, error) {
	now := time.Now().UTC()
	snapshot := domain.WorkerSnapshot{
		WorkerID:           uuid.NewString(),
		Hostname:           registration.Hostname,
		Version:            registration.Version,
		SupportedTaskTypes: slices.Clone(registration.SupportedTaskTypes),
		Capacity:           registration.Capacity,
		CPUCapacityUnits:   registration.CPUCapacityUnits,
		MemoryCapacityMB:   registration.MemoryCapacityMB,
		LastHeartbeatAt:    now,
		Health:             domain.WorkerHealthy,
		RegisteredAt:       now,
	}
	if snapshot.Capacity <= 0 {
		snapshot.Capacity = 1
	}
	if snapshot.CPUCapacityUnits <= 0 {
		snapshot.CPUCapacityUnits = snapshot.Capacity * 1000
	}
	if snapshot.MemoryCapacityMB <= 0 {
		snapshot.MemoryCapacityMB = 1024
	}
	snapshot.FreeSlots = snapshot.Capacity
	if len(snapshot.SupportedTaskTypes) == 0 {
		return domain.WorkerRegistrationResult{}, fmt.Errorf("worker must advertise supported_task_types")
	}
	token, tokenHash, err := generateWorkerToken()
	if err != nil {
		return domain.WorkerRegistrationResult{}, err
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO workers (
			worker_id, hostname, version, supported_task_types, capacity, cpu_capacity_units, memory_capacity_mb,
			free_slots, last_heartbeat_at, health, registered_at, auth_token_hash
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`, snapshot.WorkerID, snapshot.Hostname, snapshot.Version, snapshot.SupportedTaskTypes, snapshot.Capacity,
		snapshot.CPUCapacityUnits, snapshot.MemoryCapacityMB, snapshot.FreeSlots, now, snapshot.Health, now, tokenHash); err != nil {
		return domain.WorkerRegistrationResult{}, fmt.Errorf("insert worker: %w", err)
	}
	return domain.WorkerRegistrationResult{
		Worker: snapshot,
		Token:  token,
	}, nil
}

func (s *Store) HeartbeatWorker(ctx context.Context, workerID string, heartbeat domain.WorkerHeartbeat) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE workers
		SET last_heartbeat_at = NOW(),
		    health = $2,
		    cpu_load = LEAST(GREATEST($3, 0), 1),
		    memory_used_mb = GREATEST($4, 0),
		    free_slots = GREATEST($5, 0),
		    queue_depth = GREATEST($6, 0)
		WHERE worker_id = $1
	`, workerID, domain.WorkerHealthy, heartbeat.CPULoad, heartbeat.MemoryUsedMB, heartbeat.FreeSlots, heartbeat.QueueDepth)
	if err != nil {
		return fmt.Errorf("heartbeat worker: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("worker %s not found", workerID)
	}
	s.metrics.WorkerHeartbeats.Inc()
	s.metrics.WorkerCPULoad.WithLabelValues(workerID).Set(heartbeat.CPULoad)
	s.metrics.WorkerMemoryUsedMB.WithLabelValues(workerID).Set(float64(heartbeat.MemoryUsedMB))
	s.metrics.WorkerFreeSlots.WithLabelValues(workerID).Set(float64(heartbeat.FreeSlots))
	s.metrics.WorkerQueueDepth.WithLabelValues(workerID).Set(float64(heartbeat.QueueDepth))
	return nil
}

func (s *Store) ListWorkers(ctx context.Context) ([]domain.WorkerSnapshot, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			w.worker_id, w.hostname, w.version, w.supported_task_types, w.capacity,
			w.cpu_load, w.memory_used_mb, w.memory_capacity_mb, w.cpu_capacity_units, w.free_slots, w.queue_depth,
			w.last_heartbeat_at, w.health, w.registered_at,
			COALESCE((
				SELECT COUNT(*)
				FROM tasks t
				WHERE t.assigned_worker = w.worker_id AND t.state IN ($1, $2)
			), 0) AS running_task_count,
			COALESCE((
				SELECT SUM(t.cpu_units)
				FROM tasks t
				WHERE t.assigned_worker = w.worker_id AND t.state IN ($1, $2)
			), 0) AS allocated_cpu_units,
			COALESCE((
				SELECT SUM(t.memory_mb)
				FROM tasks t
				WHERE t.assigned_worker = w.worker_id AND t.state IN ($1, $2)
			), 0) AS allocated_memory_mb
		FROM workers w
		ORDER BY w.registered_at ASC
	`, domain.TaskStateLeased, domain.TaskStateRunning)
	if err != nil {
		return nil, fmt.Errorf("query workers: %w", err)
	}
	defer rows.Close()
	var out []domain.WorkerSnapshot
	for rows.Next() {
		var snapshot domain.WorkerSnapshot
		if err := rows.Scan(&snapshot.WorkerID, &snapshot.Hostname, &snapshot.Version, &snapshot.SupportedTaskTypes,
			&snapshot.Capacity, &snapshot.CPULoad, &snapshot.MemoryUsedMB, &snapshot.MemoryCapacityMB, &snapshot.CPUCapacityUnits,
			&snapshot.FreeSlots, &snapshot.QueueDepth, &snapshot.LastHeartbeatAt, &snapshot.Health, &snapshot.RegisteredAt,
			&snapshot.RunningTaskCount, &snapshot.AllocatedCPUUnits, &snapshot.AllocatedMemoryMB); err != nil {
			return nil, fmt.Errorf("scan worker: %w", err)
		}
		out = append(out, snapshot)
	}
	return out, nil
}

func (s *Store) AuthenticateWorker(ctx context.Context, workerID, token string) error {
	if workerID == "" || token == "" {
		return fmt.Errorf("worker credentials are required")
	}
	var expectedHash string
	err := s.pool.QueryRow(ctx, `SELECT auth_token_hash FROM workers WHERE worker_id = $1`, workerID).Scan(&expectedHash)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("worker %s not found", workerID)
		}
		return fmt.Errorf("lookup worker token: %w", err)
	}
	actualHash := hashToken(token)
	if subtle.ConstantTimeCompare([]byte(expectedHash), []byte(actualHash)) != 1 {
		return fmt.Errorf("invalid worker token")
	}
	return nil
}

func (s *Store) RefreshWorkerHealth(ctx context.Context, staleAfter, deadAfter time.Duration) error {
	now := time.Now().UTC()
	deadBefore := now.Add(-deadAfter)
	staleBefore := now.Add(-staleAfter)
	if _, err := s.pool.Exec(ctx, `
		UPDATE workers
		SET health = CASE
			WHEN last_heartbeat_at <= $1 THEN $2
			WHEN last_heartbeat_at <= $3 THEN $4
			ELSE $5
		END
	`, deadBefore, domain.WorkerDead, staleBefore, domain.WorkerStale, domain.WorkerHealthy); err != nil {
		return fmt.Errorf("refresh worker health: %w", err)
	}
	count, err := s.healthyWorkerCount(ctx)
	if err != nil {
		return err
	}
	s.metrics.WorkerGauge.Set(float64(count))
	return nil
}

func (s *Store) PromoteRetryableTasks(ctx context.Context) error {
	rows, err := s.pool.Query(ctx, `
		UPDATE tasks
		SET state = $1, next_run_at = NULL, updated_at = NOW()
		WHERE state = $2 AND next_run_at <= NOW()
		RETURNING workflow_id, task_id
	`, domain.TaskStateReady, domain.TaskStateRetryWait)
	if err != nil {
		return fmt.Errorf("promote retryable tasks: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var workflowID, taskID string
		if err := rows.Scan(&workflowID, &taskID); err != nil {
			return fmt.Errorf("scan promoted task: %w", err)
		}
		s.metrics.TaskTransitions.WithLabelValues(string(domain.TaskStateRetryWait), string(domain.TaskStateReady), "recovery").Inc()
		if err := s.insertEvent(ctx, domain.TaskEvent{
			EventID:     uuid.NewString(),
			WorkflowID:  workflowID,
			TaskID:      taskID,
			Actor:       "recovery",
			OldState:    string(domain.TaskStateRetryWait),
			NewState:    string(domain.TaskStateReady),
			Reason:      "backoff window elapsed",
			OccurredAt:  time.Now().UTC(),
			Description: "task returned to ready queue after retry backoff",
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ListReadyTasks(ctx context.Context, limit int) ([]domain.TaskRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT workflow_id, task_id, task_type, state, input_payload, timeout_seconds, retry_policy,
		       max_attempts, attempt_count, priority, cpu_units, memory_mb, expected_duration_seconds,
		       COALESCE(idempotency_key, ''), created_at, updated_at
		FROM tasks
		WHERE state = $1
		ORDER BY priority DESC, created_at ASC
		LIMIT $2
	`, domain.TaskStateReady, limit)
	if err != nil {
		return nil, fmt.Errorf("query ready tasks: %w", err)
	}
	defer rows.Close()
	var tasks []domain.TaskRecord
	for rows.Next() {
		var task domain.TaskRecord
		var retryRaw []byte
		if err := rows.Scan(&task.WorkflowID, &task.TaskID, &task.TaskType, &task.State, &task.InputPayload,
			&task.TimeoutSeconds, &retryRaw, &task.MaxAttempts, &task.Attempt, &task.Priority,
			&task.CPUUnits, &task.MemoryMB, &task.ExpectedDurationSeconds, &task.IdempotencyKey,
			&task.CreatedAt, &task.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan ready task: %w", err)
		}
		_ = json.Unmarshal(retryRaw, &task.RetryPolicy)
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func (s *Store) ListSchedulableWorkers(ctx context.Context) ([]domain.WorkerSnapshot, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			w.worker_id, w.hostname, w.version, w.supported_task_types, w.capacity,
			w.cpu_load, w.memory_used_mb, w.memory_capacity_mb, w.cpu_capacity_units, w.free_slots, w.queue_depth,
			w.last_heartbeat_at, w.health, w.registered_at,
			COALESCE((
				SELECT COUNT(*)
				FROM tasks t
				WHERE t.assigned_worker = w.worker_id AND t.state IN ($1, $2)
			), 0) AS running_task_count,
			COALESCE((
				SELECT SUM(t.cpu_units)
				FROM tasks t
				WHERE t.assigned_worker = w.worker_id AND t.state IN ($1, $2)
			), 0) AS allocated_cpu_units,
			COALESCE((
				SELECT SUM(t.memory_mb)
				FROM tasks t
				WHERE t.assigned_worker = w.worker_id AND t.state IN ($1, $2)
			), 0) AS allocated_memory_mb
		FROM workers w
		WHERE w.health = $3
		ORDER BY w.free_slots DESC, running_task_count ASC, w.cpu_load ASC, w.queue_depth ASC, w.registered_at ASC
	`, domain.TaskStateLeased, domain.TaskStateRunning, domain.WorkerHealthy)
	if err != nil {
		return nil, fmt.Errorf("query schedulable workers: %w", err)
	}
	defer rows.Close()
	var workers []domain.WorkerSnapshot
	for rows.Next() {
		var worker domain.WorkerSnapshot
		if err := rows.Scan(&worker.WorkerID, &worker.Hostname, &worker.Version, &worker.SupportedTaskTypes,
			&worker.Capacity, &worker.CPULoad, &worker.MemoryUsedMB, &worker.MemoryCapacityMB, &worker.CPUCapacityUnits,
			&worker.FreeSlots, &worker.QueueDepth, &worker.LastHeartbeatAt, &worker.Health, &worker.RegisteredAt,
			&worker.RunningTaskCount, &worker.AllocatedCPUUnits, &worker.AllocatedMemoryMB); err != nil {
			return nil, fmt.Errorf("scan schedulable worker: %w", err)
		}
		workers = append(workers, worker)
	}
	return workers, nil
}

func (s *Store) LeaseTask(ctx context.Context, task domain.TaskRecord, worker domain.WorkerSnapshot, leaseDuration time.Duration, placementPolicy string) (domain.Assignment, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.Assignment{}, fmt.Errorf("begin lease tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var currentState domain.TaskState
	var currentAttempt int
	var timeoutSeconds int
	var inputPayload []byte
	var idempotencyKey string
	err = tx.QueryRow(ctx, `
		SELECT state, attempt_count, timeout_seconds, input_payload, COALESCE(idempotency_key, ''), cpu_units, memory_mb, expected_duration_seconds
		FROM tasks
		WHERE workflow_id = $1 AND task_id = $2
		FOR UPDATE
	`, task.WorkflowID, task.TaskID).Scan(&currentState, &currentAttempt, &timeoutSeconds, &inputPayload, &idempotencyKey,
		&task.CPUUnits, &task.MemoryMB, &task.ExpectedDurationSeconds)
	if err != nil {
		return domain.Assignment{}, fmt.Errorf("lock task: %w", err)
	}
	if currentState != domain.TaskStateReady {
		return domain.Assignment{}, fmt.Errorf("task %s is no longer ready", task.TaskID)
	}
	if !slices.Contains(worker.SupportedTaskTypes, task.TaskType) {
		return domain.Assignment{}, fmt.Errorf("worker %s does not support task type %s", worker.WorkerID, task.TaskType)
	}
	var health domain.WorkerHealth
	var capacity, cpuCapacityUnits, memoryCapacityMB int
	var cpuLoad float64
	if err := tx.QueryRow(ctx, `
		SELECT health, capacity, cpu_capacity_units, memory_capacity_mb, cpu_load
		FROM workers
		WHERE worker_id = $1
		FOR UPDATE
	`, worker.WorkerID).Scan(&health, &capacity, &cpuCapacityUnits, &memoryCapacityMB, &cpuLoad); err != nil {
		return domain.Assignment{}, fmt.Errorf("lock worker: %w", err)
	}
	if health != domain.WorkerHealthy {
		return domain.Assignment{}, fmt.Errorf("worker %s is not healthy", worker.WorkerID)
	}
	var running int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM tasks
		WHERE assigned_worker = $1 AND state IN ($2, $3)
	`, worker.WorkerID, domain.TaskStateLeased, domain.TaskStateRunning).Scan(&running); err != nil {
		return domain.Assignment{}, fmt.Errorf("count worker load: %w", err)
	}
	if running >= capacity {
		return domain.Assignment{}, fmt.Errorf("worker %s is at capacity", worker.WorkerID)
	}
	var allocatedCPUUnits, allocatedMemoryMB int
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(cpu_units), 0), COALESCE(SUM(memory_mb), 0)
		FROM tasks
		WHERE assigned_worker = $1 AND state IN ($2, $3)
	`, worker.WorkerID, domain.TaskStateLeased, domain.TaskStateRunning).Scan(&allocatedCPUUnits, &allocatedMemoryMB); err != nil {
		return domain.Assignment{}, fmt.Errorf("sum worker reservations: %w", err)
	}
	if task.CPUUnits > 0 && cpuCapacityUnits > 0 && allocatedCPUUnits+task.CPUUnits > cpuCapacityUnits {
		return domain.Assignment{}, fmt.Errorf("worker %s lacks cpu capacity for task %s", worker.WorkerID, task.TaskID)
	}
	if task.MemoryMB > 0 && memoryCapacityMB > 0 && allocatedMemoryMB+task.MemoryMB > memoryCapacityMB {
		return domain.Assignment{}, fmt.Errorf("worker %s lacks memory capacity for task %s", worker.WorkerID, task.TaskID)
	}
	if placementPolicy == "resource-aware" && cpuLoad >= 0.95 {
		return domain.Assignment{}, fmt.Errorf("worker %s cpu load %.2f is too high", worker.WorkerID, cpuLoad)
	}

	now := time.Now().UTC()
	attempt := currentAttempt + 1
	leaseExpiry := now.Add(leaseDuration)
	if timeoutSeconds > 0 {
		timeoutExpiry := now.Add(time.Duration(timeoutSeconds) * time.Second)
		if timeoutExpiry.Before(leaseExpiry) {
			leaseExpiry = timeoutExpiry
		}
	}
	attemptID := uuid.NewString()

	if _, err := tx.Exec(ctx, `
		UPDATE tasks
		SET state = $1, attempt_count = $2, current_attempt_id = $3, assigned_worker = $4,
		    lease_expires_at = $5, next_run_at = NULL, updated_at = $6
		WHERE workflow_id = $7 AND task_id = $8
	`, domain.TaskStateLeased, attempt, attemptID, worker.WorkerID, leaseExpiry, now, task.WorkflowID, task.TaskID); err != nil {
		return domain.Assignment{}, fmt.Errorf("update leased task: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO task_attempts (attempt_id, workflow_id, task_id, worker_id, attempt, state, lease_expires_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
	`, attemptID, task.WorkflowID, task.TaskID, worker.WorkerID, attempt, domain.TaskStateLeased, leaseExpiry, now); err != nil {
		return domain.Assignment{}, fmt.Errorf("insert task attempt: %w", err)
	}
	if err := s.insertEventTx(ctx, tx, domain.TaskEvent{
		EventID:    uuid.NewString(),
		WorkflowID: task.WorkflowID,
		TaskID:     task.TaskID,
		AttemptID:  attemptID,
		Actor:      "scheduler",
		OldState:   string(domain.TaskStateReady),
		NewState:   string(domain.TaskStateLeased),
		Reason:     fmt.Sprintf("worker selected by %s policy", placementPolicy),
		OccurredAt: now,
		Metadata: map[string]string{
			"worker_id":                 worker.WorkerID,
			"scheduler_policy":          placementPolicy,
			"task_cpu_units":            fmt.Sprintf("%d", task.CPUUnits),
			"task_memory_mb":            fmt.Sprintf("%d", task.MemoryMB),
			"worker_allocated_cpu":      fmt.Sprintf("%d", allocatedCPUUnits),
			"worker_allocated_memory":   fmt.Sprintf("%d", allocatedMemoryMB),
			"expected_duration_seconds": fmt.Sprintf("%d", task.ExpectedDurationSeconds),
		},
		Description: "task leased to worker",
	}); err != nil {
		return domain.Assignment{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Assignment{}, fmt.Errorf("commit lease tx: %w", err)
	}
	s.metrics.TaskTransitions.WithLabelValues(string(domain.TaskStateReady), string(domain.TaskStateLeased), "scheduler").Inc()
	s.metrics.Assignments.Inc()
	return domain.Assignment{
		WorkflowID:     task.WorkflowID,
		TaskID:         task.TaskID,
		TaskType:       task.TaskType,
		AttemptID:      attemptID,
		Attempt:        attempt,
		WorkerID:       worker.WorkerID,
		TimeoutSeconds: timeoutSeconds,
		LeaseExpiresAt: leaseExpiry,
		InputPayload:   inputPayload,
		IdempotencyKey: idempotencyKey,
	}, nil
}

func (s *Store) StartTask(ctx context.Context, workflowID, taskID string, req domain.StartTaskRequest) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin start task tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var state domain.TaskState
	var attemptID, workerID string
	if err := tx.QueryRow(ctx, `
		SELECT state, COALESCE(current_attempt_id, ''), COALESCE(assigned_worker, '')
		FROM tasks
		WHERE workflow_id = $1 AND task_id = $2
		FOR UPDATE
	`, workflowID, taskID).Scan(&state, &attemptID, &workerID); err != nil {
		return fmt.Errorf("lock task for start: %w", err)
	}
	if attemptID != req.AttemptID || workerID != req.WorkerID {
		return fmt.Errorf("start request does not match active attempt")
	}
	if state == domain.TaskStateRunning {
		return tx.Commit(ctx)
	}
	if state != domain.TaskStateLeased {
		return fmt.Errorf("task %s is not leased", taskID)
	}
	if err := domain.ValidateTaskTransition(state, domain.TaskStateRunning); err != nil {
		return err
	}
	now := time.Now().UTC()
	if _, err := tx.Exec(ctx, `
		UPDATE tasks
		SET state = $1, updated_at = $2
		WHERE workflow_id = $3 AND task_id = $4
	`, domain.TaskStateRunning, now, workflowID, taskID); err != nil {
		return fmt.Errorf("update running task: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE task_attempts
		SET state = $1, started_at = $2, updated_at = $2
		WHERE attempt_id = $3
	`, domain.TaskStateRunning, now, req.AttemptID); err != nil {
		return fmt.Errorf("update running attempt: %w", err)
	}
	if err := s.insertEventTx(ctx, tx, domain.TaskEvent{
		EventID:     uuid.NewString(),
		WorkflowID:  workflowID,
		TaskID:      taskID,
		AttemptID:   req.AttemptID,
		Actor:       "worker",
		OldState:    string(domain.TaskStateLeased),
		NewState:    string(domain.TaskStateRunning),
		Reason:      "worker acknowledged execution start",
		OccurredAt:  now,
		Metadata:    map[string]string{"worker_id": req.WorkerID, "trace_id": req.TraceID},
		Description: "worker started task execution",
	}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit start task tx: %w", err)
	}
	s.metrics.TaskTransitions.WithLabelValues(string(domain.TaskStateLeased), string(domain.TaskStateRunning), "worker").Inc()
	return nil
}

func (s *Store) CompleteTask(ctx context.Context, workflowID, taskID string, req domain.CompleteTaskRequest) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin complete task tx: %w", err)
	}
	defer tx.Rollback(ctx)
	now := time.Now().UTC()

	var state domain.TaskState
	var currentAttemptID, assignedWorker string
	if err := tx.QueryRow(ctx, `
		SELECT state, COALESCE(current_attempt_id, ''), COALESCE(assigned_worker, '')
		FROM tasks
		WHERE workflow_id = $1 AND task_id = $2
		FOR UPDATE
	`, workflowID, taskID).Scan(&state, &currentAttemptID, &assignedWorker); err != nil {
		return fmt.Errorf("lock task for completion: %w", err)
	}
	if currentAttemptID != req.AttemptID || assignedWorker != req.WorkerID {
		return fmt.Errorf("completion does not match active attempt")
	}
	if state != domain.TaskStateRunning && state != domain.TaskStateLeased {
		return fmt.Errorf("task %s is not active", taskID)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE tasks
		SET state = $1, output_payload = $2, lease_expires_at = NULL, assigned_worker = NULL, current_attempt_id = NULL,
		    completed_at = $3, updated_at = $3, last_error = NULL
		WHERE workflow_id = $4 AND task_id = $5
	`, domain.TaskStateSucceeded, []byte(req.OutputPayload), now, workflowID, taskID); err != nil {
		return fmt.Errorf("update succeeded task: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE task_attempts
		SET state = $1, completed_at = $2, updated_at = $2
		WHERE attempt_id = $3
	`, domain.TaskStateSucceeded, now, req.AttemptID); err != nil {
		return fmt.Errorf("update succeeded attempt: %w", err)
	}
	if err := s.insertTaskResultTx(ctx, tx, domain.TaskResult{
		ResultID:      uuid.NewString(),
		WorkflowID:    workflowID,
		TaskID:        taskID,
		AttemptID:     req.AttemptID,
		WorkerID:      req.WorkerID,
		Status:        string(domain.TaskStateSucceeded),
		OutputPayload: req.OutputPayload,
		RecordedAt:    now,
	}); err != nil {
		return err
	}
	if err := s.insertEventTx(ctx, tx, domain.TaskEvent{
		EventID:     uuid.NewString(),
		WorkflowID:  workflowID,
		TaskID:      taskID,
		AttemptID:   req.AttemptID,
		Actor:       "worker",
		OldState:    string(state),
		NewState:    string(domain.TaskStateSucceeded),
		Reason:      "worker reported success",
		OccurredAt:  now,
		Metadata:    map[string]string{"worker_id": req.WorkerID, "trace_id": req.TraceID},
		Description: "task completed successfully",
	}); err != nil {
		return err
	}
	if err := s.unlockDependentsTx(ctx, tx, workflowID, taskID, now); err != nil {
		return err
	}
	if err := s.refreshWorkflowStateTx(ctx, tx, workflowID, now, "worker"); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit complete task tx: %w", err)
	}
	s.metrics.TaskTransitions.WithLabelValues(string(state), string(domain.TaskStateSucceeded), "worker").Inc()
	return nil
}

func (s *Store) FailTask(ctx context.Context, workflowID, taskID string, req domain.FailTaskRequest) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin fail task tx: %w", err)
	}
	defer tx.Rollback(ctx)
	now := time.Now().UTC()

	var task domain.TaskRecord
	var retryRaw []byte
	if err := tx.QueryRow(ctx, `
		SELECT state, attempt_count, max_attempts, retry_policy, COALESCE(current_attempt_id, ''), COALESCE(assigned_worker, '')
		FROM tasks
		WHERE workflow_id = $1 AND task_id = $2
		FOR UPDATE
	`, workflowID, taskID).Scan(&task.State, &task.Attempt, &task.MaxAttempts, &retryRaw, &task.CurrentAttempt, &task.AssignedWorker); err != nil {
		return fmt.Errorf("lock task for failure: %w", err)
	}
	_ = json.Unmarshal(retryRaw, &task.RetryPolicy)
	if task.CurrentAttempt != req.AttemptID || task.AssignedWorker != req.WorkerID {
		return fmt.Errorf("failure does not match active attempt")
	}
	if task.State != domain.TaskStateRunning && task.State != domain.TaskStateLeased {
		return fmt.Errorf("task %s is not active", taskID)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE task_attempts
		SET state = $1, error_message = $2, completed_at = $3, updated_at = $3
		WHERE attempt_id = $4
	`, domain.TaskStateFailed, req.Error, now, req.AttemptID); err != nil {
		return fmt.Errorf("update failed attempt: %w", err)
	}
	if err := s.insertTaskResultTx(ctx, tx, domain.TaskResult{
		ResultID:     uuid.NewString(),
		WorkflowID:   workflowID,
		TaskID:       taskID,
		AttemptID:    req.AttemptID,
		WorkerID:     req.WorkerID,
		Status:       string(domain.TaskStateFailed),
		ErrorMessage: req.Error,
		RecordedAt:   now,
	}); err != nil {
		return err
	}

	nextState := domain.TaskStateFailed
	var nextRunAt *time.Time
	reason := "retry budget exhausted"
	if req.Retryable && task.Attempt < task.RetryPolicy.Normalized().MaxAttempts {
		nextState = domain.TaskStateRetryWait
		retryAt := now.Add(task.RetryPolicy.BackoffForAttempt(task.Attempt))
		nextRunAt = &retryAt
		reason = "retry scheduled"
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tasks
		SET state = $1, next_run_at = $2, last_error = $3, lease_expires_at = NULL, assigned_worker = NULL,
		    current_attempt_id = NULL, updated_at = $4, completed_at = CASE WHEN $1 = $5 THEN $4 ELSE completed_at END
		WHERE workflow_id = $6 AND task_id = $7
	`, nextState, nextRunAt, req.Error, now, domain.TaskStateFailed, workflowID, taskID); err != nil {
		return fmt.Errorf("update failed task: %w", err)
	}
	if err := s.insertEventTx(ctx, tx, domain.TaskEvent{
		EventID:     uuid.NewString(),
		WorkflowID:  workflowID,
		TaskID:      taskID,
		AttemptID:   req.AttemptID,
		Actor:       "worker",
		OldState:    string(task.State),
		NewState:    string(nextState),
		Reason:      reason,
		OccurredAt:  now,
		Metadata:    map[string]string{"worker_id": req.WorkerID, "trace_id": req.TraceID, "error": req.Error},
		Description: "task failure reported by worker",
	}); err != nil {
		return err
	}
	if nextState == domain.TaskStateFailed {
		if err := s.cancelOutstandingTasksTx(ctx, tx, workflowID, now); err != nil {
			return err
		}
	}
	if err := s.refreshWorkflowStateTx(ctx, tx, workflowID, now, "worker"); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit fail task tx: %w", err)
	}
	s.metrics.TaskTransitions.WithLabelValues(string(task.State), string(nextState), "worker").Inc()
	return nil
}

func (s *Store) RecoverActiveTasks(ctx context.Context) error {
	rows, err := s.pool.Query(ctx, `
		SELECT workflow_id, task_id, COALESCE(current_attempt_id, ''), COALESCE(assigned_worker, ''), state, attempt_count, max_attempts, retry_policy
		FROM tasks
		WHERE state IN ($1, $2)
		  AND (
		    lease_expires_at <= NOW()
		    OR assigned_worker IN (SELECT worker_id FROM workers WHERE health = $3)
		  )
	`, domain.TaskStateLeased, domain.TaskStateRunning, domain.WorkerDead)
	if err != nil {
		return fmt.Errorf("query recoverable tasks: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var workflowID, taskID, attemptID, workerID string
		var state domain.TaskState
		var attempt, maxAttempts int
		var retryRaw []byte
		if err := rows.Scan(&workflowID, &taskID, &attemptID, &workerID, &state, &attempt, &maxAttempts, &retryRaw); err != nil {
			return fmt.Errorf("scan recoverable task: %w", err)
		}
		var policy domain.RetryPolicy
		_ = json.Unmarshal(retryRaw, &policy)
		if err := s.recoverSingleTask(ctx, workflowID, taskID, attemptID, workerID, state, attempt, policy); err != nil {
			s.logger.Warn("recover task", "workflow_id", workflowID, "task_id", taskID, "err", err)
		}
	}
	return nil
}

func (s *Store) taskDependencies(ctx context.Context, workflowID, taskID string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT dependency_task_id
		FROM task_dependencies
		WHERE workflow_id = $1 AND task_id = $2
		ORDER BY dependency_task_id
	`, workflowID, taskID)
	if err != nil {
		return nil, fmt.Errorf("query dependencies for task %s: %w", taskID, err)
	}
	defer rows.Close()
	var deps []string
	for rows.Next() {
		var dep string
		if err := rows.Scan(&dep); err != nil {
			return nil, fmt.Errorf("scan dependency: %w", err)
		}
		deps = append(deps, dep)
	}
	return deps, nil
}

func (s *Store) recoverSingleTask(ctx context.Context, workflowID, taskID, attemptID, workerID string, state domain.TaskState, attempt int, policy domain.RetryPolicy) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin recovery tx: %w", err)
	}
	defer tx.Rollback(ctx)
	var currentState domain.TaskState
	var currentAttemptID string
	if err := tx.QueryRow(ctx, `
		SELECT state, COALESCE(current_attempt_id, '')
		FROM tasks
		WHERE workflow_id = $1 AND task_id = $2
		FOR UPDATE
	`, workflowID, taskID).Scan(&currentState, &currentAttemptID); err != nil {
		return fmt.Errorf("lock recoverable task: %w", err)
	}
	if currentAttemptID != attemptID || (currentState != domain.TaskStateLeased && currentState != domain.TaskStateRunning) {
		return tx.Commit(ctx)
	}
	now := time.Now().UTC()
	if _, err := tx.Exec(ctx, `
		UPDATE task_attempts
		SET state = $1, error_message = $2, completed_at = $3, updated_at = $3
		WHERE attempt_id = $4
	`, domain.TaskStateTimedOut, "lease expired or worker became unavailable", now, attemptID); err != nil {
		return fmt.Errorf("mark attempt timed out: %w", err)
	}
	if err := s.insertTaskResultTx(ctx, tx, domain.TaskResult{
		ResultID:     uuid.NewString(),
		WorkflowID:   workflowID,
		TaskID:       taskID,
		AttemptID:    attemptID,
		WorkerID:     workerID,
		Status:       string(domain.TaskStateTimedOut),
		ErrorMessage: "lease expired or worker became unavailable",
		RecordedAt:   now,
	}); err != nil {
		return err
	}
	if err := s.insertEventTx(ctx, tx, domain.TaskEvent{
		EventID:     uuid.NewString(),
		WorkflowID:  workflowID,
		TaskID:      taskID,
		AttemptID:   attemptID,
		Actor:       "recovery",
		OldState:    string(currentState),
		NewState:    string(domain.TaskStateTimedOut),
		Reason:      "lease expired or worker unavailable",
		OccurredAt:  now,
		Metadata:    map[string]string{"worker_id": workerID},
		Description: "task attempt timed out and entered recovery",
	}); err != nil {
		return err
	}
	nextState := domain.TaskStateFailed
	var nextRunAt *time.Time
	if attempt < policy.Normalized().MaxAttempts {
		nextState = domain.TaskStateRetryWait
		retryAt := now.Add(policy.BackoffForAttempt(attempt))
		nextRunAt = &retryAt
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tasks
		SET state = $1, next_run_at = $2, last_error = $3, assigned_worker = NULL, current_attempt_id = NULL,
		    lease_expires_at = NULL, updated_at = $4, completed_at = CASE WHEN $1 = $5 THEN $4 ELSE completed_at END
		WHERE workflow_id = $6 AND task_id = $7
	`, nextState, nextRunAt, "lease expired or worker unavailable", now, domain.TaskStateFailed, workflowID, taskID); err != nil {
		return fmt.Errorf("update recovered task: %w", err)
	}
	if err := s.insertEventTx(ctx, tx, domain.TaskEvent{
		EventID:     uuid.NewString(),
		WorkflowID:  workflowID,
		TaskID:      taskID,
		Actor:       "recovery",
		OldState:    string(domain.TaskStateTimedOut),
		NewState:    string(nextState),
		Reason:      "recovery policy applied",
		OccurredAt:  now,
		Description: "task recovery policy determined next state",
	}); err != nil {
		return err
	}
	if nextState == domain.TaskStateFailed {
		if err := s.cancelOutstandingTasksTx(ctx, tx, workflowID, now); err != nil {
			return err
		}
	}
	if err := s.refreshWorkflowStateTx(ctx, tx, workflowID, now, "recovery"); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit recovery tx: %w", err)
	}
	s.metrics.Recoveries.Inc()
	s.metrics.TaskTransitions.WithLabelValues(string(currentState), string(domain.TaskStateTimedOut), "recovery").Inc()
	s.metrics.TaskTransitions.WithLabelValues(string(domain.TaskStateTimedOut), string(nextState), "recovery").Inc()
	return nil
}

func (s *Store) unlockDependentsTx(ctx context.Context, tx pgx.Tx, workflowID, completedTaskID string, now time.Time) error {
	rows, err := tx.Query(ctx, `
		SELECT task_id
		FROM task_dependencies
		WHERE workflow_id = $1 AND dependency_task_id = $2
	`, workflowID, completedTaskID)
	if err != nil {
		return fmt.Errorf("query dependents: %w", err)
	}
	defer rows.Close()
	var dependents []string
	for rows.Next() {
		var taskID string
		if err := rows.Scan(&taskID); err != nil {
			return fmt.Errorf("scan dependent: %w", err)
		}
		dependents = append(dependents, taskID)
	}
	for _, dependentTaskID := range dependents {
		var pendingDeps int
		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM task_dependencies td
			JOIN tasks t ON t.workflow_id = td.workflow_id AND t.task_id = td.dependency_task_id
			WHERE td.workflow_id = $1 AND td.task_id = $2 AND t.state != $3
		`, workflowID, dependentTaskID, domain.TaskStateSucceeded).Scan(&pendingDeps); err != nil {
			return fmt.Errorf("count pending dependencies: %w", err)
		}
		if pendingDeps == 0 {
			tag, err := tx.Exec(ctx, `
				UPDATE tasks
				SET state = $1, updated_at = $2
				WHERE workflow_id = $3 AND task_id = $4 AND state = $5
			`, domain.TaskStateReady, now, workflowID, dependentTaskID, domain.TaskStatePending)
			if err != nil {
				return fmt.Errorf("promote dependent task: %w", err)
			}
			if tag.RowsAffected() > 0 {
				s.metrics.TaskTransitions.WithLabelValues(string(domain.TaskStatePending), string(domain.TaskStateReady), "scheduler").Inc()
				if err := s.insertEventTx(ctx, tx, domain.TaskEvent{
					EventID:     uuid.NewString(),
					WorkflowID:  workflowID,
					TaskID:      dependentTaskID,
					Actor:       "scheduler",
					OldState:    string(domain.TaskStatePending),
					NewState:    string(domain.TaskStateReady),
					Reason:      "all upstream dependencies succeeded",
					OccurredAt:  now,
					Description: "task unlocked by upstream completion",
				}); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *Store) cancelOutstandingTasksTx(ctx context.Context, tx pgx.Tx, workflowID string, now time.Time) error {
	rows, err := tx.Query(ctx, `
		UPDATE tasks
		SET state = $1, updated_at = $2, completed_at = $2, assigned_worker = NULL, current_attempt_id = NULL, lease_expires_at = NULL
		WHERE workflow_id = $3 AND state IN ($4, $5, $6, $7, $8)
		RETURNING task_id
	`, domain.TaskStateCancelled, now, workflowID, domain.TaskStatePending, domain.TaskStateReady, domain.TaskStateRetryWait, domain.TaskStateLeased, domain.TaskStateRunning)
	if err != nil {
		return fmt.Errorf("cancel outstanding tasks: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var taskID string
		if err := rows.Scan(&taskID); err != nil {
			return fmt.Errorf("scan cancelled task: %w", err)
		}
		if err := s.insertEventTx(ctx, tx, domain.TaskEvent{
			EventID:     uuid.NewString(),
			WorkflowID:  workflowID,
			TaskID:      taskID,
			Actor:       "scheduler",
			NewState:    string(domain.TaskStateCancelled),
			Reason:      "workflow entered terminal failure",
			OccurredAt:  now,
			Description: "outstanding task cancelled because workflow failed",
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) refreshWorkflowStateTx(ctx context.Context, tx pgx.Tx, workflowID string, now time.Time, actor string) error {
	var total, succeeded, failed, cancelled int
	if err := tx.QueryRow(ctx, `
		SELECT
			COUNT(*) AS total,
			COUNT(*) FILTER (WHERE state = $2) AS succeeded,
			COUNT(*) FILTER (WHERE state = $3) AS failed,
			COUNT(*) FILTER (WHERE state = $4) AS cancelled
		FROM tasks
		WHERE workflow_id = $1
	`, workflowID, domain.TaskStateSucceeded, domain.TaskStateFailed, domain.TaskStateCancelled).Scan(&total, &succeeded, &failed, &cancelled); err != nil {
		return fmt.Errorf("query workflow aggregates: %w", err)
	}

	nextState := domain.WorkflowStateRunning
	switch {
	case failed > 0:
		nextState = domain.WorkflowStateFailed
	case succeeded == total && total > 0:
		nextState = domain.WorkflowStateSucceeded
	case cancelled == total && total > 0:
		nextState = domain.WorkflowStateCancelled
	}
	var currentState domain.WorkflowState
	if err := tx.QueryRow(ctx, `
		SELECT state
		FROM workflows
		WHERE workflow_id = $1
		FOR UPDATE
	`, workflowID).Scan(&currentState); err != nil {
		return fmt.Errorf("lock workflow state: %w", err)
	}
	if currentState == nextState {
		_, err := tx.Exec(ctx, `UPDATE workflows SET updated_at = $2 WHERE workflow_id = $1`, workflowID, now)
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE workflows
		SET state = $1, updated_at = $2
		WHERE workflow_id = $3
	`, nextState, now, workflowID); err != nil {
		return fmt.Errorf("update workflow state: %w", err)
	}
	return s.insertEventTx(ctx, tx, domain.TaskEvent{
		EventID:     uuid.NewString(),
		WorkflowID:  workflowID,
		Actor:       actor,
		OldState:    string(currentState),
		NewState:    string(nextState),
		Reason:      "workflow aggregate state changed",
		OccurredAt:  now,
		Description: "workflow status recomputed from task graph",
	})
}

func (s *Store) healthyWorkerCount(ctx context.Context) (int, error) {
	var count int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM workers WHERE health = $1`, domain.WorkerHealthy).Scan(&count); err != nil {
		return 0, fmt.Errorf("count healthy workers: %w", err)
	}
	return count, nil
}

func (s *Store) insertEvent(ctx context.Context, event domain.TaskEvent) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO task_events (event_id, workflow_id, task_id, attempt_id, actor, old_state, new_state, reason, metadata, description, occurred_at)
		VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), $5, NULLIF($6, ''), NULLIF($7, ''), NULLIF($8, ''), $9, $10, $11)
	`, event.EventID, event.WorkflowID, event.TaskID, event.AttemptID, event.Actor, event.OldState, event.NewState, event.Reason, mustJSON(event.Metadata), event.Description, event.OccurredAt)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	if event.TaskID != "" && event.NewState != "" {
		if _, err := s.pool.Exec(ctx, `
			INSERT INTO task_state_history (history_id, workflow_id, task_id, attempt_id, old_state, new_state, actor, reason, recorded_at)
			VALUES ($1, $2, $3, NULLIF($4, ''), NULLIF($5, ''), $6, $7, NULLIF($8, ''), $9)
		`, uuid.NewString(), event.WorkflowID, event.TaskID, event.AttemptID, event.OldState, event.NewState, event.Actor, event.Reason, event.OccurredAt); err != nil {
			return fmt.Errorf("insert task state history: %w", err)
		}
	}
	return nil
}

func (s *Store) insertEventTx(ctx context.Context, tx pgx.Tx, event domain.TaskEvent) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO task_events (event_id, workflow_id, task_id, attempt_id, actor, old_state, new_state, reason, metadata, description, occurred_at)
		VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), $5, NULLIF($6, ''), NULLIF($7, ''), NULLIF($8, ''), $9, $10, $11)
	`, event.EventID, event.WorkflowID, event.TaskID, event.AttemptID, event.Actor, event.OldState, event.NewState, event.Reason, mustJSON(event.Metadata), event.Description, event.OccurredAt)
	if err != nil {
		return fmt.Errorf("insert event tx: %w", err)
	}
	if event.TaskID != "" && event.NewState != "" {
		if _, err := tx.Exec(ctx, `
			INSERT INTO task_state_history (history_id, workflow_id, task_id, attempt_id, old_state, new_state, actor, reason, recorded_at)
			VALUES ($1, $2, $3, NULLIF($4, ''), NULLIF($5, ''), $6, $7, NULLIF($8, ''), $9)
		`, uuid.NewString(), event.WorkflowID, event.TaskID, event.AttemptID, event.OldState, event.NewState, event.Actor, event.Reason, event.OccurredAt); err != nil {
			return fmt.Errorf("insert task state history tx: %w", err)
		}
	}
	return nil
}

func (s *Store) insertTaskResultTx(ctx context.Context, tx pgx.Tx, result domain.TaskResult) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO task_results (result_id, workflow_id, task_id, attempt_id, worker_id, status, output_payload, error_message, recorded_at)
		VALUES (
			$1, $2, $3, $4, $5, $6,
			CASE WHEN $7 = '' OR $7 = 'null' THEN NULL ELSE $7::jsonb END,
			NULLIF($8, ''),
			$9
		)
	`, result.ResultID, result.WorkflowID, result.TaskID, result.AttemptID, result.WorkerID, result.Status, string(result.OutputPayload), result.ErrorMessage, result.RecordedAt)
	if err != nil {
		return fmt.Errorf("insert task result tx: %w", err)
	}
	return nil
}

func mustJSON(m map[string]string) []byte {
	if len(m) == 0 {
		return []byte(`{}`)
	}
	raw, _ := json.Marshal(m)
	return raw
}

func cloneMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func generateWorkerToken() (string, string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", "", fmt.Errorf("generate worker token: %w", err)
	}
	token := fmt.Sprintf("%x", buf)
	return token, hashToken(token), nil
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return fmt.Sprintf("%x", sum[:])
}
