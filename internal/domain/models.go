package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

type WorkflowState string

const (
	WorkflowStateSubmitted WorkflowState = "submitted"
	WorkflowStateRunning   WorkflowState = "running"
	WorkflowStateSucceeded WorkflowState = "succeeded"
	WorkflowStateFailed    WorkflowState = "failed"
	WorkflowStateCancelled WorkflowState = "cancelled"
)

type TaskState string

const (
	TaskStatePending   TaskState = "pending"
	TaskStateReady     TaskState = "ready"
	TaskStateLeased    TaskState = "leased"
	TaskStateRunning   TaskState = "running"
	TaskStateRetryWait TaskState = "retry_wait"
	TaskStateSucceeded TaskState = "succeeded"
	TaskStateFailed    TaskState = "failed"
	TaskStateTimedOut  TaskState = "timed_out"
	TaskStateCancelled TaskState = "cancelled"
)

type WorkerHealth string

const (
	WorkerHealthy WorkerHealth = "healthy"
	WorkerStale   WorkerHealth = "stale"
	WorkerDead    WorkerHealth = "dead"
)

type RetryPolicy struct {
	MaxAttempts           int     `json:"max_attempts"`
	InitialBackoffSeconds int     `json:"initial_backoff_seconds"`
	MaxBackoffSeconds     int     `json:"max_backoff_seconds"`
	Multiplier            float64 `json:"multiplier"`
}

func (p RetryPolicy) Normalized() RetryPolicy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 3
	}
	if p.InitialBackoffSeconds <= 0 {
		p.InitialBackoffSeconds = 2
	}
	if p.MaxBackoffSeconds <= 0 {
		p.MaxBackoffSeconds = 30
	}
	if p.Multiplier < 1 {
		p.Multiplier = 2
	}
	return p
}

func (p RetryPolicy) BackoffForAttempt(attempt int) time.Duration {
	p = p.Normalized()
	if attempt <= 1 {
		return time.Duration(p.InitialBackoffSeconds) * time.Second
	}
	backoff := float64(p.InitialBackoffSeconds)
	for i := 1; i < attempt; i++ {
		backoff *= p.Multiplier
		if int(backoff) >= p.MaxBackoffSeconds {
			backoff = float64(p.MaxBackoffSeconds)
			break
		}
	}
	return time.Duration(backoff) * time.Second
}

type WorkflowSpec struct {
	Name     string            `json:"name"`
	Labels   map[string]string `json:"labels,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Tasks    []TaskSpec        `json:"tasks"`
}

type TaskSpec struct {
	TaskID                  string            `json:"task_id"`
	TaskType                string            `json:"task_type"`
	Dependencies            []string          `json:"dependencies,omitempty"`
	InputPayload            json.RawMessage   `json:"input_payload"`
	TimeoutSeconds          int               `json:"timeout_seconds"`
	RetryPolicy             RetryPolicy       `json:"retry_policy"`
	Labels                  map[string]string `json:"labels,omitempty"`
	Metadata                map[string]string `json:"metadata,omitempty"`
	IdempotencyKey          string            `json:"idempotency_key,omitempty"`
	Priority                int               `json:"priority,omitempty"`
	CPUUnits                int               `json:"cpu_units,omitempty"`
	MemoryMB                int               `json:"memory_mb,omitempty"`
	ExpectedDurationSeconds int               `json:"expected_duration_seconds,omitempty"`
}

type WorkflowSummary struct {
	WorkflowID string            `json:"workflow_id"`
	Name       string            `json:"name"`
	State      WorkflowState     `json:"state"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
	Labels     map[string]string `json:"labels,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

type TaskRecord struct {
	TaskID                  string            `json:"task_id"`
	WorkflowID              string            `json:"workflow_id"`
	TaskType                string            `json:"task_type"`
	State                   TaskState         `json:"state"`
	Dependencies            []string          `json:"dependencies,omitempty"`
	Attempt                 int               `json:"attempt"`
	MaxAttempts             int               `json:"max_attempts"`
	AssignedWorker          string            `json:"assigned_worker,omitempty"`
	TimeoutSeconds          int               `json:"timeout_seconds"`
	LeaseExpiresAt          *time.Time        `json:"lease_expires_at,omitempty"`
	NextRunAt               *time.Time        `json:"next_run_at,omitempty"`
	LastError               string            `json:"last_error,omitempty"`
	IdempotencyKey          string            `json:"idempotency_key,omitempty"`
	InputPayload            json.RawMessage   `json:"input_payload,omitempty"`
	OutputPayload           json.RawMessage   `json:"output_payload,omitempty"`
	RetryPolicy             RetryPolicy       `json:"retry_policy"`
	Labels                  map[string]string `json:"labels,omitempty"`
	Metadata                map[string]string `json:"metadata,omitempty"`
	CurrentAttempt          string            `json:"current_attempt,omitempty"`
	CreatedAt               time.Time         `json:"created_at"`
	UpdatedAt               time.Time         `json:"updated_at"`
	CompletedAt             *time.Time        `json:"completed_at,omitempty"`
	Priority                int               `json:"priority"`
	CPUUnits                int               `json:"cpu_units,omitempty"`
	MemoryMB                int               `json:"memory_mb,omitempty"`
	ExpectedDurationSeconds int               `json:"expected_duration_seconds,omitempty"`
	DependencyCount         int               `json:"dependency_count"`
}

type WorkflowDetails struct {
	WorkflowSummary
	Tasks  []TaskRecord `json:"tasks"`
	Events []TaskEvent  `json:"events"`
}

type TaskEvent struct {
	EventID     string            `json:"event_id"`
	WorkflowID  string            `json:"workflow_id"`
	TaskID      string            `json:"task_id,omitempty"`
	AttemptID   string            `json:"attempt_id,omitempty"`
	Actor       string            `json:"actor"`
	OldState    string            `json:"old_state,omitempty"`
	NewState    string            `json:"new_state,omitempty"`
	Reason      string            `json:"reason,omitempty"`
	OccurredAt  time.Time         `json:"occurred_at"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Description string            `json:"description,omitempty"`
}

type WorkerRegistration struct {
	Hostname           string   `json:"hostname"`
	Version            string   `json:"version"`
	SupportedTaskTypes []string `json:"supported_task_types"`
	Capacity           int      `json:"capacity"`
	CPUCapacityUnits   int      `json:"cpu_capacity_units,omitempty"`
	MemoryCapacityMB   int      `json:"memory_capacity_mb,omitempty"`
}

type WorkerSnapshot struct {
	WorkerID           string       `json:"worker_id"`
	Hostname           string       `json:"hostname"`
	Version            string       `json:"version"`
	SupportedTaskTypes []string     `json:"supported_task_types"`
	Capacity           int          `json:"capacity"`
	RunningTaskCount   int          `json:"running_task_count"`
	FreeSlots          int          `json:"free_slots"`
	QueueDepth         int          `json:"queue_depth"`
	CPULoad            float64      `json:"cpu_load"`
	CPUCapacityUnits   int          `json:"cpu_capacity_units"`
	AllocatedCPUUnits  int          `json:"allocated_cpu_units"`
	MemoryUsedMB       int          `json:"memory_used_mb"`
	MemoryCapacityMB   int          `json:"memory_capacity_mb"`
	AllocatedMemoryMB  int          `json:"allocated_memory_mb"`
	LastHeartbeatAt    time.Time    `json:"last_heartbeat_at"`
	Health             WorkerHealth `json:"health"`
	RegisteredAt       time.Time    `json:"registered_at"`
}

type WorkerRegistrationResult struct {
	Worker WorkerSnapshot `json:"worker"`
	Token  string         `json:"worker_token"`
}

type WorkerHeartbeat struct {
	CPULoad          float64 `json:"cpu_load"`
	MemoryUsedMB     int     `json:"memory_used_mb"`
	FreeSlots        int     `json:"free_slots"`
	QueueDepth       int     `json:"queue_depth"`
	RunningTaskCount int     `json:"running_task_count"`
}

type Lease struct {
	WorkflowID     string     `json:"workflow_id"`
	TaskID         string     `json:"task_id"`
	AttemptID      string     `json:"attempt_id"`
	WorkerID       string     `json:"worker_id"`
	LeaseExpiresAt time.Time  `json:"lease_expires_at"`
	TimeoutAt      *time.Time `json:"timeout_at,omitempty"`
}

type TaskAttempt struct {
	AttemptID      string     `json:"attempt_id"`
	WorkflowID     string     `json:"workflow_id"`
	TaskID         string     `json:"task_id"`
	WorkerID       string     `json:"worker_id"`
	Attempt        int        `json:"attempt"`
	State          TaskState  `json:"state"`
	LeaseExpiresAt *time.Time `json:"lease_expires_at,omitempty"`
	ErrorMessage   string     `json:"error_message,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

type TaskResult struct {
	ResultID      string          `json:"result_id"`
	WorkflowID    string          `json:"workflow_id"`
	TaskID        string          `json:"task_id"`
	AttemptID     string          `json:"attempt_id"`
	WorkerID      string          `json:"worker_id"`
	Status        string          `json:"status"`
	OutputPayload json.RawMessage `json:"output_payload,omitempty"`
	ErrorMessage  string          `json:"error_message,omitempty"`
	RecordedAt    time.Time       `json:"recorded_at"`
}

type Assignment struct {
	WorkflowID     string          `json:"workflow_id"`
	TaskID         string          `json:"task_id"`
	TaskType       string          `json:"task_type"`
	AttemptID      string          `json:"attempt_id"`
	Attempt        int             `json:"attempt"`
	WorkerID       string          `json:"worker_id"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	LeaseExpiresAt time.Time       `json:"lease_expires_at"`
	InputPayload   json.RawMessage `json:"input_payload"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

type StartTaskRequest struct {
	WorkerID   string `json:"worker_id"`
	AttemptID  string `json:"attempt_id"`
	StartedAt  string `json:"started_at,omitempty"`
	TraceID    string `json:"trace_id,omitempty"`
	ReasonHint string `json:"reason_hint,omitempty"`
}

type CompleteTaskRequest struct {
	WorkerID      string          `json:"worker_id"`
	AttemptID     string          `json:"attempt_id"`
	OutputPayload json.RawMessage `json:"output_payload"`
	TraceID       string          `json:"trace_id,omitempty"`
}

type FailTaskRequest struct {
	WorkerID  string `json:"worker_id"`
	AttemptID string `json:"attempt_id"`
	Error     string `json:"error"`
	Retryable bool   `json:"retryable"`
	TraceID   string `json:"trace_id,omitempty"`
}

type HealthResponse struct {
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
	Version   string    `json:"version"`
}

type ReadyResponse struct {
	Status    string            `json:"status"`
	Timestamp time.Time         `json:"timestamp"`
	Checks    map[string]string `json:"checks"`
}

type TaskListResponse struct {
	WorkflowID string       `json:"workflow_id"`
	Tasks      []TaskRecord `json:"tasks"`
}

type WorkflowEventListResponse struct {
	WorkflowID string      `json:"workflow_id"`
	Events     []TaskEvent `json:"events"`
}

type WorkflowListResponse struct {
	Workflows []WorkflowSummary `json:"workflows"`
}

func ValidateWorkflowTerminal(tasks []TaskRecord) error {
	terminalSeen := 0
	for _, task := range tasks {
		switch task.State {
		case TaskStateSucceeded, TaskStateFailed, TaskStateCancelled:
			terminalSeen++
		}
	}
	if terminalSeen == 0 {
		return fmt.Errorf("workflow has no terminal task state")
	}
	return nil
}
