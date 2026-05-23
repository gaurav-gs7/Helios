package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/gaurav-gs7/helios/internal/dispatch"
	"github.com/gaurav-gs7/helios/internal/domain"
	"github.com/gaurav-gs7/helios/internal/store/postgres"
)

type Scheduler struct {
	store       *postgres.Store
	dispatcher  *dispatch.Dispatcher
	logger      *slog.Logger
	leasePeriod time.Duration
	policy      PlacementPolicy
}

type PlacementPolicy string

const (
	PolicyLeastLoaded   PlacementPolicy = "least-loaded"
	PolicyResourceAware PlacementPolicy = "resource-aware"
	PolicyPriorityFirst PlacementPolicy = "priority-first"
)

func NormalizePolicy(policy string) PlacementPolicy {
	switch PlacementPolicy(strings.TrimSpace(strings.ToLower(policy))) {
	case PolicyLeastLoaded:
		return PolicyLeastLoaded
	case PolicyPriorityFirst:
		return PolicyPriorityFirst
	case PolicyResourceAware:
		return PolicyResourceAware
	default:
		return PolicyResourceAware
	}
}

func New(store *postgres.Store, dispatcher *dispatch.Dispatcher, logger *slog.Logger, leasePeriod time.Duration, policy string) *Scheduler {
	return &Scheduler{
		store:       store,
		dispatcher:  dispatcher,
		logger:      logger,
		leasePeriod: leasePeriod,
		policy:      NormalizePolicy(policy),
	}
}

func (s *Scheduler) Tick(ctx context.Context) error {
	tasks, err := s.store.ListReadyTasks(ctx, 64)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		return nil
	}
	workers, err := s.store.ListSchedulableWorkers(ctx)
	if err != nil {
		return err
	}
	if len(workers) == 0 {
		s.recordBackpressure("no_healthy_workers", domain.TaskRecord{})
		return nil
	}
	for _, task := range tasks {
		worker, ok, reason := pickWorker(workers, task, s.policy)
		if !ok {
			s.recordBackpressure(reason, task)
			continue
		}
		assignment, err := s.store.LeaseTask(ctx, task, worker, s.leasePeriod, string(s.policy))
		if err != nil {
			s.logger.Debug("lease task", "workflow_id", task.WorkflowID, "task_id", task.TaskID, "err", err)
			continue
		}
		if err := s.dispatcher.PublishAssignment(ctx, assignment); err != nil {
			s.logger.Error("publish assignment", "workflow_id", assignment.WorkflowID, "task_id", assignment.TaskID, "worker_id", assignment.WorkerID, "err", err)
			continue
		}
		for i := range workers {
			if workers[i].WorkerID == worker.WorkerID {
				workers[i].RunningTaskCount++
				if workers[i].FreeSlots > 0 {
					workers[i].FreeSlots--
				}
				workers[i].AllocatedCPUUnits += task.CPUUnits
				workers[i].AllocatedMemoryMB += task.MemoryMB
			}
		}
	}
	return nil
}

func (s *Scheduler) recordBackpressure(reason string, task domain.TaskRecord) {
	if reason == "" {
		reason = "no_eligible_worker"
	}
	s.store.RecordSchedulerBackpressure(reason)
	args := []any{"reason", reason, "policy", string(s.policy)}
	if task.TaskID != "" {
		args = append(args,
			"workflow_id", task.WorkflowID,
			"task_id", task.TaskID,
			"task_type", task.TaskType,
			"priority", task.Priority,
			"cpu_units", task.CPUUnits,
			"memory_mb", task.MemoryMB,
		)
	}
	s.logger.Warn("scheduler backpressure: task remains ready", args...)
}

func pickWorker(workers []domain.WorkerSnapshot, task domain.TaskRecord, policy PlacementPolicy) (domain.WorkerSnapshot, bool, string) {
	var selected domain.WorkerSnapshot
	found := false
	reason := "no_eligible_worker"
	for _, worker := range workers {
		eligible, ineligibleReason := eligibleWorker(worker, task, policy)
		if !eligible {
			reason = ineligibleReason
			continue
		}
		if !found || betterWorker(worker, selected, task, policy) {
			selected = worker
			found = true
		}
	}
	return selected, found, reason
}

func eligibleWorker(worker domain.WorkerSnapshot, task domain.TaskRecord, policy PlacementPolicy) (bool, string) {
	if !slices.Contains(worker.SupportedTaskTypes, task.TaskType) {
		return false, "unsupported_task_type"
	}
	if availableSlots(worker) <= 0 {
		return false, "worker_at_capacity"
	}
	if task.CPUUnits > 0 && worker.CPUCapacityUnits > 0 && worker.AllocatedCPUUnits+task.CPUUnits > worker.CPUCapacityUnits {
		return false, "insufficient_cpu"
	}
	if task.MemoryMB > 0 && worker.MemoryCapacityMB > 0 && worker.AllocatedMemoryMB+task.MemoryMB > worker.MemoryCapacityMB {
		return false, "insufficient_memory"
	}
	if policy == PolicyResourceAware && worker.CPULoad >= 0.95 {
		return false, "high_cpu_load"
	}
	return true, ""
}

func betterWorker(candidate, selected domain.WorkerSnapshot, task domain.TaskRecord, policy PlacementPolicy) bool {
	switch policy {
	case PolicyResourceAware:
		candidateMem := memoryUtilization(candidate, task.MemoryMB)
		selectedMem := memoryUtilization(selected, task.MemoryMB)
		if candidate.CPULoad != selected.CPULoad {
			return candidate.CPULoad < selected.CPULoad
		}
		if candidateMem != selectedMem {
			return candidateMem < selectedMem
		}
		if candidate.QueueDepth != selected.QueueDepth {
			return candidate.QueueDepth < selected.QueueDepth
		}
		if availableSlots(candidate) != availableSlots(selected) {
			return availableSlots(candidate) > availableSlots(selected)
		}
	case PolicyPriorityFirst:
		if availableSlots(candidate) != availableSlots(selected) {
			return availableSlots(candidate) > availableSlots(selected)
		}
		if candidate.RunningTaskCount != selected.RunningTaskCount {
			return candidate.RunningTaskCount < selected.RunningTaskCount
		}
	default:
		if candidate.RunningTaskCount != selected.RunningTaskCount {
			return candidate.RunningTaskCount < selected.RunningTaskCount
		}
		if candidate.QueueDepth != selected.QueueDepth {
			return candidate.QueueDepth < selected.QueueDepth
		}
	}
	return candidate.RegisteredAt.Before(selected.RegisteredAt)
}

func availableSlots(worker domain.WorkerSnapshot) int {
	dbFreeSlots := worker.Capacity - worker.RunningTaskCount
	if dbFreeSlots < 0 {
		dbFreeSlots = 0
	}
	if worker.FreeSlots <= 0 || worker.FreeSlots > dbFreeSlots {
		return dbFreeSlots
	}
	return worker.FreeSlots
}

func memoryUtilization(worker domain.WorkerSnapshot, taskMemoryMB int) float64 {
	if worker.MemoryCapacityMB <= 0 {
		return 0
	}
	return float64(worker.AllocatedMemoryMB+taskMemoryMB) / float64(worker.MemoryCapacityMB)
}

func RunLoop(ctx context.Context, logger *slog.Logger, period time.Duration, fn func(context.Context) error) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		if err := fn(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("background loop tick failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
