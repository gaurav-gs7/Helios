package scheduler

import (
	"testing"
	"time"

	"github.com/gauravgs7/helios/internal/domain"
)

func TestPickWorkerChoosesLeastLoadedEligibleWorker(t *testing.T) {
	workers := []domain.WorkerSnapshot{
		{
			WorkerID:           "worker-busy",
			SupportedTaskTypes: []string{"validate_records"},
			Capacity:           4,
			RunningTaskCount:   3,
			FreeSlots:          1,
			RegisteredAt:       time.Now(),
		},
		{
			WorkerID:           "worker-ready",
			SupportedTaskTypes: []string{"validate_records", "score_fraud_risk"},
			Capacity:           4,
			RunningTaskCount:   1,
			FreeSlots:          3,
			RegisteredAt:       time.Now(),
		},
	}

	selected, ok, _ := pickWorker(workers, domain.TaskRecord{TaskType: "validate_records"}, PolicyLeastLoaded)
	if !ok {
		t.Fatal("expected a worker to be selected")
	}
	if selected.WorkerID != "worker-ready" {
		t.Fatalf("expected least-loaded eligible worker, got %s", selected.WorkerID)
	}
}

func TestPickWorkerRejectsUnsupportedTaskTypes(t *testing.T) {
	workers := []domain.WorkerSnapshot{
		{
			WorkerID:           "worker-1",
			SupportedTaskTypes: []string{"validate_records"},
			Capacity:           2,
			FreeSlots:          2,
		},
	}

	if _, ok, _ := pickWorker(workers, domain.TaskRecord{TaskType: "score_fraud_risk"}, PolicyLeastLoaded); ok {
		t.Fatal("expected no worker to be selected for unsupported task type")
	}
}

func TestPickWorkerResourceAwareRespectsReservations(t *testing.T) {
	workers := []domain.WorkerSnapshot{
		{
			WorkerID:           "small-worker",
			SupportedTaskTypes: []string{"score_fraud_risk"},
			Capacity:           2,
			FreeSlots:          2,
			CPUCapacityUnits:   500,
			MemoryCapacityMB:   256,
		},
		{
			WorkerID:           "resource-fit-worker",
			SupportedTaskTypes: []string{"score_fraud_risk"},
			Capacity:           2,
			FreeSlots:          2,
			CPUCapacityUnits:   2000,
			MemoryCapacityMB:   1024,
			CPULoad:            0.25,
		},
	}

	task := domain.TaskRecord{TaskType: "score_fraud_risk", CPUUnits: 1000, MemoryMB: 512}
	selected, ok, reason := pickWorker(workers, task, PolicyResourceAware)
	if !ok {
		t.Fatalf("expected resource-aware policy to find a worker, reason=%s", reason)
	}
	if selected.WorkerID != "resource-fit-worker" {
		t.Fatalf("expected resource-fit-worker, got %s", selected.WorkerID)
	}
}

func TestPickWorkerPriorityFirstPrefersFreeSlots(t *testing.T) {
	workers := []domain.WorkerSnapshot{
		{
			WorkerID:           "nearly-full",
			SupportedTaskTypes: []string{"validate_records"},
			Capacity:           4,
			RunningTaskCount:   3,
			FreeSlots:          1,
			RegisteredAt:       time.Now().Add(-time.Minute),
		},
		{
			WorkerID:           "more-headroom",
			SupportedTaskTypes: []string{"validate_records"},
			Capacity:           4,
			RunningTaskCount:   1,
			FreeSlots:          3,
			RegisteredAt:       time.Now(),
		},
	}

	selected, ok, _ := pickWorker(workers, domain.TaskRecord{TaskType: "validate_records", Priority: 100}, PolicyPriorityFirst)
	if !ok {
		t.Fatal("expected a worker to be selected")
	}
	if selected.WorkerID != "more-headroom" {
		t.Fatalf("expected priority-first policy to prefer free slots, got %s", selected.WorkerID)
	}
}
