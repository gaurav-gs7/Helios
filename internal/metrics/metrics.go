package metrics

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	WorkflowsSubmitted prometheus.Counter
	TaskTransitions    *prometheus.CounterVec
	Assignments        prometheus.Counter
	Recoveries         prometheus.Counter
	Backpressure       *prometheus.CounterVec
	WorkerHeartbeats   prometheus.Counter
	WorkerGauge        prometheus.Gauge
	WorkerCPULoad      *prometheus.GaugeVec
	WorkerMemoryUsedMB *prometheus.GaugeVec
	WorkerFreeSlots    *prometheus.GaugeVec
	WorkerQueueDepth   *prometheus.GaugeVec
	TaskGauge          *prometheus.GaugeVec
}

func New(registry *prometheus.Registry) *Metrics {
	m := &Metrics{
		WorkflowsSubmitted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "helios",
			Subsystem: "control_plane",
			Name:      "workflows_submitted_total",
			Help:      "Total number of workflows accepted by the API.",
		}),
		TaskTransitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "helios",
			Subsystem: "control_plane",
			Name:      "task_transitions_total",
			Help:      "Number of task state transitions.",
		}, []string{"old_state", "new_state", "actor"}),
		Assignments: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "helios",
			Subsystem: "scheduler",
			Name:      "assignments_total",
			Help:      "Total number of task assignments dispatched to workers.",
		}),
		Recoveries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "helios",
			Subsystem: "scheduler",
			Name:      "recoveries_total",
			Help:      "Total number of recovered task attempts.",
		}),
		Backpressure: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "helios",
			Subsystem: "scheduler",
			Name:      "backpressure_total",
			Help:      "Total number of ready tasks left queued because no eligible worker was available.",
		}, []string{"reason"}),
		WorkerHeartbeats: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "helios",
			Subsystem: "workers",
			Name:      "heartbeats_total",
			Help:      "Total number of worker heartbeats received.",
		}),
		WorkerGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "helios",
			Subsystem: "workers",
			Name:      "healthy_workers",
			Help:      "Current number of healthy workers.",
		}),
		WorkerCPULoad: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "helios",
			Subsystem: "workers",
			Name:      "cpu_load",
			Help:      "Latest reported worker CPU load ratio from 0.0 to 1.0.",
		}, []string{"worker_id"}),
		WorkerMemoryUsedMB: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "helios",
			Subsystem: "workers",
			Name:      "memory_used_mb",
			Help:      "Latest reported worker memory usage in megabytes.",
		}, []string{"worker_id"}),
		WorkerFreeSlots: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "helios",
			Subsystem: "workers",
			Name:      "free_slots",
			Help:      "Latest reported free execution slots per worker.",
		}, []string{"worker_id"}),
		WorkerQueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "helios",
			Subsystem: "workers",
			Name:      "queue_depth",
			Help:      "Latest reported local worker assignment queue depth.",
		}, []string{"worker_id"}),
		TaskGauge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "helios",
			Subsystem: "tasks",
			Name:      "by_state",
			Help:      "Number of tasks by state.",
		}, []string{"state"}),
	}
	registry.MustRegister(
		m.WorkflowsSubmitted,
		m.TaskTransitions,
		m.Assignments,
		m.Recoveries,
		m.Backpressure,
		m.WorkerHeartbeats,
		m.WorkerGauge,
		m.WorkerCPULoad,
		m.WorkerMemoryUsedMB,
		m.WorkerFreeSlots,
		m.WorkerQueueDepth,
		m.TaskGauge,
	)
	return m
}
