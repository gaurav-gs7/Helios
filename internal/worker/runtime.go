package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	goruntime "runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	heliosv1 "github.com/gauravgs7/helios/helios/v1"
	"github.com/gauravgs7/helios/internal/config"
	"github.com/gauravgs7/helios/internal/dispatch"
	"github.com/gauravgs7/helios/internal/domain"
	"github.com/gauravgs7/helios/internal/handlers"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type Runtime struct {
	cfg         config.Config
	logger      *slog.Logger
	client      *http.Client
	grpcConn    *grpc.ClientConn
	grpcClient  heliosv1.ControlPlaneServiceClient
	dispatcher  *dispatch.Dispatcher
	worker      domain.WorkerSnapshot
	workerToken string
	handlerByID map[string]handlers.Handler
	capacity    int
	semaphore   chan struct{}
	running     atomic.Int64
	queued      atomic.Int64
	cpuSampler  cpuSampler
}

func New(cfg config.Config, logger *slog.Logger, dispatcher *dispatch.Dispatcher) *Runtime {
	return &Runtime{
		cfg:         cfg,
		logger:      logger,
		client:      &http.Client{Timeout: 15 * time.Second},
		dispatcher:  dispatcher,
		handlerByID: handlers.Builtins(),
	}
}

func (r *Runtime) Run(ctx context.Context, supportedTaskTypes []string, capacity int, cpuCapacityUnits int, memoryCapacityMB int) error {
	hostname, _ := os.Hostname()
	if capacity <= 0 {
		capacity = 1
	}
	if cpuCapacityUnits <= 0 {
		cpuCapacityUnits = capacity * 1000
	}
	if memoryCapacityMB <= 0 {
		memoryCapacityMB = 1024
	}
	r.capacity = capacity
	r.semaphore = make(chan struct{}, capacity)
	if err := r.initGRPCClient(ctx); err != nil {
		return err
	}
	defer r.closeGRPCClient()
	registration, err := r.register(ctx, domain.WorkerRegistration{
		Hostname:           hostname,
		Version:            r.cfg.Version,
		SupportedTaskTypes: supportedTaskTypes,
		Capacity:           capacity,
		CPUCapacityUnits:   cpuCapacityUnits,
		MemoryCapacityMB:   memoryCapacityMB,
	})
	if err != nil {
		return err
	}
	r.worker = registration.Worker
	r.workerToken = registration.Token
	sub, err := r.dispatcher.Subscribe(r.worker.WorkerID, func(msg *nats.Msg) {
		r.handleAssignment(ctx, msg.Data)
	})
	if err != nil {
		return fmt.Errorf("subscribe for assignments: %w", err)
	}
	defer sub.Unsubscribe()
	go r.heartbeatLoop(ctx)
	r.logger.Info("worker registered", "worker_id", r.worker.WorkerID, "types", supportedTaskTypes, "capacity", capacity, "cpu_capacity_units", cpuCapacityUnits, "memory_capacity_mb", memoryCapacityMB)
	<-ctx.Done()
	return nil
}

func (r *Runtime) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.WorkerHeartbeatInterval)
	defer ticker.Stop()
	for {
		if err := r.heartbeat(ctx); err != nil && ctx.Err() == nil {
			r.logger.Warn("send heartbeat", "worker_id", r.worker.WorkerID, "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Runtime) handleAssignment(ctx context.Context, body []byte) {
	var assignment domain.Assignment
	if err := json.Unmarshal(body, &assignment); err != nil {
		r.logger.Error("decode assignment", "err", err)
		return
	}
	handler, ok := r.handlerByID[assignment.TaskType]
	if !ok {
		r.reportFailure(ctx, assignment, fmt.Errorf("non-retryable: unknown task type %s", assignment.TaskType), false)
		return
	}
	r.queued.Add(1)
	go func() {
		select {
		case r.semaphore <- struct{}{}:
		case <-ctx.Done():
			r.queued.Add(-1)
			return
		}
		r.queued.Add(-1)
		r.running.Add(1)
		defer func() {
			r.running.Add(-1)
			<-r.semaphore
		}()
		if err := r.post(ctx, "/api/v1/workflows/"+assignment.WorkflowID+"/tasks/"+assignment.TaskID+"/start", domain.StartTaskRequest{
			WorkerID:  r.worker.WorkerID,
			AttemptID: assignment.AttemptID,
			StartedAt: time.Now().Format(time.RFC3339),
		}); err != nil {
			r.logger.Error("report task start", "task_id", assignment.TaskID, "err", err)
			return
		}
		execCtx, cancel := context.WithTimeout(ctx, time.Duration(assignment.TimeoutSeconds)*time.Second)
		defer cancel()
		output, err := handler(execCtx, handlers.ExecutionContext{Assignment: assignment})
		if err != nil {
			r.reportFailure(ctx, assignment, err, handlers.IsRetryable(err))
			return
		}
		if err := r.post(ctx, "/api/v1/workflows/"+assignment.WorkflowID+"/tasks/"+assignment.TaskID+"/complete", domain.CompleteTaskRequest{
			WorkerID:      r.worker.WorkerID,
			AttemptID:     assignment.AttemptID,
			OutputPayload: output,
		}); err != nil {
			r.logger.Error("report task completion", "task_id", assignment.TaskID, "err", err)
		}
	}()
}

func (r *Runtime) reportFailure(ctx context.Context, assignment domain.Assignment, err error, retryable bool) {
	if postErr := r.post(ctx, "/api/v1/workflows/"+assignment.WorkflowID+"/tasks/"+assignment.TaskID+"/fail", domain.FailTaskRequest{
		WorkerID:  r.worker.WorkerID,
		AttemptID: assignment.AttemptID,
		Error:     err.Error(),
		Retryable: retryable,
	}); postErr != nil {
		r.logger.Error("report task failure", "task_id", assignment.TaskID, "err", postErr, "original_err", err)
	}
}

func (r *Runtime) register(ctx context.Context, payload domain.WorkerRegistration) (domain.WorkerRegistrationResult, error) {
	if r.grpcClient != nil {
		resp, err := r.grpcClient.RegisterWorker(r.withBearerToken(ctx, r.cfg.WorkerBootstrapToken), &heliosv1.WorkerRegistration{
			Hostname:           payload.Hostname,
			Version:            payload.Version,
			SupportedTaskTypes: append([]string(nil), payload.SupportedTaskTypes...),
			Capacity:           int32(payload.Capacity),
			CpuCapacityUnits:   int32(payload.CPUCapacityUnits),
			MemoryCapacityMb:   int32(payload.MemoryCapacityMB),
		})
		if err != nil {
			return domain.WorkerRegistrationResult{}, err
		}
		return domain.WorkerRegistrationResult{
			Worker: domain.WorkerSnapshot{
				WorkerID:           resp.GetWorker().GetWorkerId(),
				Hostname:           resp.GetWorker().GetHostname(),
				Version:            resp.GetWorker().GetVersion(),
				SupportedTaskTypes: append([]string(nil), resp.GetWorker().GetSupportedTaskTypes()...),
				Capacity:           int(resp.GetWorker().GetCapacity()),
				RunningTaskCount:   int(resp.GetWorker().GetRunningTaskCount()),
				FreeSlots:          int(resp.GetWorker().GetFreeSlots()),
				QueueDepth:         int(resp.GetWorker().GetQueueDepth()),
				CPULoad:            resp.GetWorker().GetCpuLoad(),
				CPUCapacityUnits:   int(resp.GetWorker().GetCpuCapacityUnits()),
				AllocatedCPUUnits:  int(resp.GetWorker().GetAllocatedCpuUnits()),
				MemoryUsedMB:       int(resp.GetWorker().GetMemoryUsedMb()),
				MemoryCapacityMB:   int(resp.GetWorker().GetMemoryCapacityMb()),
				AllocatedMemoryMB:  int(resp.GetWorker().GetAllocatedMemoryMb()),
				LastHeartbeatAt:    resp.GetWorker().GetLastHeartbeatAt().AsTime(),
				Health:             domain.WorkerHealth(resp.GetWorker().GetHealth()),
				RegisteredAt:       resp.GetWorker().GetRegisteredAt().AsTime(),
			},
			Token: resp.GetWorkerToken(),
		}, nil
	}
	var registration domain.WorkerRegistrationResult
	if err := r.postAndDecode(ctx, "/api/v1/workers/register", payload, &registration, r.cfg.WorkerBootstrapToken); err != nil {
		return domain.WorkerRegistrationResult{}, err
	}
	return registration, nil
}

func (r *Runtime) heartbeat(ctx context.Context) error {
	payload := r.resourceSnapshot()
	if r.grpcClient != nil {
		_, err := r.grpcClient.HeartbeatWorker(r.withBearerToken(ctx, r.workerToken), &heliosv1.HeartbeatWorkerRequest{
			WorkerId: r.worker.WorkerID,
			Heartbeat: &heliosv1.WorkerHeartbeat{
				CpuLoad:          payload.CPULoad,
				MemoryUsedMb:     int32(payload.MemoryUsedMB),
				FreeSlots:        int32(payload.FreeSlots),
				QueueDepth:       int32(payload.QueueDepth),
				RunningTaskCount: int32(payload.RunningTaskCount),
			},
		})
		return err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.cfg.ControlPlaneURL+"/api/v1/workers/"+r.worker.WorkerID+"/heartbeat", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+r.workerToken)
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("heartbeat failed with %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (r *Runtime) resourceSnapshot() domain.WorkerHeartbeat {
	running := int(r.running.Load())
	queued := int(r.queued.Load())
	freeSlots := r.capacity - running - queued
	if freeSlots < 0 {
		freeSlots = 0
	}
	cpuLoad, ok := r.cpuSampler.sample()
	if !ok && r.capacity > 0 {
		cpuLoad = float64(running) / float64(r.capacity)
	}
	memStats := goruntime.MemStats{}
	goruntime.ReadMemStats(&memStats)
	return domain.WorkerHeartbeat{
		CPULoad:          cpuLoad,
		MemoryUsedMB:     int(memStats.Sys / 1024 / 1024),
		FreeSlots:        freeSlots,
		QueueDepth:       queued,
		RunningTaskCount: running,
	}
}

func (r *Runtime) post(ctx context.Context, path string, payload any) error {
	if r.grpcClient != nil {
		return r.postGRPC(ctx, path, payload)
	}
	return r.postAndDecode(ctx, path, payload, nil, r.workerToken)
}

func (r *Runtime) initGRPCClient(ctx context.Context) error {
	target := strings.TrimSpace(r.cfg.ControlPlaneGRPCAddress)
	if target == "" {
		return nil
	}
	conn, err := grpc.DialContext(
		ctx,
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("dial control-plane grpc: %w", err)
	}
	r.grpcConn = conn
	r.grpcClient = heliosv1.NewControlPlaneServiceClient(conn)
	return nil
}

func (r *Runtime) closeGRPCClient() {
	if r.grpcConn != nil {
		_ = r.grpcConn.Close()
	}
}

func (r *Runtime) withBearerToken(ctx context.Context, token string) context.Context {
	if strings.TrimSpace(token) == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func (r *Runtime) postGRPC(ctx context.Context, path string, payload any) error {
	callCtx := r.withBearerToken(ctx, r.workerToken)
	switch req := payload.(type) {
	case domain.StartTaskRequest:
		workflowID, taskID, err := workflowAndTaskFromPath(path)
		if err != nil {
			return err
		}
		_, err = r.grpcClient.StartTaskExecution(callCtx, &heliosv1.StartTaskExecutionRequest{
			WorkflowId: workflowID,
			TaskId:     taskID,
			WorkerId:   req.WorkerID,
			AttemptId:  req.AttemptID,
			StartedAt:  req.StartedAt,
			TraceId:    req.TraceID,
			ReasonHint: req.ReasonHint,
		})
		return err
	case domain.CompleteTaskRequest:
		workflowID, taskID, err := workflowAndTaskFromPath(path)
		if err != nil {
			return err
		}
		_, err = r.grpcClient.CompleteTaskExecution(callCtx, &heliosv1.CompleteTaskExecutionRequest{
			WorkflowId:    workflowID,
			TaskId:        taskID,
			WorkerId:      req.WorkerID,
			AttemptId:     req.AttemptID,
			OutputPayload: append([]byte(nil), req.OutputPayload...),
			TraceId:       req.TraceID,
		})
		return err
	case domain.FailTaskRequest:
		workflowID, taskID, err := workflowAndTaskFromPath(path)
		if err != nil {
			return err
		}
		_, err = r.grpcClient.FailTaskExecution(callCtx, &heliosv1.FailTaskExecutionRequest{
			WorkflowId: workflowID,
			TaskId:     taskID,
			WorkerId:   req.WorkerID,
			AttemptId:  req.AttemptID,
			Error:      req.Error,
			Retryable:  req.Retryable,
			TraceId:    req.TraceID,
		})
		return err
	default:
		return fmt.Errorf("unsupported grpc payload type %T", payload)
	}
}

func workflowAndTaskFromPath(path string) (string, string, error) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 6 {
		return "", "", fmt.Errorf("unexpected task path %q", path)
	}
	return parts[3], parts[5], nil
}

func (r *Runtime) postAndDecode(ctx context.Context, path string, payload any, target any, bearerToken string) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.cfg.ControlPlaneURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}
	if r.worker.WorkerID != "" {
		req.Header.Set("X-Helios-Worker-ID", r.worker.WorkerID)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("request %s failed with %d: %s", path, resp.StatusCode, string(respBody))
	}
	if target != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, target); err != nil {
			return fmt.Errorf("decode response for %s: %w", path, err)
		}
	}
	return nil
}

type cpuSampler struct {
	lastIdle  uint64
	lastTotal uint64
	ready     bool
}

func (s *cpuSampler) sample() (float64, bool) {
	idle, total, ok := readProcStatCPU()
	if !ok {
		return 0, false
	}
	if !s.ready {
		s.lastIdle = idle
		s.lastTotal = total
		s.ready = true
		return 0, false
	}
	idleDelta := idle - s.lastIdle
	totalDelta := total - s.lastTotal
	s.lastIdle = idle
	s.lastTotal = total
	if totalDelta == 0 {
		return 0, false
	}
	load := 1 - (float64(idleDelta) / float64(totalDelta))
	if load < 0 {
		load = 0
	}
	if load > 1 {
		load = 1
	}
	return load, true
}

func readProcStatCPU() (idle uint64, total uint64, ok bool) {
	body, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0, false
	}
	lines := strings.SplitN(string(body), "\n", 2)
	fields := strings.Fields(lines[0])
	if len(fields) < 5 || fields[0] != "cpu" {
		return 0, 0, false
	}
	values := make([]uint64, 0, len(fields)-1)
	for _, field := range fields[1:] {
		value, err := strconv.ParseUint(field, 10, 64)
		if err != nil {
			return 0, 0, false
		}
		values = append(values, value)
		total += value
	}
	idle = values[3]
	if len(values) > 4 {
		idle += values[4]
	}
	return idle, total, true
}
