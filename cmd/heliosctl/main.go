package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

const (
	defaultAPIURL = "http://localhost:8080"
)

type cliConfig struct {
	apiURL string
	token  string
	json   bool
}

type workflowSummary struct {
	WorkflowID string            `json:"workflow_id"`
	Name       string            `json:"name"`
	State      string            `json:"state"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

type workflowListResponse struct {
	Workflows []workflowSummary `json:"workflows"`
}

type taskRecord struct {
	TaskID                  string     `json:"task_id"`
	TaskType                string     `json:"task_type"`
	State                   string     `json:"state"`
	Attempt                 int        `json:"attempt"`
	MaxAttempts             int        `json:"max_attempts"`
	Priority                int        `json:"priority"`
	CPUUnits                int        `json:"cpu_units,omitempty"`
	MemoryMB                int        `json:"memory_mb,omitempty"`
	ExpectedDurationSeconds int        `json:"expected_duration_seconds,omitempty"`
	AssignedWorker          string     `json:"assigned_worker,omitempty"`
	LastError               string     `json:"last_error,omitempty"`
	CompletedAt             *time.Time `json:"completed_at,omitempty"`
}

type taskListResponse struct {
	WorkflowID string       `json:"workflow_id"`
	Tasks      []taskRecord `json:"tasks"`
}

type taskEvent struct {
	TaskID     string            `json:"task_id,omitempty"`
	Actor      string            `json:"actor"`
	OldState   string            `json:"old_state,omitempty"`
	NewState   string            `json:"new_state,omitempty"`
	Reason     string            `json:"reason,omitempty"`
	OccurredAt time.Time         `json:"occurred_at"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

type eventListResponse struct {
	WorkflowID string      `json:"workflow_id"`
	Events     []taskEvent `json:"events"`
}

type workerSnapshot struct {
	WorkerID           string    `json:"worker_id"`
	Hostname           string    `json:"hostname"`
	Version            string    `json:"version"`
	Health             string    `json:"health"`
	Capacity           int       `json:"capacity"`
	RunningTaskCount   int       `json:"running_task_count"`
	FreeSlots          int       `json:"free_slots"`
	QueueDepth         int       `json:"queue_depth"`
	CPULoad            float64   `json:"cpu_load"`
	CPUCapacityUnits   int       `json:"cpu_capacity_units"`
	AllocatedCPUUnits  int       `json:"allocated_cpu_units"`
	MemoryUsedMB       int       `json:"memory_used_mb"`
	MemoryCapacityMB   int       `json:"memory_capacity_mb"`
	AllocatedMemoryMB  int       `json:"allocated_memory_mb"`
	LastHeartbeatAt    time.Time `json:"last_heartbeat_at"`
	SupportedTaskTypes []string  `json:"supported_task_types"`
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	var err error
	switch os.Args[1] {
	case "submit":
		err = submit(os.Args[2:])
	case "status":
		err = workflowShow(os.Args[2:])
	case "workflows":
		err = workflows(os.Args[2:])
	case "workflow":
		err = workflow(os.Args[2:])
	case "workers":
		err = workers(os.Args[2:])
	case "planner":
		err = planner(os.Args[2:])
	default:
		usage()
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func submit(args []string) error {
	fs, cfg := commandFlags("submit")
	file := fs.String("file", "", "path to workflow spec JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *file == "" {
		return fmt.Errorf("-file is required")
	}
	body, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	respBody, err := request(http.MethodPost, cfg, "/api/v1/workflows", body)
	if err != nil {
		return err
	}
	return printJSON(respBody)
}

func workflows(args []string) error {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: heliosctl workflows list [flags]")
		return flag.ErrHelp
	}
	switch args[0] {
	case "list":
		return workflowsList(args[1:])
	default:
		return fmt.Errorf("unknown workflows command %q", args[0])
	}
}

func workflowsList(args []string) error {
	fs, cfg := commandFlags("workflows list")
	state := fs.String("state", "", "optional workflow state filter")
	limit := fs.Int("limit", 20, "maximum workflows to return")
	if err := fs.Parse(args); err != nil {
		return err
	}
	query := url.Values{}
	if *state != "" {
		query.Set("state", *state)
	}
	query.Set("limit", strconv.Itoa(*limit))
	path := "/api/v1/workflows?" + query.Encode()
	body, err := request(http.MethodGet, cfg, path, nil)
	if err != nil {
		return err
	}
	if cfg.json {
		return printJSON(body)
	}
	var out workflowListResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return err
	}
	w := newTabWriter()
	fmt.Fprintln(w, "WORKFLOW ID\tSTATE\tNAME\tCREATED")
	for _, workflow := range out.Workflows {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", workflow.WorkflowID, workflow.State, workflow.Name, formatTime(workflow.CreatedAt))
	}
	return w.Flush()
}

func workflow(args []string) error {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: heliosctl workflow <show|tasks|events|cancel> ...")
		return flag.ErrHelp
	}
	switch args[0] {
	case "show":
		return workflowShow(args[1:])
	case "tasks":
		return workflowTasks(args[1:])
	case "events":
		return workflowEvents(args[1:])
	case "cancel":
		return workflowCancel(args[1:])
	default:
		return fmt.Errorf("unknown workflow command %q", args[0])
	}
}

func workflowShow(args []string) error {
	fs, cfg := commandFlags("workflow show")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: heliosctl workflow show <workflow-id>")
	}
	body, err := request(http.MethodGet, cfg, "/api/v1/workflows/"+fs.Arg(0), nil)
	if err != nil {
		return err
	}
	if cfg.json {
		return printJSON(body)
	}
	var workflow workflowSummary
	if err := json.Unmarshal(body, &workflow); err != nil {
		return err
	}
	w := newTabWriter()
	fmt.Fprintln(w, "WORKFLOW ID\tSTATE\tNAME\tCREATED\tUPDATED")
	fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", workflow.WorkflowID, workflow.State, workflow.Name, formatTime(workflow.CreatedAt), formatTime(workflow.UpdatedAt))
	return w.Flush()
}

func workflowTasks(args []string) error {
	fs, cfg := commandFlags("workflow tasks")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: heliosctl workflow tasks <workflow-id>")
	}
	body, err := request(http.MethodGet, cfg, "/api/v1/workflows/"+fs.Arg(0)+"/tasks", nil)
	if err != nil {
		return err
	}
	if cfg.json {
		return printJSON(body)
	}
	var out taskListResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return err
	}
	w := newTabWriter()
	fmt.Fprintln(w, "TASK ID\tSTATE\tTYPE\tATTEMPT\tPRIORITY\tCPU\tMEMORY\tWORKER\tERROR")
	for _, task := range out.Tasks {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d/%d\t%d\t%d\t%dMB\t%s\t%s\n",
			task.TaskID,
			task.State,
			task.TaskType,
			task.Attempt,
			task.MaxAttempts,
			task.Priority,
			task.CPUUnits,
			task.MemoryMB,
			shortID(task.AssignedWorker),
			task.LastError,
		)
	}
	return w.Flush()
}

func workflowEvents(args []string) error {
	fs, cfg := commandFlags("workflow events")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: heliosctl workflow events <workflow-id>")
	}
	body, err := request(http.MethodGet, cfg, "/api/v1/workflows/"+fs.Arg(0)+"/events", nil)
	if err != nil {
		return err
	}
	if cfg.json {
		return printJSON(body)
	}
	var out eventListResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return err
	}
	w := newTabWriter()
	fmt.Fprintln(w, "TIME\tTASK\tACTOR\tTRANSITION\tREASON")
	for _, event := range out.Events {
		transition := event.NewState
		if event.OldState != "" {
			transition = event.OldState + " -> " + event.NewState
		}
		taskID := event.TaskID
		if taskID == "" {
			taskID = "<workflow>"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", formatTime(event.OccurredAt), taskID, event.Actor, transition, event.Reason)
	}
	return w.Flush()
}

func workflowCancel(args []string) error {
	fs, cfg := commandFlags("workflow cancel")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: heliosctl workflow cancel <workflow-id>")
	}
	body, err := request(http.MethodPost, cfg, "/api/v1/workflows/"+fs.Arg(0)+"/cancel", nil)
	if err != nil {
		return err
	}
	return printJSON(body)
}

func workers(args []string) error {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: heliosctl workers list [flags]")
		return flag.ErrHelp
	}
	switch args[0] {
	case "list":
		return workersList(args[1:])
	default:
		return fmt.Errorf("unknown workers command %q", args[0])
	}
}

func workersList(args []string) error {
	fs, cfg := commandFlags("workers list")
	if err := fs.Parse(args); err != nil {
		return err
	}
	body, err := request(http.MethodGet, cfg, "/api/v1/workers", nil)
	if err != nil {
		return err
	}
	if cfg.json {
		return printJSON(body)
	}
	var workers []workerSnapshot
	if err := json.Unmarshal(body, &workers); err != nil {
		return err
	}
	w := newTabWriter()
	fmt.Fprintln(w, "WORKER ID\tHEALTH\tSLOTS\tCPU LOAD\tCPU\tMEMORY\tQUEUE\tLAST HEARTBEAT")
	for _, worker := range workers {
		fmt.Fprintf(w, "%s\t%s\t%d/%d\t%.2f\t%d/%d\t%d/%dMB\t%d\t%s\n",
			shortID(worker.WorkerID),
			worker.Health,
			worker.FreeSlots,
			worker.Capacity,
			worker.CPULoad,
			worker.AllocatedCPUUnits,
			worker.CPUCapacityUnits,
			worker.AllocatedMemoryMB,
			worker.MemoryCapacityMB,
			worker.QueueDepth,
			formatTime(worker.LastHeartbeatAt),
		)
	}
	return w.Flush()
}

func planner(args []string) error {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: heliosctl planner <intent|dry-run> -file <request.json>")
		return flag.ErrHelp
	}
	switch args[0] {
	case "intent":
		return plannerPost(args[1:], "/api/v1/planner/intent", "planner intent")
	case "dry-run":
		return plannerPost(args[1:], "/api/v1/planner/dry-run", "planner dry-run")
	default:
		return fmt.Errorf("unknown planner command %q", args[0])
	}
}

func plannerPost(args []string, path string, name string) error {
	fs, cfg := commandFlags(name)
	file := fs.String("file", "", "path to planner request JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *file == "" {
		return fmt.Errorf("-file is required")
	}
	payload, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	body, err := request(http.MethodPost, cfg, path, payload)
	if err != nil {
		return err
	}
	return printJSON(body)
}

func commandFlags(name string) (*flag.FlagSet, *cliConfig) {
	cfg := &cliConfig{}
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.StringVar(&cfg.apiURL, "api", env("HELIOS_API_URL", defaultAPIURL), "control plane API base URL")
	fs.StringVar(&cfg.token, "token", env("HELIOS_ADMIN_TOKEN", ""), "admin bearer token")
	fs.BoolVar(&cfg.json, "json", false, "print raw JSON response")
	return fs, cfg
}

func request(method string, cfg *cliConfig, path string, payload []byte) ([]byte, error) {
	req, err := http.NewRequest(method, strings.TrimRight(cfg.apiURL, "/")+path, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if cfg.token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("%s %s failed with %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

func printJSON(body []byte) error {
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, body, "", "  "); err != nil {
		_, _ = os.Stdout.Write(body)
		return err
	}
	pretty.WriteByte('\n')
	_, err := pretty.WriteTo(os.Stdout)
	return err
}

func newTabWriter() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Local().Format("2006-01-02 15:04:05")
}

func shortID(id string) string {
	if id == "" {
		return "-"
	}
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func usage() {
	fmt.Println("heliosctl submit -file examples/workflow.json [-token <admin-token>]")
	fmt.Println("heliosctl workflows list [-state succeeded] [-limit 20]")
	fmt.Println("heliosctl workflow show <workflow-id>")
	fmt.Println("heliosctl workflow tasks <workflow-id>")
	fmt.Println("heliosctl workflow events <workflow-id>")
	fmt.Println("heliosctl workflow cancel <workflow-id>")
	fmt.Println("heliosctl workers list")
	fmt.Println("heliosctl planner intent -file examples/intent_request.json")
	fmt.Println("heliosctl planner dry-run -file examples/intent_request.json")
	fmt.Println()
	fmt.Println("Global flags: -api, -token, -json")
	fmt.Println("Env defaults: HELIOS_API_URL, HELIOS_ADMIN_TOKEN")
}
