package dag

import (
	"fmt"

	"github.com/gaurav-gs7/helios/internal/domain"
)

func Validate(spec domain.WorkflowSpec) error {
	if spec.Name == "" {
		return fmt.Errorf("workflow name is required")
	}
	if len(spec.Tasks) == 0 {
		return fmt.Errorf("workflow must contain at least one task")
	}
	taskIndex := make(map[string]domain.TaskSpec, len(spec.Tasks))
	for _, task := range spec.Tasks {
		if task.TaskID == "" {
			return fmt.Errorf("task_id is required")
		}
		if task.TaskType == "" {
			return fmt.Errorf("task %q is missing task_type", task.TaskID)
		}
		if task.TimeoutSeconds <= 0 {
			return fmt.Errorf("task %q must declare timeout_seconds", task.TaskID)
		}
		if task.CPUUnits < 0 {
			return fmt.Errorf("task %q cpu_units cannot be negative", task.TaskID)
		}
		if task.MemoryMB < 0 {
			return fmt.Errorf("task %q memory_mb cannot be negative", task.TaskID)
		}
		if task.ExpectedDurationSeconds < 0 {
			return fmt.Errorf("task %q expected_duration_seconds cannot be negative", task.TaskID)
		}
		if _, exists := taskIndex[task.TaskID]; exists {
			return fmt.Errorf("duplicate task_id %q", task.TaskID)
		}
		taskIndex[task.TaskID] = task
	}
	for _, task := range spec.Tasks {
		for _, dep := range task.Dependencies {
			if dep == task.TaskID {
				return fmt.Errorf("task %q cannot depend on itself", task.TaskID)
			}
			if _, ok := taskIndex[dep]; !ok {
				return fmt.Errorf("task %q references unknown dependency %q", task.TaskID, dep)
			}
		}
	}
	if err := detectCycle(taskIndex); err != nil {
		return err
	}
	return nil
}

func detectCycle(index map[string]domain.TaskSpec) error {
	visiting := make(map[string]bool, len(index))
	visited := make(map[string]bool, len(index))

	var dfs func(string) error
	dfs = func(node string) error {
		if visiting[node] {
			return fmt.Errorf("cycle detected at task %q", node)
		}
		if visited[node] {
			return nil
		}
		visiting[node] = true
		task := index[node]
		for _, dep := range task.Dependencies {
			if err := dfs(dep); err != nil {
				return err
			}
		}
		visiting[node] = false
		visited[node] = true
		return nil
	}

	for taskID := range index {
		if err := dfs(taskID); err != nil {
			return err
		}
	}
	return nil
}
