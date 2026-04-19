package domain

import "fmt"

var allowedTaskTransitions = map[TaskState]map[TaskState]struct{}{
	TaskStatePending: {
		TaskStateReady:     {},
		TaskStateCancelled: {},
	},
	TaskStateReady: {
		TaskStateLeased:    {},
		TaskStateCancelled: {},
	},
	TaskStateLeased: {
		TaskStateRunning:   {},
		TaskStateRetryWait: {},
		TaskStateTimedOut:  {},
		TaskStateCancelled: {},
	},
	TaskStateRunning: {
		TaskStateSucceeded: {},
		TaskStateFailed:    {},
		TaskStateRetryWait: {},
		TaskStateTimedOut:  {},
		TaskStateCancelled: {},
	},
	TaskStateRetryWait: {
		TaskStateReady:     {},
		TaskStateCancelled: {},
	},
	TaskStateTimedOut: {
		TaskStateRetryWait: {},
		TaskStateFailed:    {},
	},
	TaskStateFailed:    {},
	TaskStateSucceeded: {},
	TaskStateCancelled: {},
}

func ValidateTaskTransition(oldState, newState TaskState) error {
	if oldState == newState {
		return nil
	}
	transitions, ok := allowedTaskTransitions[oldState]
	if !ok {
		return fmt.Errorf("unknown old task state %q", oldState)
	}
	if _, ok := transitions[newState]; !ok {
		return fmt.Errorf("invalid task state transition %q -> %q", oldState, newState)
	}
	return nil
}
