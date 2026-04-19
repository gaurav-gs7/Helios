package domain

import "testing"

func TestValidateTaskTransitionAllowsExpectedPath(t *testing.T) {
	if err := ValidateTaskTransition(TaskStateReady, TaskStateLeased); err != nil {
		t.Fatalf("expected ready -> leased to be allowed, got %v", err)
	}
}

func TestValidateTaskTransitionRejectsTerminalRegression(t *testing.T) {
	if err := ValidateTaskTransition(TaskStateSucceeded, TaskStateRunning); err == nil {
		t.Fatal("expected succeeded -> running to be rejected")
	}
}
