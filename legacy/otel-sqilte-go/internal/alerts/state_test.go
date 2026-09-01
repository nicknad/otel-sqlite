package alerts

import (
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

func alertFixture(status AlertStatus, lastMatched int64) *Alert {
	return &Alert{
		ID:          "test-rule:res-1",
		RuleID:      "test-rule",
		ResourceID:  "res-1",
		Status:      status,
		Severity:    model.SeverityError,
		OpenedAt:    lastMatched - int64(time.Minute),
		UpdatedAt:   lastMatched,
		LastMatched: lastMatched,
		Count:       0,
	}
}

func TestEvaluateAlert_PendingToFiring(t *testing.T) {
	now := time.Now().UnixNano()
	alert := alertFixture(AlertPending, now-int64(time.Second))
	threshold := 3
	resolveWindow := 15 * time.Minute

	// Count=2, below threshold — no transition.
	tr := EvaluateAlert(alert, 2, now, threshold, resolveWindow)
	if tr != TransitionNone {
		t.Errorf("expected TransitionNone, got %v", tr)
	}

	// Count=3, at threshold — fires.
	tr = EvaluateAlert(alert, 3, now, threshold, resolveWindow)
	if tr != TransitionFiring {
		t.Errorf("expected TransitionFiring, got %v", tr)
	}
}

func TestEvaluateAlert_FiringToResolved(t *testing.T) {
	now := time.Now().UnixNano()
	alert := alertFixture(AlertFiring, now-int64(20*time.Minute))
	threshold := 3
	resolveWindow := 15 * time.Minute

	// Silence longer than resolve window, count=0.
	tr := EvaluateAlert(alert, 0, now, threshold, resolveWindow)
	if tr != TransitionResolved {
		t.Errorf("expected TransitionResolved, got %v", tr)
	}

	// Silence shorter than resolve window — stays firing.
	alert2 := alertFixture(AlertFiring, now-int64(1*time.Minute))
	tr = EvaluateAlert(alert2, 1, now, threshold, resolveWindow)
	if tr != TransitionNone {
		t.Errorf("expected TransitionNone for recent firing, got %v", tr)
	}
}

func TestEvaluateAlert_ResolvedToFiring(t *testing.T) {
	now := time.Now().UnixNano()
	alert := alertFixture(AlertResolved, now-int64(1*time.Minute))
	threshold := 3
	resolveWindow := 15 * time.Minute

	// Re-trigger.
	tr := EvaluateAlert(alert, 5, now, threshold, resolveWindow)
	if tr != TransitionFiring {
		t.Errorf("expected TransitionFiring from resolved, got %v", tr)
	}

	// No re-trigger — stays resolved.
	tr = EvaluateAlert(alert, 0, now, threshold, resolveWindow)
	if tr != TransitionNone {
		t.Errorf("expected TransitionNone, got %v", tr)
	}
}

func TestEvaluateAlert_PendingGarbage(t *testing.T) {
	now := time.Now().UnixNano()
	alert := alertFixture(AlertPending, now-int64(20*time.Minute))
	threshold := 3
	resolveWindow := 15 * time.Minute

	// Never fired, count=0 (initial hit), long silence.
	tr := EvaluateAlert(alert, 0, now, threshold, resolveWindow)
	if tr != TransitionGarbage {
		t.Errorf("expected TransitionGarbage for stale pending, got %v", tr)
	}

	// count=1 with long silence also garbage (only the initial hit, then nothing).
	alert2 := alertFixture(AlertPending, now-int64(20*time.Minute))
	tr = EvaluateAlert(alert2, 1, now, threshold, resolveWindow)
	if tr != TransitionGarbage {
		t.Errorf("expected TransitionGarbage for stale pending, got %v", tr)
	}
}

func TestApplyTransition_Firing(t *testing.T) {
	now := time.Now().UnixNano()
	alert := alertFixture(AlertPending, now)

	ApplyTransition(alert, TransitionFiring, 5, now)
	if alert.Status != AlertFiring {
		t.Errorf("status = %v, want firing", alert.Status)
	}
	if alert.Count != 5 {
		t.Errorf("count = %d, want 5", alert.Count)
	}
	if alert.UpdatedAt != now {
		t.Errorf("UpdatedAt not set")
	}
}

func TestApplyTransition_Resolved(t *testing.T) {
	now := time.Now().UnixNano()
	alert := alertFixture(AlertFiring, now)

	ApplyTransition(alert, TransitionResolved, 0, now)
	if alert.Status != AlertResolved {
		t.Errorf("status = %v, want resolved", alert.Status)
	}
}

func TestAlertStatusString(t *testing.T) {
	if AlertPending.String() != "pending" {
		t.Errorf("pending string = %q", AlertPending.String())
	}
	if AlertFiring.String() != "firing" {
		t.Errorf("firing string = %q", AlertFiring.String())
	}
	if AlertResolved.String() != "resolved" {
		t.Errorf("resolved string = %q", AlertResolved.String())
	}
}
