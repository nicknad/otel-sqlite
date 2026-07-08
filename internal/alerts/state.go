package alerts

import "time"

// StateTransition describes what happened after evaluating alert state.
type StateTransition int

const (
	TransitionNone     StateTransition = iota // no change
	TransitionFiring                          // pending → firing (threshold crossed)
	TransitionResolved                        // firing → resolved (silence exceeded)
	TransitionGarbage                         // pending alert can be GC'd (no recent hits)
)

// EvaluateAlert determines whether an alert should change state based on
// the current counter value and the time since the last match.
//
// Rules:
//   - Pending + count >= threshold       → Firing
//   - Firing + zero count + silence >= rw → Resolved
//   - Resolved + re-trigger              → Firing again
//   - Pending + zero count + silence > rw → garbage collect
func EvaluateAlert(alert *Alert, count int, now int64, threshold int, resolveWindow time.Duration) StateTransition {
	switch alert.Status {
	case AlertPending:
		if count >= threshold {
			return TransitionFiring
		}
		silence := now - alert.LastMatched
		if count <= 1 && silence >= int64(resolveWindow) {
			// Never really fired, and been silent long enough — garbage.
			return TransitionGarbage
		}
		return TransitionNone

	case AlertFiring:
		if count >= threshold {
			// Still firing — update count but no transition.
			return TransitionNone
		}
		// Count dropped below threshold. Check silence duration.
		silence := now - alert.LastMatched
		if silence >= int64(resolveWindow) {
			return TransitionResolved
		}
		return TransitionNone

	case AlertResolved:
		// Re-trigger if threshold is met again.
		if count >= threshold {
			return TransitionFiring
		}
		return TransitionNone

	default:
		return TransitionNone
	}
}

// ApplyTransition modifies the alert in-place to reflect the given transition.
func ApplyTransition(alert *Alert, tr StateTransition, count int, now int64) {
	alert.Count = count
	alert.UpdatedAt = now

	switch tr {
	case TransitionFiring:
		alert.Status = AlertFiring
	case TransitionResolved:
		alert.Status = AlertResolved
	case TransitionGarbage:
		// Mark for GC; caller should delete.
		alert.Status = AlertPending // keep as pending so GC can find it
	default:
		// No state change, just update Count and UpdatedAt.
	}
}
