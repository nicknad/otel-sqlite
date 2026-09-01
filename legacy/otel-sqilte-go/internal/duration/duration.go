// Package duration provides extended duration parsing shared by the config
// and maintenance packages. It exists to avoid the copy that previously
// lived in both packages (internal/config imports internal/maintenance, so
// a shared leaf package is required to deduplicate without a cycle).
package duration

import (
	"fmt"
	"time"
)

// Parse parses a duration string supporting Go durations and a "d" suffix
// for days. Examples: "30d", "24h", "1h30m", "5s".
func Parse(s string) (time.Duration, error) {
	// Try standard Go duration first.
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}

	// Custom handling for "d" suffix (days).
	if len(s) >= 2 && s[len(s)-1] == 'd' {
		daysStr := s[:len(s)-1]
		var days int
		if _, err := fmt.Sscanf(daysStr, "%d", &days); err != nil {
			return 0, fmt.Errorf("invalid duration %q: %w", s, err)
		}
		if days < 0 {
			return 0, fmt.Errorf("invalid duration %q: negative days not allowed", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return 0, fmt.Errorf("invalid duration %q", s)
}
