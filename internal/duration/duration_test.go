package duration

import (
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    time.Duration
		wantErr bool
	}{
		{"standard", "24h", 24 * time.Hour, false},
		{"compound", "1h30m", 90 * time.Minute, false},
		{"seconds", "5s", 5 * time.Second, false},
		{"days", "30d", 30 * 24 * time.Hour, false},
		{"single day", "1d", 24 * time.Hour, false},
		{"zero days", "0d", 0, false},
		{"negative days", "-2d", 0, true},
		{"bad days", "xd", 0, true},
		{"bare d", "d", 0, true},
		{"garbage", "soon", 0, true},
		{"empty", "", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Parse(%q) = %v, want error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("Parse(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
