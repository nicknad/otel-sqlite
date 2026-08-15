package alerts

import (
	"testing"
	"time"
)

func TestAddToSlice_Appends(t *testing.T) {
	got := AddToSlice(nil, 100, time.Minute)
	if len(got) != 1 || got[0] != 100 {
		t.Fatalf("AddToSlice(nil) = %v, want [100]", got)
	}
}

func TestAddToSlice_PrunesExpired(t *testing.T) {
	// Window is 1 minute; entries older than (ts - 1m) are pruned.
	got := AddToSlice([]int64{1, 2, 59_000_000_000, 61_000_000_000}, 121_000_000_000, time.Minute)
	want := []int64{61_000_000_000, 121_000_000_000}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestAddToSlice_PrunesAll(t *testing.T) {
	got := AddToSlice([]int64{1, 2, 3}, 121_000_000_000, time.Minute)
	if len(got) != 1 || got[0] != 121_000_000_000 {
		t.Fatalf("got %v, want [121000000000]", got)
	}
}
