package main

import (
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	sorted := []time.Duration{
		1 * time.Millisecond,
		2 * time.Millisecond,
		3 * time.Millisecond,
		4 * time.Millisecond,
		5 * time.Millisecond,
	}

	tests := []struct {
		p    float64
		want time.Duration
	}{
		{0.0, 1 * time.Millisecond},
		{0.5, 3 * time.Millisecond},
		{0.99, 5 * time.Millisecond},
		{1.0, 5 * time.Millisecond}, // must clamp to the last element, not index out of range
	}
	for _, tt := range tests {
		if got := percentile(sorted, tt.p); got != tt.want {
			t.Errorf("percentile(sorted, %v) = %v, want %v", tt.p, got, tt.want)
		}
	}
}

func TestPercentile_Empty(t *testing.T) {
	if got := percentile(nil, 0.5); got != 0 {
		t.Errorf("percentile(nil, 0.5) = %v, want 0", got)
	}
}
