package ratecounter

import (
	"context"
	"fmt"
	"io/ioutil"
	"testing"
	"time"
)

func TestRateCounter(t *testing.T) {
	interval := 50 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())

	r := NewRateCounter(ctx, interval)

	check := func(expected int64) {
		val := r.Rate()
		if val != expected {
			t.Error("Expected ", val, " to equal ", expected)
		}
	}

	check(0)
	r.Incr(1)
	check(1)
	r.Incr(2)
	check(3)
	time.Sleep(2 * interval)
	check(0)
	if r.running == 0 {
		t.Error("The RateCounter should continue to run even if its value is 0")
	}
	cancel()
	time.Sleep(20 * time.Millisecond)
	if r.running != 0 {
		t.Errorf("The RateCounter should not run anymore after its context has been cancelled (%d)", r.running)
	}

}

func TestRateCounterResetAndRestart(t *testing.T) {
	interval := 50 * time.Millisecond

	r := NewRateCounter(context.Background(), interval)

	check := func(expected int64) {
		val := r.Rate()
		if val != expected {
			t.Error("Expected ", val, " to equal ", expected)
		}
	}

	check(0)
	r.Incr(1)
	check(1)
	time.Sleep(2 * interval)
	check(0)
	time.Sleep(2 * interval)
	r.Incr(2)
	check(2)
	time.Sleep(2 * interval)
	check(0)
	r.Incr(2)
	check(2)
}

func TestRateCounterPartial(t *testing.T) {
	interval := 50 * time.Millisecond
	almost := 40 * time.Millisecond

	r := NewRateCounter(context.Background(), interval)

	check := func(expected int64) {
		val := r.Rate()
		if val != expected {
			t.Error("Expected ", val, " to equal ", expected)
		}
	}

	check(0)
	r.Incr(1)
	check(1)
	time.Sleep(almost)
	r.Incr(2)
	check(3)
	time.Sleep(almost)
	check(2)
	time.Sleep(2 * interval)
	check(0)
}

func TestRateCounterHighResolution(t *testing.T) {
	interval := 50 * time.Millisecond
	tenth := 5 * time.Millisecond

	r := NewRateCounter(context.Background(), interval).WithResolution(100)

	check := func(expected int64) {
		val := r.Rate()
		if val != expected {
			t.Error("Expected ", val, " to equal ", expected)
		}
	}

	check(0)
	r.Incr(1)
	check(1)
	time.Sleep(2 * tenth)
	r.Incr(1)
	check(2)
	time.Sleep(2 * tenth)
	r.Incr(1)
	check(3)
	time.Sleep(interval - 5*tenth)
	check(3)
	time.Sleep(2 * tenth)
	check(2)
	time.Sleep(2 * tenth)
	check(1)
	time.Sleep(2 * tenth)
	check(0)
}

func TestRateCounterLowResolution(t *testing.T) {
	interval := 50 * time.Millisecond
	tenth := 5 * time.Millisecond

	r := NewRateCounter(context.Background(), interval).WithResolution(4)

	check := func(expected int64) {
		val := r.Rate()
		if val != expected {
			t.Error("Expected ", val, " to equal ", expected)
		}
	}

	check(0)
	r.Incr(1)
	check(1)
	time.Sleep(2 * tenth)
	r.Incr(1)
	check(2)
	time.Sleep(2 * tenth)
	r.Incr(1)
	check(3)
	time.Sleep(interval - 5*tenth)
	check(3)
	time.Sleep(2 * tenth)
	check(1)
	time.Sleep(2 * tenth)
	check(0)
	time.Sleep(2 * tenth)
	check(0)
}

func TestRateCounterMinResolution(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Resolution < 1 did not panic")
		}
	}()

	NewRateCounter(context.Background(), 50*time.Millisecond).WithResolution(0)
}

func TestRateCounterNoResolution(t *testing.T) {
	interval := 50 * time.Millisecond
	tenth := 5 * time.Millisecond

	r := NewRateCounter(context.Background(), interval).WithResolution(1)

	check := func(expected int64) {
		val := r.Rate()
		if val != expected {
			t.Error("Expected ", val, " to equal ", expected)
		}
	}

	check(0)
	r.Incr(1)
	check(1)
	time.Sleep(2 * tenth)
	r.Incr(1)
	check(2)
	time.Sleep(2 * tenth)
	r.Incr(1)
	check(3)
	time.Sleep(interval - 5*tenth)
	check(3)
	time.Sleep(2 * tenth)
	check(0)
	time.Sleep(2 * tenth)
	check(0)
	time.Sleep(2 * tenth)
	check(0)
}

func TestRateCounter_String(t *testing.T) {
	r := NewRateCounter(context.Background(), 1*time.Second)
	if r.String() != "0" {
		t.Error("Expected ", r.String(), " to equal ", "0")
	}

	r.Incr(1)
	if r.String() != "1" {
		t.Error("Expected ", r.String(), " to equal ", "1")
	}
}

func TestRateCounter_Incr_ReturnsImmediately(t *testing.T) {
	interval := 1 * time.Second
	r := NewRateCounter(context.Background(), interval)

	start := time.Now()
	r.Incr(-1)
	duration := time.Since(start)

	if duration >= 1*time.Second {
		t.Error("incr took", duration, "to return")
	}
}

func BenchmarkRateCounter(b *testing.B) {
	interval := 0 * time.Millisecond
	r := NewRateCounter(context.Background(), interval)

	for i := 0; i < b.N; i++ {
		r.Incr(1)
		r.Rate()
	}
}

func BenchmarkRateCounter_Parallel(b *testing.B) {
	interval := 0 * time.Millisecond
	r := NewRateCounter(context.Background(), interval)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.Incr(1)
			r.Rate()
		}
	})
}

func BenchmarkRateCounter_With5MillionExisting(b *testing.B) {
	interval := 1 * time.Hour
	r := NewRateCounter(context.Background(), interval)

	for i := 0; i < 5000000; i++ {
		r.Incr(1)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r.Incr(1)
		r.Rate()
	}
}

func Benchmark_TimeNowAndAdd(b *testing.B) {
	var a time.Time
	for i := 0; i < b.N; i++ {
		a = time.Now().Add(1 * time.Second)
	}
	fmt.Fprintln(ioutil.Discard, a)
}
