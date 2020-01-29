package ratecounter

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"
)

// A RateCounter is a thread-safe counter which returns the number of times
// 'Incr' has been called in the last interval
type RateCounter struct {
	counter    Counter
	interval   time.Duration
	resolution int
	partials   []Counter
	current    int32
	running    int32
	ctx        context.Context
}

// NewRateCounter Constructs a new RateCounter, for the interval provided
func NewRateCounter(ctx context.Context, intrvl time.Duration) *RateCounter {
	ratecounter := &RateCounter{
		interval: intrvl,
		running:  0,
		ctx:      ctx,
	}

	return ratecounter.WithResolution(20)
}

// WithResolution determines the minimum resolution of this counter, default is 20
func (r *RateCounter) WithResolution(resolution int) *RateCounter {
	if resolution < 1 {
		panic("RateCounter resolution cannot be less than 1")
	}

	r.resolution = resolution
	r.partials = make([]Counter, resolution)
	r.current = 0

	return r
}

func (r *RateCounter) run() {
	if ok := atomic.CompareAndSwapInt32(&r.running, 0, 1); !ok {
		return
	}

	go func() {
		ticker := time.NewTicker(time.Duration(float64(r.interval) / float64(r.resolution)))

		for {
			select {
			case <-r.ctx.Done():
				atomic.StoreInt32(&r.running, 0)
				ticker.Stop()
				return
			case <-ticker.C:
				current := atomic.LoadInt32(&r.current)
				next := (int(current) + 1) % r.resolution
				r.counter.Incr(-1 * r.partials[next].Value())
				r.partials[next].Reset()
				atomic.CompareAndSwapInt32(&r.current, current, int32(next))
			}
		}
	}()
}

// Incr Add an event into the RateCounter
func (r *RateCounter) Incr(val int64) {
	r.counter.Incr(val)
	r.partials[atomic.LoadInt32(&r.current)].Incr(val)
	r.run()
}

// Rate Return the current number of events in the last interval
func (r *RateCounter) Rate() int64 {
	return r.counter.Value()
}

func (r *RateCounter) String() string {
	return strconv.FormatInt(r.counter.Value(), 10)
}
