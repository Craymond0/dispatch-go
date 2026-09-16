package tracker

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// hostLimiter is a token bucket per hostname, so a sweep that fans out into
// many polls cannot hammer one board API. It is deliberately simple: no
// dependency, no background goroutine, state only for hosts seen.
type hostLimiter struct {
	mu      sync.Mutex
	rate    float64 // tokens per second
	burst   float64
	buckets map[string]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

func newHostLimiter(perSecond, burst float64) *hostLimiter {
	return &hostLimiter{rate: perSecond, burst: burst, buckets: map[string]*bucket{}}
}

// wait blocks until a token is available for host or ctx ends.
func (l *hostLimiter) wait(ctx context.Context, host string) error {
	if l == nil {
		return nil
	}
	for {
		l.mu.Lock()
		b, ok := l.buckets[host]
		now := time.Now()
		if !ok {
			b = &bucket{tokens: l.burst, last: now}
			l.buckets[host] = b
		}
		b.tokens += now.Sub(b.last).Seconds() * l.rate
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.last = now
		if b.tokens >= 1 {
			b.tokens--
			l.mu.Unlock()
			return nil
		}
		wait := time.Duration((1 - b.tokens) / l.rate * float64(time.Second))
		l.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

// do sends a request through the per-host limiter.
func (t *Tracker) do(req *http.Request) (*http.Response, error) {
	if err := t.limiter.wait(req.Context(), req.URL.Host); err != nil {
		return nil, err
	}
	return t.HTTP.Do(req)
}
