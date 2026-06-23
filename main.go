package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type waiter struct {
	ch  chan struct{}
	msg string
}

type queue struct {
	messages []string
	waiters  []*waiter
}

type broker struct {
	mu     sync.Mutex
	queues map[string]*queue
}

func newBroker() *broker {
	return &broker{queues: make(map[string]*queue)}
}

func (b *broker) getQueue(name string) *queue {
	q := b.queues[name]
	if q == nil {
		q = &queue{}
		b.queues[name] = q
	}
	return q
}

func (b *broker) put(name, msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	q := b.getQueue(name)

	if len(q.waiters) > 0 {
		w := q.waiters[0]
		q.waiters = q.waiters[1:]
		w.msg = msg
		close(w.ch)
		return
	}

	q.messages = append(q.messages, msg)
}

func (b *broker) get(name string, timeout time.Duration, wait bool, done <-chan struct{}) (string, bool) {
	b.mu.Lock()

	q := b.getQueue(name)

	if len(q.messages) > 0 {
		msg := q.messages[0]
		q.messages = q.messages[1:]
		b.mu.Unlock()
		return msg, true
	}

	if !wait {
		b.mu.Unlock()
		return "", false
	}

	w := &waiter{ch: make(chan struct{})}
	q.waiters = append(q.waiters, w)
	b.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-w.ch:
		return w.msg, true

	case <-timer.C:
		if b.removeWaiter(name, w) {
			return "", false
		}
		return w.msg, true

	case <-done:
		if b.removeWaiter(name, w) {
			return "", false
		}
		return w.msg, true
	}
}

func (b *broker) removeWaiter(name string, target *waiter) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	q := b.queues[name]
	if q == nil {
		return false
	}

	for i, w := range q.waiters {
		if w == target {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			return true
		}
	}

	return false
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: go run main.go <port>")
		os.Exit(1)
	}

	port := os.Args[1]
	b := newBroker()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Path[1:]
		if name == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		switch r.Method {
		case http.MethodPut:
			values, ok := r.URL.Query()["v"]
			if !ok || len(values) == 0 || values[0] == "" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			b.put(name, values[0])
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			timeout := time.Duration(0)
			wait := false

			if raw := r.URL.Query().Get("timeout"); raw != "" {
				n, err := strconv.Atoi(raw)
				if err != nil || n < 0 {
					w.WriteHeader(http.StatusBadRequest)
					return
				}

				timeout = time.Duration(n) * time.Second
				wait = true
			}

			msg, ok := b.get(name, timeout, wait, r.Context().Done())
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			_, _ = w.Write([]byte(msg))

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
