package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	boleto "github.com/victorcondackdev/saga-go/examples/boleto-payment"
	"github.com/victorcondackdev/saga-go/store"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func TestServiceEndToEnd(t *testing.T) {
	for _, kind := range []string{"memory", "wal"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			addrc := make(chan string, 1)
			done := make(chan error, 1)
			var logs syncBuffer
			args := []string{"-addr", "127.0.0.1:0", "-store", kind, "-dir", t.TempDir(), "-fail-rate", "0", "-settle-after", "10ms", "-log-level", "warn"}
			go func() { done <- run(ctx, args, &logs, func(a string) { addrc <- a }) }()
			var base string
			select {
			case a := <-addrc:
				base = "http://" + a
			case err := <-done:
				t.Fatal(err)
			}
			start := func(typ, key, input string) string {
				req, _ := http.NewRequest("POST", base+"/v1/sagas/"+typ, strings.NewReader(input))
				req.Header.Set("Idempotency-Key", key)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				var in store.Instance
				json.NewDecoder(resp.Body).Decode(&in)
				if resp.StatusCode != 201 {
					t.Fatalf("start %s: %d", typ, resp.StatusCode)
				}
				return in.ID
			}
			ids := []string{
				start("pix-payout", "p1", `{"payout_id":"1","account":"acc-1","pix_key":"a@b.c","amount":1000}`),
				start("boleto-payment", "b1", `{"account":"acc-1","line":"`+boleto.MakeLine("23793381286000000000300000000400000000012345")+`","amount":1000}`),
			}
			for _, id := range ids {
				deadline := time.Now().Add(10 * time.Second)
				for {
					resp, err := http.Get(base + "/v1/sagas/" + id)
					if err != nil {
						t.Fatal(err)
					}
					b, _ := io.ReadAll(resp.Body)
					resp.Body.Close()
					var in store.Instance
					json.Unmarshal(b, &in)
					if in.Status == store.StatusCompleted {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("%s not completed: %s\nlogs:\n%s", id, b, logs.String())
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFlags(t *testing.T) {
	var out bytes.Buffer
	for _, args := range [][]string{{"-store", "redis"}, {"-fail-rate", "2"}, {"-bogus"}} {
		if _, err := parseFlags(args, &out); err == nil {
			t.Errorf("%v accepted", args)
		}
	}
	ctx := context.Background()
	if err := run(ctx, []string{"-log-level", "loud"}, &out, nil); err == nil {
		t.Error("bad log level accepted")
	}
	if err := run(ctx, []string{"-store", "wal", "-dir", t.TempDir(), "-sync", "sometimes"}, &out, nil); err == nil {
		t.Error("bad sync policy accepted")
	}
	if err := run(ctx, []string{"-addr", "256.0.0.1:99999"}, &out, nil); err == nil {
		t.Error("bad address accepted")
	}
}
