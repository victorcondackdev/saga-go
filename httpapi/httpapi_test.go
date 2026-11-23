package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/victorcondackdev/saga-go/httpapi"
	"github.com/victorcondackdev/saga-go/saga"
	"github.com/victorcondackdev/saga-go/sagatest"
	"github.com/victorcondackdev/saga-go/store"
)

type pay struct {
	Amount int    `json:"amount"`
	Paid   string `json:"paid,omitempty"`
}

func setup(t *testing.T) (*httptest.Server, *saga.Engine, store.Store) {
	t.Helper()
	st := store.NewMemory()
	clock := sagatest.NewClock(time.Now())
	e := saga.New(st, saga.WithLogger(saga.DiscardLogger()), saga.WithClock(clock))
	broken := true
	def := saga.Define[pay]("pay").
		Step("reserve", func(sc saga.Context, p *pay) error { return nil }, saga.Compensate(func(sc saga.Context, p *pay) error {
			if broken {
				broken = false
				return saga.Permanent(errors.New("ledger down"))
			}
			return nil
		})).
		Step("settle", func(sc saga.Context, p *pay) error {
			if p.Amount > 100 {
				return saga.Permanent(errors.New("limit exceeded"))
			}
			var s string
			if err := sc.Await("settled", &s); err != nil {
				return err
			}
			p.Paid = s
			return nil
		}).MustBuild()
	if err := e.Register(def); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(httpapi.New(e))
	t.Cleanup(srv.Close)
	return srv, e, st
}

func call(t *testing.T, method, url, body string, hdr ...string) (int, http.Header, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, string(b)
}

func TestAPI(t *testing.T) {
	srv, e, _ := setup(t)
	ctx := context.Background()
	u := srv.URL

	code, _, body := call(t, "POST", u+"/v1/sagas/pay", `{"amount":10}`)
	if code != 400 || !strings.Contains(body, "Idempotency-Key") {
		t.Fatalf("missing key: %d %s", code, body)
	}
	code, hdr, body := call(t, "POST", u+"/v1/sagas/pay", `{"amount":10}`, "Idempotency-Key", "k1")
	if code != 201 || hdr.Get("Location") == "" {
		t.Fatalf("start: %d %s", code, body)
	}
	var in store.Instance
	json.Unmarshal([]byte(body), &in)
	code, hdr, _ = call(t, "POST", u+"/v1/sagas/pay", `{"amount":10}`, "Idempotency-Key", "k1")
	if code != 200 || hdr.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay: %d", code)
	}
	if code, _, _ = call(t, "POST", u+"/v1/sagas/pay", `{"amount":11}`, "Idempotency-Key", "k1"); code != 409 {
		t.Fatalf("key reuse: %d", code)
	}
	if code, _, _ = call(t, "POST", u+"/v1/sagas/nope", `{}`, "Idempotency-Key", "k"); code != 404 {
		t.Fatalf("unknown type: %d", code)
	}
	if code, _, _ = call(t, "POST", u+"/v1/sagas/pay", `{"amount":`, "Idempotency-Key", "k2"); code != 400 {
		t.Fatalf("bad json: %d", code)
	}
	if code, _, _ = call(t, "POST", u+"/v1/sagas/pay", `{"amount":"x"}`, "Idempotency-Key", "k3"); code != 400 {
		t.Fatalf("wrong type: %d", code)
	}
	if code, _, _ = call(t, "POST", u+"/v1/sagas/pay", strings.Repeat(" ", httpapi.MaxBody+1), "Idempotency-Key", "k4"); code != 400 {
		t.Fatalf("huge body: %d", code)
	}

	e.Tick(ctx) // runs reserve, parks settle on the signal
	code, _, body = call(t, "GET", u+"/v1/sagas/"+in.ID, "")
	if code != 200 || !strings.Contains(body, `"WAITING"`) {
		t.Fatalf("get: %d %s", code, body)
	}
	if code, _, _ = call(t, "GET", u+"/v1/sagas/missing", ""); code != 404 {
		t.Fatalf("get missing: %d", code)
	}
	if code, _, _ = call(t, "POST", u+"/v1/sagas/"+in.ID+"/signals/settled", `{bad`); code != 400 {
		t.Fatalf("bad signal: %d", code)
	}
	if code, _, _ = call(t, "POST", u+"/v1/sagas/missing/signals/settled", ``); code != 404 {
		t.Fatalf("signal missing: %d", code)
	}
	if code, _, _ = call(t, "POST", u+"/v1/sagas/"+in.ID+"/signals/settled", `"yes"`); code != 202 {
		t.Fatalf("signal: %d", code)
	}
	e.Tick(ctx)
	code, _, body = call(t, "GET", u+"/v1/sagas/"+in.ID+"/timeline", "")
	if code != 200 || !strings.Contains(body, `"rows"`) || !strings.Contains(body, `"COMPLETED"`) {
		t.Fatalf("timeline: %d %s", code, body)
	}
	code, hdr, body = call(t, "GET", u+"/v1/sagas/"+in.ID+"/timeline?format=text&width=20", "")
	if code != 200 || !strings.HasPrefix(hdr.Get("Content-Type"), "text/plain") || !strings.Contains(body, "settle") {
		t.Fatalf("text timeline: %d %s", code, body)
	}
	if code, _, _ = call(t, "GET", u+"/v1/sagas/missing/timeline", ""); code != 404 {
		t.Fatalf("timeline missing: %d", code)
	}

	// A saga whose compensation fails: stuck, then retried by an operator.
	_, _, body = call(t, "POST", u+"/v1/sagas/pay", `{"amount":500}`, "Idempotency-Key", "big")
	var big store.Instance
	json.Unmarshal([]byte(body), &big)
	e.Tick(ctx)
	code, _, body = call(t, "GET", u+"/v1/stuck?older_than=1h", "")
	if code != 200 || !strings.Contains(body, big.ID) || !strings.Contains(body, "FAILED_NEEDS_ATTENTION") {
		t.Fatalf("stuck: %d %s", code, body)
	}
	if code, _, _ = call(t, "GET", u+"/v1/stuck?older_than=forever", ""); code != 400 {
		t.Fatalf("bad duration: %d", code)
	}
	if code, _, _ = call(t, "POST", u+"/v1/sagas/"+big.ID+"/resolve", `{"outcome":"RUNNING"}`); code != 409 {
		t.Fatalf("bad resolve: %d", code)
	}
	if code, _, _ = call(t, "POST", u+"/v1/sagas/"+big.ID+"/retry", `{bad`); code != 400 {
		t.Fatalf("bad retry body: %d", code)
	}
	code, _, body = call(t, "POST", u+"/v1/sagas/"+big.ID+"/retry", `{"note":"ledger is back"}`)
	if code != 200 || !strings.Contains(body, "COMPENSATING") {
		t.Fatalf("retry: %d %s", code, body)
	}
	e.Tick(ctx)
	if code, _, _ = call(t, "POST", u+"/v1/sagas/"+big.ID+"/retry", ``); code != 409 {
		t.Fatalf("retry compensated: %d", code)
	}
	if code, _, _ = call(t, "POST", u+"/v1/sagas/missing/resolve", `{"outcome":"COMPLETED"}`); code != 404 {
		t.Fatalf("resolve missing: %d", code)
	}

	code, _, body = call(t, "GET", u+"/v1/sagas?status=COMPLETED&type=pay&limit=5", "")
	if code != 200 || !strings.Contains(body, in.ID) || strings.Contains(body, big.ID) {
		t.Fatalf("list: %d %s", code, body)
	}
	if code, _, _ = call(t, "GET", u+"/v1/sagas?limit=-1", ""); code != 400 {
		t.Fatalf("bad limit: %d", code)
	}
	code, _, body = call(t, "GET", u+"/v1/sagas", "")
	var rows []httpapi.Summary
	json.Unmarshal([]byte(body), &rows)
	steps := map[string]string{}
	for _, r := range rows {
		steps[r.ID] = r.Step
	}
	if code != 200 || len(rows) != 2 || steps[in.ID] != "" || steps[big.ID] != "settle" {
		t.Fatalf("list all: %d %+v", code, rows)
	}
	if _, _, body = call(t, "GET", u+"/v1/types", ""); !strings.Contains(body, `"pay"`) {
		t.Fatal(body)
	}
	if _, _, body = call(t, "GET", u+"/metrics", ""); !strings.Contains(body, `saga_instances{status="COMPLETED"} 1`) {
		t.Fatal(body)
	}
	if code, _, body = call(t, "GET", u+"/healthz", ""); code != 200 || body != "ok\n" {
		t.Fatal(code, body)
	}
}

func TestResolveAndStoreErrors(t *testing.T) {
	srv, e, st := setup(t)
	ctx := context.Background()
	_, _, body := call(t, "POST", srv.URL+"/v1/sagas/pay", `{"amount":500}`, "Idempotency-Key", "big")
	var in store.Instance
	json.Unmarshal([]byte(body), &in)
	e.Tick(ctx)
	code, _, body := call(t, "POST", srv.URL+"/v1/sagas/"+in.ID+"/resolve", `{"outcome":"COMPENSATED","note":"refunded by hand"}`)
	if code != 200 || !strings.Contains(body, "refunded by hand") {
		t.Fatalf("resolve: %d %s", code, body)
	}
	st.Close()
	for _, p := range []string{"/v1/sagas", "/v1/stuck", "/metrics"} {
		if code, _, _ := call(t, "GET", srv.URL+p, ""); code != 500 {
			t.Errorf("%s with closed store: %d", p, code)
		}
	}
}
