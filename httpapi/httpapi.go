// Package httpapi exposes an Engine over HTTP/JSON:
//
//	POST /v1/sagas/{type}                 start (Idempotency-Key header required)
//	GET  /v1/sagas?status=&type=&limit=   list
//	GET  /v1/sagas/{id}                   instance, steps and history
//	GET  /v1/sagas/{id}/timeline          attempts on a time axis (?format=text for ASCII)
//	POST /v1/sagas/{id}/signals/{name}    deliver a signal (body is the payload)
//	POST /v1/sagas/{id}/retry             {"note": "..."}
//	POST /v1/sagas/{id}/resolve           {"outcome": "COMPLETED|COMPENSATED", "note": "..."}
//	GET  /v1/stuck?older_than=5m          needs attention, or in flight and not moving
//	GET  /v1/types                        registered saga types
//	GET  /metrics                         Prometheus text format
//	GET  /healthz
//
// The API has no authentication of its own; run it behind your gateway.
package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/victorcondackdev/saga-go/saga"
	"github.com/victorcondackdev/saga-go/store"
)

// MaxBody bounds request bodies.
const MaxBody = 1 << 20

// Summary is the list representation of an instance.
type Summary struct {
	ID        string       `json:"id"`
	Type      string       `json:"type"`
	Key       string       `json:"key"`
	Status    store.Status `json:"status"`
	Failure   string       `json:"failure,omitempty"`
	Step      string       `json:"step,omitempty"` // current forward step
	CreatedAt time.Time    `json:"created_at"`
	UpdatedAt time.Time    `json:"updated_at"`
}

func summarize(in *store.Instance) Summary {
	s := Summary{ID: in.ID, Type: in.Type, Key: in.Key, Status: in.Status, Failure: in.Failure, CreatedAt: in.CreatedAt, UpdatedAt: in.UpdatedAt}
	for _, r := range in.Steps {
		if r.Status != store.StepDone && r.Status != store.StepSkipped {
			s.Step = r.Name
			break
		}
	}
	return s
}

type api struct{ e *saga.Engine }

// New returns a handler serving the API for e.
func New(e *saga.Engine) http.Handler {
	a := &api{e: e}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sagas/{type}", a.start)
	mux.HandleFunc("GET /v1/sagas", a.list)
	mux.HandleFunc("GET /v1/sagas/{id}", a.get)
	mux.HandleFunc("GET /v1/sagas/{id}/timeline", a.timeline)
	mux.HandleFunc("POST /v1/sagas/{id}/signals/{name}", a.signal)
	mux.HandleFunc("POST /v1/sagas/{id}/retry", a.retry)
	mux.HandleFunc("POST /v1/sagas/{id}/resolve", a.resolve)
	mux.HandleFunc("GET /v1/stuck", a.stuck)
	mux.HandleFunc("GET /v1/types", a.types)
	mux.HandleFunc("GET /metrics", a.metrics)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok\n") })
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, store.ErrNotFound), errors.Is(err, saga.ErrUnknownType):
		code = http.StatusNotFound
	case errors.Is(err, saga.ErrKeyConflict), errors.Is(err, saga.ErrInvalidTransition), errors.Is(err, store.ErrConflict):
		code = http.StatusConflict
	case errors.Is(err, errBadRequest), errors.Is(err, saga.ErrInvalidInput):
		code = http.StatusBadRequest
	}
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

var errBadRequest = errors.New("bad request")

type badRequest struct{ msg string }

func (b badRequest) Error() string   { return b.msg }
func (b badRequest) Is(t error) bool { return t == errBadRequest }

func body(r *http.Request) ([]byte, error) {
	b, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, MaxBody))
	if err != nil {
		return nil, badRequest{"request body: " + err.Error()}
	}
	return b, nil
}

func (a *api) start(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeErr(w, badRequest{"Idempotency-Key header is required"})
		return
	}
	b, err := body(r)
	if err != nil {
		writeErr(w, err)
		return
	}
	if len(b) > 0 && !json.Valid(b) {
		writeErr(w, badRequest{"body is not valid JSON"})
		return
	}
	in, created, err := a.e.Start(r.Context(), r.PathValue("type"), key, b)
	if err != nil {
		writeErr(w, err)
		return
	}
	code := http.StatusCreated
	if !created {
		code = http.StatusOK
		w.Header().Set("Idempotent-Replayed", "true")
	}
	w.Header().Set("Location", "/v1/sagas/"+in.ID)
	writeJSON(w, code, in)
}

func (a *api) get(w http.ResponseWriter, r *http.Request) {
	in, err := a.e.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, in)
}

func (a *api) timeline(w http.ResponseWriter, r *http.Request) {
	in, err := a.e.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	if r.URL.Query().Get("format") == "text" {
		width, _ := strconv.Atoi(r.URL.Query().Get("width"))
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_ = saga.RenderTimeline(w, in, width)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": in.ID, "status": in.Status, "rows": saga.Timeline(in)})
}

func (a *api) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.Filter{Type: q.Get("type")}
	for _, s := range q["status"] {
		f.Statuses = append(f.Statuses, store.Status(s))
	}
	if l := q.Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 0 {
			writeErr(w, badRequest{"limit must be a non-negative integer"})
			return
		}
		f.Limit = n
	}
	ins, err := a.e.List(r.Context(), f)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, summaries(ins))
}

func summaries(ins []*store.Instance) []Summary {
	out := make([]Summary, len(ins))
	for i, in := range ins {
		out[i] = summarize(in)
	}
	return out
}

func (a *api) stuck(w http.ResponseWriter, r *http.Request) {
	older := 5 * time.Minute
	if s := r.URL.Query().Get("older_than"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			writeErr(w, badRequest{"older_than: " + err.Error()})
			return
		}
		older = d
	}
	ins, err := a.e.Stuck(r.Context(), older)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, summaries(ins))
}

func (a *api) signal(w http.ResponseWriter, r *http.Request) {
	b, err := body(r)
	if err != nil {
		writeErr(w, err)
		return
	}
	if len(b) == 0 {
		b = []byte("null")
	}
	if !json.Valid(b) {
		writeErr(w, badRequest{"signal payload is not valid JSON"})
		return
	}
	if err := a.e.Signal(r.Context(), r.PathValue("id"), r.PathValue("name"), json.RawMessage(b)); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

type manual struct {
	Outcome store.Status `json:"outcome"`
	Note    string       `json:"note"`
}

func decodeManual(r *http.Request) (manual, error) {
	var m manual
	b, err := body(r)
	if err != nil || len(b) == 0 {
		return m, err
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return m, badRequest{"body: " + err.Error()}
	}
	return m, nil
}

func (a *api) retry(w http.ResponseWriter, r *http.Request) {
	m, err := decodeManual(r)
	if err == nil {
		err = a.e.Retry(r.Context(), r.PathValue("id"), m.Note)
	}
	if err != nil {
		writeErr(w, err)
		return
	}
	a.get(w, r)
}

func (a *api) resolve(w http.ResponseWriter, r *http.Request) {
	m, err := decodeManual(r)
	if err == nil {
		err = a.e.Resolve(r.Context(), r.PathValue("id"), m.Outcome, m.Note)
	}
	if err != nil {
		writeErr(w, err)
		return
	}
	a.get(w, r)
}

func (a *api) types(w http.ResponseWriter, r *http.Request) {
	t := a.e.Types()
	sort.Strings(t)
	writeJSON(w, http.StatusOK, t)
}

func (a *api) metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	if err := a.e.WritePrometheus(r.Context(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
