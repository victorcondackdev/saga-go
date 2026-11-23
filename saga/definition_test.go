package saga_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/victorcondackdev/saga-go/saga"
	"github.com/victorcondackdev/saga-go/store"
)

type other struct{ X int }

func TestBuildValidation(t *testing.T) {
	ok := step("x")
	tests := []struct {
		name string
		b    *saga.Builder[order]
		want string
	}{
		{"no name", saga.Define[order]("").Step("a", ok), "type name is empty"},
		{"no steps", saga.Define[order]("s"), "no steps"},
		{"nil func", saga.Define[order]("s").Step("a", nil), "nil step function"},
		{"empty step name", saga.Define[order]("s").Step("", ok), "empty step name"},
		{"bad step name", saga.Define[order]("s").Step("a:b", ok), "cannot contain"},
		{"duplicate", saga.Define[order]("s").Step("a", ok).Step("a", ok), "duplicate step name"},
		{"two pivots", saga.Define[order]("s").Step("a", ok, saga.Pivot()).Step("b", ok, saga.Pivot()), "only one pivot"},
		{"pivot in group", saga.Define[order]("s").Parallel(saga.Par("a", ok, saga.Pivot()), saga.Par("b", ok)), "parallel group"},
		{"compensable pivot", saga.Define[order]("s").Step("a", ok, saga.Pivot(), saga.Compensate(ok)), "pivot cannot have a compensation"},
		{"compensation after pivot", saga.Define[order]("s").Step("a", ok, saga.Pivot()).Step("b", ok, saga.Compensate(ok)), "forward recovery only"},
		{"empty group", saga.Define[order]("s").Parallel(), "without branches"},
		{"wrong compensation type", saga.Define[order]("s").Step("a", ok, saga.Compensate(func(sc saga.Context, o *other) error { return nil })), "compensation is not a StepFunc"},
		{"wrong predicate type", saga.Define[order]("s").Step("a", ok, saga.When(func(o *other) bool { return true })), "When predicate"},
		{"attempted without compensation", saga.Define[order]("s").Step("a", ok, saga.CompensateAttempted()), "without Compensate"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.b.Build()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
	defer func() {
		if recover() == nil {
			t.Fatal("MustBuild did not panic")
		}
	}()
	saga.Define[order]("").MustBuild()
}

func TestStepsIntrospection(t *testing.T) {
	d := saga.Define[order]("intro").
		Step("a", step("a"), saga.Compensate(undo("a")), saga.CompensateAttempted(), saga.Timeout(time.Second), nil).
		Parallel(saga.Par("b", step("b")), saga.Par("c", step("c"), saga.When(func(*order) bool { return true }))).
		Step("p", step("p"), saga.Pivot(), saga.Retry(saga.RetryPolicy{MaxAttempts: 9})).
		MustBuild()
	got := d.Steps()
	if d.Name() != "intro" || len(got) != 4 {
		t.Fatal(got)
	}
	want := []saga.StepInfo{
		{Name: "a", Group: 0, Compensable: true, MaxAttempts: 3, Timeout: time.Second, CompensateAttempted: true},
		{Name: "b", Group: 1, MaxAttempts: 3},
		{Name: "c", Group: 1, Conditional: true, MaxAttempts: 3},
		{Name: "p", Group: 2, Pivot: true, MaxAttempts: 9},
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("step %d: got %+v want %+v", i, got[i], want[i])
		}
	}
}

func TestBackoff(t *testing.T) {
	p := saga.RetryPolicy{Initial: 100 * time.Millisecond, Max: time.Second, Multiplier: 2}
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{5, time.Second},
		{500, time.Second},
	}
	for _, tt := range tests {
		if got := p.Backoff(tt.attempt, nil); got != tt.want {
			t.Errorf("attempt %d: %s, want %s", tt.attempt, got, tt.want)
		}
	}
	j := saga.RetryPolicy{Initial: time.Second, Max: time.Second, Multiplier: 2, Jitter: 0.5}
	if got := j.Backoff(1, func() float64 { return 0.999 }); got < 500*time.Millisecond || got > 501*time.Millisecond {
		t.Errorf("full jitter: %s", got)
	}
	if got := j.Backoff(1, func() float64 { return 0 }); got != time.Second {
		t.Errorf("zero jitter: %s", got)
	}
	neg := saga.RetryPolicy{Initial: time.Second, Max: time.Second, Jitter: 2}
	if got := neg.Backoff(1, func() float64 { return 0.99 }); got != 0 {
		t.Errorf("over-jitter clamps at zero: %s", got)
	}
}

func TestPermanentAndWaiting(t *testing.T) {
	base := errors.New("x")
	p := saga.Permanent(base)
	if !saga.IsPermanent(p) || !errors.Is(p, base) || saga.IsPermanent(base) || saga.Permanent(nil) != nil {
		t.Fatal("permanent")
	}
	if !strings.Contains(p.Error(), "permanent: x") {
		t.Fatal(p.Error())
	}
	if saga.IsWaiting(base) {
		t.Fatal("waiting")
	}
	if saga.IdempotencyKey("s", "a", false) != "s:a" || saga.IdempotencyKey("s", "a", true) != "s:a:compensate" {
		t.Fatal("keys")
	}
}

func TestTimelineRender(t *testing.T) {
	at := func(ms int) time.Time { return testStart.Add(time.Duration(ms) * time.Millisecond) }
	in := &store.Instance{ID: "sg_1", Type: "demo", Status: store.StatusCompensated, Failure: "step \"send\" failed",
		History: []store.Event{
			{Kind: store.EvCreated, At: at(0)},
			{Kind: store.EvStarted, At: at(0)},
			{Kind: store.EvStepStarted, Step: "reserve", Attempt: 1, At: at(0)},
			{Kind: store.EvStepSucceeded, Step: "reserve", Attempt: 1, At: at(100)},
			{Kind: store.EvStepSkipped, Step: "review", At: at(100), Detail: "condition false"},
			{Kind: store.EvStepStarted, Step: "send", Attempt: 1, At: at(100)},
			{Kind: store.EvStepRetry, Step: "send", Attempt: 1, At: at(300), Error: "503"},
			{Kind: store.EvStepStarted, Step: "send", Attempt: 2, At: at(400)},
			{Kind: store.EvLeaseTakeover, At: at(500), Owner: "w1"},
			{Kind: store.EvStepAbandoned, Step: "send", Attempt: 2, At: at(500)},
			{Kind: store.EvStepStarted, Step: "send", Attempt: 3, At: at(500)},
			{Kind: store.EvStepWaiting, Step: "send", Attempt: 3, At: at(500), Detail: "awaiting signal x"},
			{Kind: store.EvStepStarted, Step: "send", Attempt: 3, At: at(600)},
			{Kind: store.EvStepFailed, Step: "send", Attempt: 3, At: at(700), Error: "declined"},
			{Kind: store.EvCompensating, At: at(700), Error: "declined"},
			{Kind: store.EvCompStarted, Step: "reserve", Attempt: 1, At: at(700)},
			{Kind: store.EvCompRetry, Step: "reserve", Attempt: 1, At: at(750), Error: "busy"},
			{Kind: store.EvCompStarted, Step: "reserve", Attempt: 2, At: at(800)},
			{Kind: store.EvCompAbandoned, Step: "reserve", Attempt: 2, At: at(850)},
			{Kind: store.EvCompStarted, Step: "reserve", Attempt: 3, At: at(900)},
			{Kind: store.EvCompSucceeded, Step: "reserve", Attempt: 3, At: at(950)},
			{Kind: store.EvCompFailed, Step: "other", Attempt: 1, At: at(960)},
			{Kind: store.EvCompensated, At: at(1000)},
		}}
	var b bytes.Buffer
	if err := saga.RenderTimeline(&b, in, 0); err != nil {
		t.Fatal(err)
	}
	out := strings.Join(strings.Fields(b.String()), " ")
	for _, want := range []string{
		"saga sg_1 type=demo status=COMPENSATED span=1s",
		"failure: step \"send\" failed",
		"|0s------------------------------------1s|",
		"reserve #1 ok |===>",
		"| 100ms",
		"review skipped",
		"send #1 retry",
		"200ms 503",
		"send #2 abandoned",
		"from w1",
		"send #3 failed",
		"<- reserve #3 ok",
		"<- reserve #2 abandoned",
		"<- other #1 failed",
		"* saga_compensated",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("timeline missing %q:\n%s", want, out)
		}
	}
	if strings.Count(out, "send #3") != 1 || !strings.Contains(out, "~~~~x") {
		t.Errorf("parked attempt not collapsed into one row:\n%s", out)
	}
	rows := saga.Timeline(in)
	if rows[2].Outcome != "ok" || rows[2].End.Sub(rows[2].Start) != 100*time.Millisecond {
		t.Fatalf("row: %+v", rows[2])
	}
	// Narrow axes and empty spans render without panicking.
	b.Reset()
	saga.RenderTimeline(&b, &store.Instance{History: []store.Event{{Kind: store.EvStepStarted, Step: "x", Attempt: 1, At: at(0)}}}, 2)
	if !strings.Contains(b.String(), "running") {
		t.Fatal(b.String())
	}
	b.Reset()
	saga.RenderTimeline(&b, &store.Instance{History: []store.Event{{Kind: store.EvStepSucceeded, Step: "x", Attempt: 1, At: at(0)}, {Kind: store.EvCompleted, At: at(1)}}}, 40)
	if !strings.Contains(b.String(), "1ms") {
		t.Fatal(b.String())
	}
	b.Reset()
	saga.RenderTimeline(&b, &store.Instance{History: []store.Event{{Kind: store.EvCreated, At: at(0)}, {Kind: store.EvCompleted, At: at(0).Add(500 * time.Microsecond)}}}, 40)
	if !strings.Contains(b.String(), "500µs") {
		t.Fatal(b.String())
	}
}
