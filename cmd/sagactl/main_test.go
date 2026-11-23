package main

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/victorcondackdev/saga-go/httpapi"
	"github.com/victorcondackdev/saga-go/saga"
	"github.com/victorcondackdev/saga-go/store"
)

type st struct {
	N    int    `json:"n"`
	Sig  string `json:"sig,omitempty"`
	Fail bool   `json:"fail,omitempty"`
}

func TestCLI(t *testing.T) {
	e := saga.New(store.NewMemory(), saga.WithLogger(saga.DiscardLogger()))
	broken := true
	e.Register(saga.Define[st]("demo").
		Step("a", func(sc saga.Context, s *st) error { s.N++; return nil }, saga.Compensate(func(sc saga.Context, s *st) error {
			if broken {
				broken = false
				return saga.Permanent(errors.New("down"))
			}
			return nil
		})).
		Step("b", func(sc saga.Context, s *st) error {
			if s.Fail {
				return saga.Permanent(errors.New("nope"))
			}
			return sc.Await("go", &s.Sig)
		}).MustBuild())
	srv := httptest.NewServer(httpapi.New(e))
	defer srv.Close()
	ctx := context.Background()

	cli := func(args ...string) (int, string, string) {
		var out, errb bytes.Buffer
		code := run(append([]string{"-server", srv.URL}, args...), &out, &errb)
		return code, out.String(), errb.String()
	}
	code, out, _ := cli("start", "demo", "-key", "k1", "-input", `{"n":1}`)
	if code != 0 || !strings.Contains(out, "PENDING") {
		t.Fatalf("start: %d %s", code, out)
	}
	id := strings.Fields(out)[0]
	f := filepath.Join(t.TempDir(), "in.json")
	os.WriteFile(f, []byte(`{"fail":true}`), 0o600)
	_, out, _ = cli("start", "demo", "-key", "k2", "-input", "@"+f)
	failing := strings.Fields(out)[0]
	e.Tick(ctx)

	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"get", id}, "WAITING"},
		{[]string{"types"}, "demo"},
		{[]string{"list", "-status", "RUNNING", "-type", "demo", "-limit", "5"}, id},
		{[]string{"stuck", "-older-than", "1h"}, failing},
		{[]string{"signal", id, "go", `"now"`}, "signal accepted"},
	} {
		code, out, errOut := cli(tc.args...)
		if code != 0 || !strings.Contains(out, tc.want) {
			t.Fatalf("%v: %d %q %q", tc.args, code, out, errOut)
		}
	}
	e.Tick(ctx)
	if code, out, _ := cli("timeline", id, "-width", "20"); code != 0 || !strings.Contains(out, "status=COMPLETED") {
		t.Fatalf("timeline: %s", out)
	}
	if code, out, _ := cli("get", failing); code != 0 || !strings.Contains(out, "compensation FAILED") {
		t.Fatalf("get failing: %s", out)
	}
	if code, out, _ := cli("retry", failing, "-note", "fixed"); code != 0 || !strings.Contains(out, "COMPENSATING") {
		t.Fatalf("retry: %d %s", code, out)
	}
	e.Tick(ctx)
	if code, _, errOut := cli("resolve", failing, "completed"); code != 1 || !strings.Contains(errOut, "409") {
		t.Fatalf("resolve compensated: %d %s", code, errOut)
	}
	for _, args := range [][]string{
		{}, {"bogus"}, {"start", "demo"}, {"get"}, {"timeline"}, {"signal", id}, {"retry"}, {"resolve", id},
		{"list", "-nope"}, {"start", "demo", "-key", "x", "-input", "@/does/not/exist"},
	} {
		if code, _, _ := cli(args...); code == 0 {
			t.Errorf("%v succeeded", args)
		}
	}
	if code, _, errOut := cli("get", "missing"); code != 1 || !strings.Contains(errOut, "404") {
		t.Fatalf("missing: %d %s", code, errOut)
	}
	var errb bytes.Buffer
	if run([]string{"-server", "http://127.0.0.1:1", "types"}, &bytes.Buffer{}, &errb) != 1 {
		t.Fatal("unreachable server")
	}
	if run([]string{"-x"}, &bytes.Buffer{}, &errb) != 2 {
		t.Fatal("bad global flag")
	}
}
