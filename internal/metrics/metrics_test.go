package metrics

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestExposition(t *testing.T) {
	r := &Registry{}
	c := r.Counter("jobs_total", "Jobs.", "kind")
	g := r.Gauge("queue", "Queue depth.")
	h := r.Histogram("lat_seconds", "Latency.", []float64{1, 0.1}, "op")
	c.Inc("a\"b\\c\nd")
	c.Add(2, "x")
	g.Set(3)
	h.Observe(0.05, "get")
	h.Observe(0.5, "get")
	h.Observe(5, "get")
	var b bytes.Buffer
	if err := r.Write(&b); err != nil {
		t.Fatal(err)
	}
	want := `# HELP jobs_total Jobs.
# TYPE jobs_total counter
jobs_total{kind="a\"b\\c\nd"} 1
jobs_total{kind="x"} 2
# HELP queue Queue depth.
# TYPE queue gauge
queue 3
# HELP lat_seconds Latency.
# TYPE lat_seconds histogram
lat_seconds_bucket{op="get",le="0.1"} 1
lat_seconds_bucket{op="get",le="1"} 2
lat_seconds_bucket{op="get",le="+Inf"} 3
lat_seconds_sum{op="get"} 5.55
lat_seconds_count{op="get"} 3
`
	if b.String() != want {
		t.Fatalf("got:\n%s\nwant:\n%s", b.String(), want)
	}
	if c.Value("x") != 2 || h.Count("get") != 3 {
		t.Fatal("accessors")
	}
	g.Reset()
	b.Reset()
	r.Write(&b)
	if strings.Contains(b.String(), "queue 3") {
		t.Fatal("reset gauge still reported")
	}
	nolabel := r.Histogram("h", "H.", []float64{1})
	nolabel.Observe(2)
	b.Reset()
	r.Write(&b)
	if !strings.Contains(b.String(), `h_bucket{le="+Inf"} 1`) || num(posInf()) != "+Inf" {
		t.Fatal(b.String())
	}
	defer func() {
		if recover() == nil {
			t.Fatal("label arity not checked")
		}
	}()
	c.Inc()
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestWriteError(t *testing.T) {
	r := &Registry{}
	r.Counter("c", "C.").Inc()
	if err := r.Write(failWriter{}); err == nil {
		t.Fatal("write error swallowed")
	}
}

func posInf() float64 { var z float64; return 1 / z }
