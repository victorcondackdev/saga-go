// Package metrics is a minimal, dependency-free Prometheus text-format
// registry: labelled counters, gauges and histograms.
package metrics

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Registry holds metric families in registration order.
type Registry struct {
	mu       sync.Mutex
	families []*family
}

type family struct {
	name, help, typ string
	labels          []string
	buckets         []float64
	series          map[string]*series
}

type series struct {
	values  []string
	value   float64
	counts  []uint64 // histogram: per bucket, non-cumulative
	sum     float64
	samples uint64
}

func (r *Registry) add(name, help, typ string, buckets []float64, labels []string) *family {
	f := &family{name: name, help: help, typ: typ, labels: labels, buckets: buckets, series: map[string]*series{}}
	r.mu.Lock()
	r.families = append(r.families, f)
	r.mu.Unlock()
	return f
}

func (r *Registry) get(f *family, lvs []string) *series {
	if len(lvs) != len(f.labels) {
		panic(fmt.Sprintf("metrics: %s wants %d label values, got %d", f.name, len(f.labels), len(lvs)))
	}
	k := strings.Join(lvs, "\xff")
	s, ok := f.series[k]
	if !ok {
		s = &series{values: append([]string(nil), lvs...)}
		if f.typ == "histogram" {
			s.counts = make([]uint64, len(f.buckets))
		}
		f.series[k] = s
	}
	return s
}

// Counter is a monotonically increasing labelled value.
type Counter struct {
	r *Registry
	f *family
}

// Counter registers a counter family.
func (r *Registry) Counter(name, help string, labels ...string) *Counter {
	return &Counter{r: r, f: r.add(name, help, "counter", nil, labels)}
}

// Add adds v (which must be >= 0) to the series with the given label values.
func (c *Counter) Add(v float64, lvs ...string) {
	c.r.mu.Lock()
	c.r.get(c.f, lvs).value += v
	c.r.mu.Unlock()
}

// Inc adds one.
func (c *Counter) Inc(lvs ...string) { c.Add(1, lvs...) }

// Value returns the current value of a series (0 if absent).
func (c *Counter) Value(lvs ...string) float64 {
	c.r.mu.Lock()
	defer c.r.mu.Unlock()
	return c.r.get(c.f, lvs).value
}

// Gauge is a labelled value that can go up and down.
type Gauge struct {
	r *Registry
	f *family
}

// Gauge registers a gauge family.
func (r *Registry) Gauge(name, help string, labels ...string) *Gauge {
	return &Gauge{r: r, f: r.add(name, help, "gauge", nil, labels)}
}

// Set sets a series.
func (g *Gauge) Set(v float64, lvs ...string) {
	g.r.mu.Lock()
	g.r.get(g.f, lvs).value = v
	g.r.mu.Unlock()
}

// Reset removes every series, so values that disappeared are not reported.
func (g *Gauge) Reset() {
	g.r.mu.Lock()
	g.f.series = map[string]*series{}
	g.r.mu.Unlock()
}

// Histogram counts observations into cumulative buckets.
type Histogram struct {
	r *Registry
	f *family
}

// DefBuckets are latency buckets in seconds from 1ms to 30s.
var DefBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}

// Histogram registers a histogram family with sorted upper bounds.
func (r *Registry) Histogram(name, help string, buckets []float64, labels ...string) *Histogram {
	b := append([]float64(nil), buckets...)
	sort.Float64s(b)
	return &Histogram{r: r, f: r.add(name, help, "histogram", b, labels)}
}

// Observe records v.
func (h *Histogram) Observe(v float64, lvs ...string) {
	h.r.mu.Lock()
	defer h.r.mu.Unlock()
	s := h.r.get(h.f, lvs)
	s.sum += v
	s.samples++
	for i, ub := range h.f.buckets {
		if v <= ub {
			s.counts[i]++
			return
		}
	}
}

// Count returns the number of observations of a series.
func (h *Histogram) Count(lvs ...string) uint64 {
	h.r.mu.Lock()
	defer h.r.mu.Unlock()
	return h.r.get(h.f, lvs).samples
}

// Write renders every family in the Prometheus text exposition format.
func (r *Registry) Write(w io.Writer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	bw := bufio.NewWriter(w)
	for _, f := range r.families {
		fmt.Fprintf(bw, "# HELP %s %s\n# TYPE %s %s\n", f.name, f.help, f.name, f.typ)
		keys := make([]string, 0, len(f.series))
		for k := range f.series {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			s := f.series[k]
			if f.typ != "histogram" {
				fmt.Fprintf(bw, "%s%s %s\n", f.name, labels(f.labels, s.values, "", ""), num(s.value))
				continue
			}
			var cum uint64
			for i, ub := range f.buckets {
				cum += s.counts[i]
				fmt.Fprintf(bw, "%s_bucket%s %d\n", f.name, labels(f.labels, s.values, "le", num(ub)), cum)
			}
			fmt.Fprintf(bw, "%s_bucket%s %d\n", f.name, labels(f.labels, s.values, "le", "+Inf"), s.samples)
			fmt.Fprintf(bw, "%s_sum%s %s\n", f.name, labels(f.labels, s.values, "", ""), num(s.sum))
			fmt.Fprintf(bw, "%s_count%s %d\n", f.name, labels(f.labels, s.values, "", ""), s.samples)
		}
	}
	return bw.Flush()
}

func labels(names, values []string, extraName, extraValue string) string {
	if len(names) == 0 && extraName == "" {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(n + `="` + escape(values[i]) + `"`)
	}
	if extraName != "" {
		if len(names) > 0 {
			b.WriteByte(',')
		}
		b.WriteString(extraName + `="` + extraValue + `"`)
	}
	b.WriteByte('}')
	return b.String()
}

func escape(s string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`).Replace(s)
}

func num(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}
