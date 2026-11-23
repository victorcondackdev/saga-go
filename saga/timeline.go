package saga

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/victorcondackdev/saga-go/store"
)

// TimelineRow is one line of a rendered timeline: an attempt (of a step or
// a compensation) or a saga-level milestone.
type TimelineRow struct {
	Label        string
	Step         string
	Compensation bool
	Attempt      int
	Start, End   time.Time
	Outcome      string // ok, retry, failed, waiting, abandoned, running, or the milestone kind
	Error        string
	Milestone    bool
	Waited       bool // part of the attempt was spent parked on a signal
}

// Timeline reconstructs attempts and milestones from an instance's history.
// An attempt that was started but never reported back (its worker crashed)
// shows up with outcome "abandoned" once the next owner noticed, or
// "running" if nobody has yet.
func Timeline(in *store.Instance) []TimelineRow {
	var rows []TimelineRow
	open := map[string]int{}   // step|comp|attempt -> row index
	parked := map[string]int{} // rows parked on a signal, reopened by the next recheck
	key := func(step string, comp bool, attempt int) string { return fmt.Sprintf("%s|%t|%d", step, comp, attempt) }
	closeRow := func(step string, comp bool, attempt int, at time.Time, outcome, errText string) {
		k := key(step, comp, attempt)
		if i, ok := open[k]; ok {
			rows[i].End, rows[i].Outcome, rows[i].Error = at, outcome, errText
			delete(open, k)
			if outcome == "waiting" {
				rows[i].Waited = true
				parked[k] = i
			} else {
				delete(parked, k)
			}
			return
		}
		rows = append(rows, TimelineRow{Label: label(step, comp), Step: step, Compensation: comp, Attempt: attempt, Start: at, End: at, Outcome: outcome, Error: errText})
	}
	for _, ev := range in.History {
		switch ev.Kind {
		case store.EvStepStarted, store.EvCompStarted:
			comp := ev.Kind == store.EvCompStarted
			k := key(ev.Step, comp, ev.Attempt)
			if i, ok := parked[k]; ok { // re-check of a parked step: same attempt, same row
				open[k] = i
				continue
			}
			open[k] = len(rows)
			rows = append(rows, TimelineRow{Label: label(ev.Step, comp), Step: ev.Step, Compensation: comp, Attempt: ev.Attempt, Start: ev.At, End: ev.At, Outcome: "running"})
		case store.EvStepSucceeded:
			closeRow(ev.Step, false, ev.Attempt, ev.At, "ok", "")
		case store.EvStepRetry:
			closeRow(ev.Step, false, ev.Attempt, ev.At, "retry", ev.Error)
		case store.EvStepFailed:
			closeRow(ev.Step, false, ev.Attempt, ev.At, "failed", ev.Error)
		case store.EvStepWaiting:
			closeRow(ev.Step, false, ev.Attempt, ev.At, "waiting", ev.Detail)
		case store.EvStepAbandoned:
			closeRow(ev.Step, false, ev.Attempt, ev.At, "abandoned", "worker died mid-attempt")
		case store.EvCompSucceeded:
			closeRow(ev.Step, true, ev.Attempt, ev.At, "ok", "")
		case store.EvCompRetry:
			closeRow(ev.Step, true, ev.Attempt, ev.At, "retry", ev.Error)
		case store.EvCompFailed:
			closeRow(ev.Step, true, ev.Attempt, ev.At, "failed", ev.Error)
		case store.EvCompAbandoned:
			closeRow(ev.Step, true, ev.Attempt, ev.At, "abandoned", "worker died mid-attempt")
		case store.EvStepSkipped:
			rows = append(rows, TimelineRow{Label: label(ev.Step, false), Step: ev.Step, Start: ev.At, End: ev.At, Outcome: "skipped", Error: ev.Detail})
		default:
			text := ev.Error
			if text == "" {
				text = ev.Detail
			}
			if ev.Owner != "" {
				text = strings.TrimSpace("from " + ev.Owner + " " + text)
			}
			rows = append(rows, TimelineRow{Label: "* " + ev.Kind, Start: ev.At, End: ev.At, Outcome: ev.Kind, Error: text, Milestone: true})
		}
	}
	return rows
}

func label(step string, comp bool) string {
	if comp {
		return "<- " + step
	}
	return step
}

// RenderTimeline writes an ASCII timeline of the instance: one row per
// attempt, positioned on a shared time axis, with its outcome, duration and
// error, interleaved with saga milestones. width is the number of columns of
// the time axis (40 if <= 0).
func RenderTimeline(w io.Writer, in *store.Instance, width int) error {
	if width <= 0 {
		width = 40
	}
	rows := Timeline(in)
	bw := bufio.NewWriter(w)
	var t0, t1 time.Time
	for i, r := range rows {
		if i == 0 || r.Start.Before(t0) {
			t0 = r.Start
		}
		if r.End.After(t1) {
			t1 = r.End
		}
	}
	span := t1.Sub(t0)
	fmt.Fprintf(bw, "saga %s  type=%s  status=%s  span=%s\n", in.ID, in.Type, in.Status, fmtDur(span))
	if in.Failure != "" {
		fmt.Fprintf(bw, "failure: %s\n", in.Failure)
	}
	nameW := 10
	for _, r := range rows {
		if len(r.Label) > nameW {
			nameW = len(r.Label)
		}
	}
	col := func(t time.Time) int {
		if span <= 0 {
			return 0
		}
		c := int(float64(t.Sub(t0)) / float64(span) * float64(width-1))
		if c < 0 {
			c = 0
		}
		if c > width-1 {
			c = width - 1
		}
		return c
	}
	fmt.Fprintf(bw, "%-*s  %3s  %-9s |%s|\n", nameW, "", "", "", axis(width, span))
	for _, r := range rows {
		bar := []byte(strings.Repeat(" ", width))
		a, b := col(r.Start), col(r.End)
		if r.Milestone {
			bar[a] = '+'
		} else {
			fill := byte('=')
			if r.Waited {
				fill = '~'
			}
			for i := a; i <= b; i++ {
				bar[i] = fill
			}
			bar[b] = marker(r.Outcome)
		}
		att := ""
		if r.Attempt > 0 {
			att = fmt.Sprintf("#%d", r.Attempt)
		}
		outcome := r.Outcome
		if r.Milestone {
			outcome = ""
		}
		line := fmt.Sprintf("%-*s  %3s  %-9s |%s| ", nameW, r.Label, att, outcome, bar)
		if !r.Milestone {
			line += fmtDur(r.End.Sub(r.Start))
		}
		if r.Error != "" {
			line += "  " + r.Error
		}
		fmt.Fprintln(bw, strings.TrimRight(line, " "))
	}
	return bw.Flush()
}

func axis(width int, span time.Duration) string {
	left, right := "0s", fmtDur(span)
	if width < len(left)+len(right)+1 {
		return strings.Repeat("-", width)
	}
	return left + strings.Repeat("-", width-len(left)-len(right)) + right
}

func marker(outcome string) byte {
	switch outcome {
	case "ok":
		return '>'
	case "retry":
		return '!'
	case "failed":
		return 'x'
	case "waiting":
		return '~'
	case "abandoned":
		return '?'
	case "skipped":
		return '-'
	}
	return '.'
}

func fmtDur(d time.Duration) string {
	switch {
	case d <= 0:
		return "0s"
	case d < time.Millisecond:
		return d.Round(time.Microsecond).String()
	case d < time.Second:
		return d.Round(100 * time.Microsecond).String()
	default:
		return d.Round(time.Millisecond).String()
	}
}
