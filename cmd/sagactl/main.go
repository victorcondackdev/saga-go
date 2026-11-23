// Command sagactl is the operator CLI for sagad.
//
//	sagactl [-server URL] start <type> -key K -input '{"...":...}'
//	sagactl get <id>
//	sagactl timeline <id> [-width 60]
//	sagactl list [-status S] [-type T] [-limit N]
//	sagactl stuck [-older-than 5m]
//	sagactl signal <id> <name> [json]
//	sagactl retry <id> [-note text]
//	sagactl resolve <id> COMPLETED|COMPENSATED [-note text]
//	sagactl types
//
// The server defaults to $SAGAD_URL or http://localhost:8080.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/victorcondackdev/saga-go/httpapi"
	"github.com/victorcondackdev/saga-go/saga"
	"github.com/victorcondackdev/saga-go/store"
)

type client struct {
	base string
	http *http.Client
	out  io.Writer
}

func (c *client) do(method, path string, body []byte, hdr map[string]string) ([]byte, error) {
	req, err := http.NewRequest(method, c.base+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		var e struct{ Error string }
		if json.Unmarshal(b, &e) == nil && e.Error != "" {
			return nil, fmt.Errorf("%s: %s", resp.Status, e.Error)
		}
		return nil, fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return b, nil
}

func usage(w io.Writer) {
	fmt.Fprintln(w, `usage: sagactl [-server URL] <command> [args]

commands:
  start <type> -key K [-input JSON | -input @file]
  get <id>
  timeline <id> [-width N]
  list [-status S] [-type T] [-limit N]
  stuck [-older-than D]
  signal <id> <name> [JSON]
  retry <id> [-note TEXT]
  resolve <id> COMPLETED|COMPENSATED [-note TEXT]
  types`)
}

func run(args []string, stdout, stderr io.Writer) int {
	global := flag.NewFlagSet("sagactl", flag.ContinueOnError)
	global.SetOutput(stderr)
	def := os.Getenv("SAGAD_URL")
	if def == "" {
		def = "http://localhost:8080"
	}
	server := global.String("server", def, "sagad base URL")
	global.Usage = func() { usage(stderr) }
	if err := global.Parse(args); err != nil {
		return 2
	}
	rest := global.Args()
	if len(rest) == 0 {
		usage(stderr)
		return 2
	}
	c := &client{base: strings.TrimRight(*server, "/"), http: &http.Client{Timeout: 30 * time.Second}, out: stdout}
	if err := dispatch(c, rest[0], rest[1:]); err != nil {
		fmt.Fprintln(stderr, "sagactl:", err)
		if errors.Is(err, errUsage) {
			return 2
		}
		return 1
	}
	return 0
}

var errUsage = errors.New("usage")

func usageErr(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{errUsage}, args...)...)
}

// parse splits positional arguments from flags so that flags may follow
// them (sagactl start pix-payout -key k).
func parse(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for len(args) > 0 {
		if strings.HasPrefix(args[0], "-") {
			if err := fs.Parse(args); err != nil {
				return nil, usageErr("%v", err)
			}
			args = fs.Args()
			continue
		}
		pos = append(pos, args[0])
		args = args[1:]
	}
	return pos, nil
}

func dispatch(c *client, cmd string, args []string) error {
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	switch cmd {
	case "start":
		key := fs.String("key", "", "idempotency key (required)")
		input := fs.String("input", "{}", "JSON input, or @file")
		pos, err := parse(fs, args)
		if err != nil {
			return err
		}
		if len(pos) != 1 || *key == "" {
			return usageErr("start <type> -key K [-input JSON]")
		}
		body := []byte(*input)
		if strings.HasPrefix(*input, "@") {
			if body, err = os.ReadFile((*input)[1:]); err != nil {
				return err
			}
		}
		b, err := c.do("POST", "/v1/sagas/"+url.PathEscape(pos[0]), body, map[string]string{"Idempotency-Key": *key})
		if err != nil {
			return err
		}
		var in store.Instance
		if err := json.Unmarshal(b, &in); err != nil {
			return err
		}
		fmt.Fprintf(c.out, "%s %s\n", in.ID, in.Status)
		return nil
	case "get":
		pos, err := parse(fs, args)
		if err != nil || len(pos) != 1 {
			return usageErr("get <id>")
		}
		b, err := c.do("GET", "/v1/sagas/"+url.PathEscape(pos[0]), nil, nil)
		if err != nil {
			return err
		}
		return printInstance(c.out, b)
	case "timeline":
		width := fs.Int("width", 50, "time axis width")
		pos, err := parse(fs, args)
		if err != nil || len(pos) != 1 {
			return usageErr("timeline <id> [-width N]")
		}
		b, err := c.do("GET", "/v1/sagas/"+url.PathEscape(pos[0]), nil, nil)
		if err != nil {
			return err
		}
		var in store.Instance
		if err := json.Unmarshal(b, &in); err != nil {
			return err
		}
		return saga.RenderTimeline(c.out, &in, *width)
	case "list", "stuck":
		status := fs.String("status", "", "filter by status")
		typ := fs.String("type", "", "filter by saga type")
		limit := fs.Int("limit", 0, "maximum results")
		older := fs.Duration("older-than", 5*time.Minute, "stuck: in flight without progress for this long")
		if _, err := parse(fs, args); err != nil {
			return err
		}
		q := url.Values{}
		path := "/v1/sagas"
		if cmd == "stuck" {
			path = "/v1/stuck"
			q.Set("older_than", older.String())
		} else {
			if *status != "" {
				q.Set("status", *status)
			}
			if *typ != "" {
				q.Set("type", *typ)
			}
			if *limit > 0 {
				q.Set("limit", fmt.Sprint(*limit))
			}
		}
		if len(q) > 0 {
			path += "?" + q.Encode()
		}
		b, err := c.do("GET", path, nil, nil)
		if err != nil {
			return err
		}
		var rows []httpapi.Summary
		if err := json.Unmarshal(b, &rows); err != nil {
			return err
		}
		tw := tabwriter.NewWriter(c.out, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tTYPE\tSTATUS\tSTEP\tUPDATED\tFAILURE")
		for _, r := range rows {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", r.ID, r.Type, r.Status, r.Step, r.UpdatedAt.Format(time.RFC3339), r.Failure)
		}
		return tw.Flush()
	case "signal":
		pos, err := parse(fs, args)
		if err != nil || len(pos) < 2 || len(pos) > 3 {
			return usageErr("signal <id> <name> [JSON]")
		}
		var body []byte
		if len(pos) == 3 {
			body = []byte(pos[2])
		}
		if _, err := c.do("POST", "/v1/sagas/"+url.PathEscape(pos[0])+"/signals/"+url.PathEscape(pos[1]), body, nil); err != nil {
			return err
		}
		fmt.Fprintln(c.out, "signal accepted")
		return nil
	case "retry", "resolve":
		note := fs.String("note", "", "note kept in the saga history")
		pos, err := parse(fs, args)
		if err != nil {
			return err
		}
		m := map[string]string{"note": *note}
		if cmd == "retry" && len(pos) != 1 {
			return usageErr("retry <id> [-note TEXT]")
		}
		if cmd == "resolve" {
			if len(pos) != 2 {
				return usageErr("resolve <id> COMPLETED|COMPENSATED [-note TEXT]")
			}
			m["outcome"] = strings.ToUpper(pos[1])
		}
		body, _ := json.Marshal(m)
		b, err := c.do("POST", "/v1/sagas/"+url.PathEscape(pos[0])+"/"+cmd, body, nil)
		if err != nil {
			return err
		}
		return printInstance(c.out, b)
	case "types":
		b, err := c.do("GET", "/v1/types", nil, nil)
		if err != nil {
			return err
		}
		var t []string
		if err := json.Unmarshal(b, &t); err != nil {
			return err
		}
		fmt.Fprintln(c.out, strings.Join(t, "\n"))
		return nil
	}
	return usageErr("unknown command %q", cmd)
}

func printInstance(w io.Writer, b []byte) error {
	var in store.Instance
	if err := json.Unmarshal(b, &in); err != nil {
		return err
	}
	fmt.Fprintf(w, "id:       %s\ntype:     %s\nkey:      %s\nstatus:   %s\n", in.ID, in.Type, in.Key, in.Status)
	if in.Failure != "" {
		fmt.Fprintf(w, "failure:  %s\n", in.Failure)
	}
	fmt.Fprintf(w, "state:    %s\nsteps:\n", in.State)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, s := range in.Steps {
		comp := ""
		if s.Comp != store.CompNone {
			comp = "compensation " + string(s.Comp)
		}
		errText := s.LastError
		if s.CompError != "" {
			errText = s.CompError
		}
		fmt.Fprintf(tw, "  %s\t%s\tattempts=%d\t%s\t%s\n", s.Name, s.Status, s.Attempts+s.Abandoned, comp, errText)
	}
	return tw.Flush()
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}
