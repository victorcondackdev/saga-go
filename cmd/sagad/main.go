// Command sagad runs the saga engine as a service: an HTTP API (see package
// httpapi) in front of a worker pool, over an in-memory or WAL store. It
// registers the two example saga types, pix-payout and boleto-payment,
// wired to in-process mock systems with configurable failure rates.
//
//	sagad -addr :8080 -store wal -dir ./data -fail-rate 0.2
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	boleto "github.com/victorcondackdev/saga-go/examples/boleto-payment"
	payout "github.com/victorcondackdev/saga-go/examples/pix-payout"
	"github.com/victorcondackdev/saga-go/httpapi"
	"github.com/victorcondackdev/saga-go/saga"
	"github.com/victorcondackdev/saga-go/store"
)

type config struct {
	addr        string
	storeKind   string
	dir         string
	sync        string
	workers     int
	lease       time.Duration
	failRate    float64
	settleAfter time.Duration
	logLevel    string
}

func parseFlags(args []string, stderr io.Writer) (config, error) {
	var c config
	fs := flag.NewFlagSet("sagad", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&c.addr, "addr", ":8080", "listen address")
	fs.StringVar(&c.storeKind, "store", "memory", "store: memory or wal")
	fs.StringVar(&c.dir, "dir", "./sagad-data", "WAL directory (store=wal)")
	fs.StringVar(&c.sync, "sync", "always", "WAL fsync policy: always, interval or never")
	fs.IntVar(&c.workers, "workers", 16, "instances processed concurrently")
	fs.DurationVar(&c.lease, "lease", 30*time.Second, "lease duration")
	fs.Float64Var(&c.failRate, "fail-rate", 0.1, "failure rate of the mock systems (0..1)")
	fs.DurationVar(&c.settleAfter, "settle-after", 500*time.Millisecond, "delay of the mock PSP settlement webhook")
	fs.StringVar(&c.logLevel, "log-level", "info", "debug, info, warn or error")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	if c.storeKind != "memory" && c.storeKind != "wal" {
		return c, fmt.Errorf("unknown store %q", c.storeKind)
	}
	if c.failRate < 0 || c.failRate > 1 {
		return c, errors.New("fail-rate must be within [0, 1]")
	}
	return c, nil
}

type service struct {
	engine  *saga.Engine
	handler http.Handler
	store   store.Store
}

func newService(c config, log *slog.Logger) (*service, error) {
	var st store.Store
	switch c.storeKind {
	case "wal":
		policy, err := store.ParseSyncPolicy(c.sync)
		if err != nil {
			return nil, err
		}
		w, err := store.OpenWAL(store.WALOptions{Dir: c.dir, Sync: policy})
		if err != nil {
			return nil, err
		}
		r := w.Recovery()
		log.Info("wal recovered", "dir", c.dir, "segments", r.Segments, "records", r.Records, "instances", r.Instances, "truncated_bytes", r.TruncatedBytes)
		st = w
	default:
		st = store.NewMemory()
	}
	eng := saga.New(st, saga.WithLogger(log), saga.WithConcurrency(c.workers), saga.WithLease(c.lease), saga.WithWaitRecheck(2*time.Second))
	if err := registerDemo(eng, c); err != nil {
		st.Close()
		return nil, err
	}
	return &service{engine: eng, handler: httpapi.New(eng), store: st}, nil
}

// registerDemo wires the example sagas to mock systems.
func registerDemo(eng *saga.Engine, c config) error {
	seed := uint64(time.Now().UnixNano())
	ledger := payout.NewLedger(map[string]int64{"acc-1": 1_000_000_00, "acc-2": 50_00})
	psp := payout.NewPSP(seed)
	psp.FailRate, psp.TimeoutRate, psp.RejectRate, psp.SettleRejectRate = c.failRate, c.failRate/2, c.failRate/10, c.failRate/5
	psp.Hang, psp.SettleAfter = 3*time.Second, c.settleAfter
	psp.OnSettle = func(sendKey string, s payout.Settlement) {
		id := strings.TrimSuffix(sendKey, ":send-pix")
		_ = eng.Signal(context.Background(), id, "settlement", s)
	}
	notifier := payout.NewNotifier(seed)
	notifier.FailRate = c.failRate
	accounts := boleto.NewAccounts(map[string]int64{"acc-1": 1_000_000_00, "acc-2": 50_00})
	svc := boleto.NewServices(seed)
	svc.FailRate, svc.FraudDeny, svc.BankRejected = c.failRate, c.failRate, c.failRate/5
	return eng.Register(
		payout.NewDefinition(payout.Deps{Ledger: ledger, PSP: psp, Notifier: notifier, SendTimeout: time.Second, SettlementDeadline: time.Minute}),
		boleto.NewDefinition(accounts, svc),
	)
}

func run(ctx context.Context, args []string, stderr io.Writer, ready func(addr string)) error {
	c, err := parseFlags(args, stderr)
	if err != nil {
		return err
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(c.logLevel)); err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: level}))
	svc, err := newService(c, log)
	if err != nil {
		return err
	}
	defer svc.store.Close()

	ln, err := net.Listen("tcp", c.addr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: svc.handler, ReadHeaderTimeout: 5 * time.Second}
	workers := make(chan struct{})
	go func() { svc.engine.Run(ctx); close(workers) }()
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	log.Info("sagad listening", "addr", ln.Addr().String(), "store", c.storeKind, "owner", svc.engine.Owner())
	if ready != nil {
		ready(ln.Addr().String())
	}
	select {
	case <-ctx.Done():
	case err := <-errc:
		return err
	}
	log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = srv.Shutdown(sctx)
	<-workers
	return err
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stderr, nil); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "sagad:", err)
		os.Exit(1)
	}
}
