package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/victorcondackdev/saga-go/store/internal/wal"
)

// SyncPolicy controls when the WAL store fsyncs.
type SyncPolicy = wal.SyncPolicy

// Sync policies for WALOptions.Sync.
const (
	// SyncAlways fsyncs before acknowledging a mutation. Concurrent
	// mutations share one fsync (group commit).
	SyncAlways = wal.SyncAlways
	// SyncInterval fsyncs in the background; a power loss can drop the
	// last interval of acknowledged mutations.
	SyncInterval = wal.SyncInterval
	// SyncNever leaves flushing to the OS: survives process crashes, not
	// power loss.
	SyncNever = wal.SyncNever
)

// ParseSyncPolicy parses "always", "interval" or "never".
func ParseSyncPolicy(s string) (SyncPolicy, error) { return wal.ParseSyncPolicy(s) }

// WALOptions configures a WAL store.
type WALOptions struct {
	// Dir holds the log segments. Required. A directory must be used by at
	// most one process at a time.
	Dir string
	// Sync is the fsync policy. Default SyncAlways.
	Sync SyncPolicy
	// SegmentSize is the size at which a segment is rotated. Default 16 MiB.
	SegmentSize int64
	// CompactAt is the log size above which the store rewrites the live
	// state into a fresh segment and deletes the old ones, provided the log
	// has also doubled since the previous compaction. Default 64 MiB;
	// negative disables automatic compaction.
	CompactAt int64
}

// WALRecovery describes what OpenWAL found on disk.
type WALRecovery struct {
	Segments       int
	Records        int
	Instances      int
	TruncatedBytes int64 // torn tail discarded from the newest segment
}

// WAL is a file-backed Store. Every mutation is appended to a segmented,
// CRC-checked write-ahead log before it is applied to the in-memory index,
// and is acknowledged only once durable under the configured SyncPolicy.
//
// On open the log is replayed; a torn record at the tail of the newest
// segment (a crash mid-append) is truncated, while corruption anywhere else
// is reported as an error rather than silently losing acknowledged data.
//
// WAL is safe for concurrent use by any number of engines in one process.
// It does not coordinate between processes.
type WAL struct {
	c           *core
	log         *wal.Log
	opts        WALOptions
	recovery    WALRecovery
	lastCompact int64
}

// OpenWAL opens or creates a WAL store in opts.Dir and recovers its state.
func OpenWAL(opts WALOptions) (*WAL, error) {
	if opts.Dir == "" {
		return nil, errors.New("store: WALOptions.Dir is required")
	}
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = 16 << 20
	}
	if opts.CompactAt == 0 {
		opts.CompactAt = 64 << 20
	}
	l, err := wal.Open(wal.Options{Dir: opts.Dir, SegmentSize: opts.SegmentSize, Sync: opts.Sync})
	if err != nil {
		return nil, err
	}
	w := &WAL{c: newCore(), log: l, opts: opts}
	ri := l.Recovery()
	w.recovery = WALRecovery{Segments: ri.Segments, Records: ri.Records, TruncatedBytes: ri.TruncatedBytes}
	err = l.Replay(func(pos wal.Position, data []byte) error {
		r, err := decodeRecord(data)
		if err != nil {
			return fmt.Errorf("store: segment %d offset %d: %w", pos.Segment, pos.Offset, err)
		}
		return w.c.apply(r)
	})
	if err != nil {
		l.Close()
		return nil, err
	}
	w.recovery.Instances = len(w.c.byID)
	w.lastCompact = l.Size()
	w.c.journal = w.write
	w.c.wait = l.WaitSync
	return w, nil
}

func decodeRecord(data []byte) (*record, error) {
	var r record
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	if r.Op != opPut && r.Op != opLease {
		return nil, fmt.Errorf("unknown op %q", r.Op)
	}
	if r.Op == opPut && (r.Inst == nil || r.Inst.ID == "") {
		return nil, errors.New("put without instance")
	}
	return &r, nil
}

func (w *WAL) write(r *record) (uint64, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return 0, err
	}
	_, t, err := w.log.Write([][]byte{b})
	return t, err
}

// Recovery reports what OpenWAL found on disk.
func (w *WAL) Recovery() WALRecovery { return w.recovery }

// Size returns the current size of the log in bytes.
func (w *WAL) Size() int64 { return w.log.Size() }

// Compact rewrites the live state into a new segment and removes every
// older segment. A crash at any point during compaction is safe: until the
// old segments are deleted, replaying them followed by the (possibly torn)
// compacted segment yields the same state, because every compacted record
// is a full snapshot that supersedes what came before it.
func (w *WAL) Compact() error {
	w.c.mu.Lock()
	defer w.c.mu.Unlock()
	return w.compactLocked()
}

func (w *WAL) compactLocked() error {
	if w.c.closed {
		return ErrClosed
	}
	id, err := w.log.Rotate()
	if err != nil {
		return err
	}
	ids := make([]string, 0, len(w.c.byID))
	for k := range w.c.byID {
		ids = append(ids, k)
	}
	sort.Strings(ids)
	batch := make([][]byte, 0, 64)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		_, _, err := w.log.Write(batch)
		batch = batch[:0]
		return err
	}
	for _, k := range ids {
		b, err := json.Marshal(&record{Op: opPut, Inst: w.c.byID[k]})
		if err != nil {
			return err
		}
		batch = append(batch, b)
		if len(batch) == cap(batch) {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if err := w.log.Sync(); err != nil {
		return err
	}
	if err := w.log.RemoveBefore(id); err != nil {
		return err
	}
	w.lastCompact = w.log.Size()
	return nil
}

func (w *WAL) maybeCompact() {
	if w.opts.CompactAt < 0 {
		return
	}
	size := w.log.Size()
	if size < w.opts.CompactAt || size < 2*w.lastCompact {
		return
	}
	w.c.mu.Lock()
	defer w.c.mu.Unlock()
	if w.log.Size() >= 2*w.lastCompact { // re-check under the lock
		_ = w.compactLocked()
	}
}

// Create implements Store.
func (w *WAL) Create(ctx context.Context, in *Instance) (*Instance, bool, error) {
	defer w.maybeCompact()
	return w.c.create(ctx, in)
}

// Get implements Store.
func (w *WAL) Get(ctx context.Context, id string) (*Instance, error) { return w.c.get(ctx, id) }

// Update implements Store.
func (w *WAL) Update(ctx context.Context, in *Instance, owner string) error {
	defer w.maybeCompact()
	return w.c.update(ctx, in, owner)
}

// Acquire implements Store.
func (w *WAL) Acquire(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]*Instance, error) {
	return w.c.acquire(ctx, owner, now, lease, limit)
}

// Heartbeat implements Store.
func (w *WAL) Heartbeat(ctx context.Context, id, owner string, until time.Time) error {
	return w.c.heartbeat(ctx, id, owner, until)
}

// Release implements Store.
func (w *WAL) Release(ctx context.Context, id, owner string) error {
	return w.c.release(ctx, id, owner)
}

// Signal implements Store.
func (w *WAL) Signal(ctx context.Context, id, name string, payload json.RawMessage) error {
	return w.c.signal(ctx, id, name, payload)
}

// List implements Store.
func (w *WAL) List(ctx context.Context, f Filter) ([]*Instance, error) { return w.c.list(ctx, f) }

// Stats implements Store.
func (w *WAL) Stats(ctx context.Context) (map[Status]int, error) { return w.c.stats(ctx) }

// Close flushes and closes the log. Further calls return ErrClosed.
func (w *WAL) Close() error {
	w.c.mu.Lock()
	if w.c.closed {
		w.c.mu.Unlock()
		return nil
	}
	w.c.closed = true
	w.c.mu.Unlock()
	return w.log.Close()
}
