package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

// record is one journaled mutation. A "put" carries a full snapshot of an
// instance; a "lease" only changes lease fields, which keeps acquire,
// heartbeat and release cheap to log.
type record struct {
	Op      string    `json:"op"`
	Inst    *Instance `json:"inst,omitempty"`
	ID      string    `json:"id,omitempty"`
	Owner   string    `json:"owner,omitempty"`
	Until   time.Time `json:"until,omitempty"`
	Version int64     `json:"version,omitempty"`
}

const (
	opPut   = "put"
	opLease = "lease"
)

// core is the indexed in-memory state shared by Memory and WAL. Every
// mutation is first handed to journal (nil for Memory) and only applied once
// the journal accepted it, so the in-memory view never runs ahead of what a
// restart would recover.
type core struct {
	mu     sync.Mutex
	byID   map[string]*Instance
	byKey  map[string]string
	active map[string]struct{}
	closed bool
	// journal writes r (without waiting for durability) and returns a
	// ticket; wait blocks until that ticket is durable. wait runs after the
	// lock is dropped, so concurrent writers share one fsync.
	journal func(*record) (uint64, error)
	wait    func(uint64) error
	tickets uint64 // highest ticket issued under mu, read by mutators
}

func newCore() *core {
	return &core{
		byID:   make(map[string]*Instance),
		byKey:  make(map[string]string),
		active: make(map[string]struct{}),
	}
}

func keyOf(typ, key string) string { return typ + "\x00" + key }

// apply installs r. It is used both after journaling and during replay.
func (c *core) apply(r *record) error {
	switch r.Op {
	case opPut:
		if r.Inst == nil || r.Inst.ID == "" {
			return fmt.Errorf("store: put record without instance")
		}
		in := r.Inst
		c.byID[in.ID] = in
		c.byKey[keyOf(in.Type, in.Key)] = in.ID
		if in.Status.Runnable() {
			c.active[in.ID] = struct{}{}
		} else {
			delete(c.active, in.ID)
		}
	case opLease:
		in, ok := c.byID[r.ID]
		if !ok {
			return fmt.Errorf("store: lease record for unknown instance %q", r.ID)
		}
		in.Owner, in.LeaseUntil = r.Owner, r.Until
		if r.Version != 0 {
			in.Version = r.Version
		}
	default:
		return fmt.Errorf("store: unknown record op %q", r.Op)
	}
	return nil
}

func (c *core) commit(r *record) error {
	if c.journal != nil {
		t, err := c.journal(r)
		if err != nil {
			return err
		}
		c.tickets = t
	}
	return c.apply(r)
}

// unlock releases mu and, if the critical section journaled anything, waits
// for it to become durable before the mutation is acknowledged.
func (c *core) unlock(wrote bool, err *error) {
	t := c.tickets
	c.mu.Unlock()
	if wrote && *err == nil && c.wait != nil {
		*err = c.wait(t)
	}
}

func (c *core) create(_ context.Context, in *Instance) (out *Instance, created bool, err error) {
	c.mu.Lock()
	defer c.unlock(true, &err)
	if c.closed {
		return nil, false, ErrClosed
	}
	if id, ok := c.byKey[keyOf(in.Type, in.Key)]; ok {
		return c.byID[id].Clone(), false, nil
	}
	if _, ok := c.byID[in.ID]; ok || in.ID == "" {
		return nil, false, fmt.Errorf("store: invalid or duplicate id %q", in.ID)
	}
	n := in.Clone()
	n.Version = 1
	if err := c.commit(&record{Op: opPut, Inst: n}); err != nil {
		return nil, false, err
	}
	in.Version = n.Version
	return n.Clone(), true, nil
}

func (c *core) get(_ context.Context, id string) (*Instance, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	in, ok := c.byID[id]
	if !ok {
		return nil, ErrNotFound
	}
	return in.Clone(), nil
}

func (c *core) update(_ context.Context, in *Instance, owner string) (err error) {
	c.mu.Lock()
	defer c.unlock(true, &err)
	if c.closed {
		return ErrClosed
	}
	cur, ok := c.byID[in.ID]
	if !ok {
		return ErrNotFound
	}
	if cur.Version != in.Version || cur.Owner != owner {
		return ErrConflict
	}
	n := in.Clone()
	n.Version = cur.Version + 1
	n.Type, n.Key = cur.Type, cur.Key
	if cur.SignalSeq != in.SignalSeq {
		n.NextRunAt = time.Time{}
	}
	n.SignalSeq = cur.SignalSeq
	n.Signals = cloneSignals(cur.Signals)
	if err := c.commit(&record{Op: opPut, Inst: n}); err != nil {
		return err
	}
	in.Version = n.Version
	return nil
}

func (c *core) acquire(_ context.Context, owner string, now time.Time, lease time.Duration, limit int) (_ []*Instance, err error) {
	c.mu.Lock()
	defer c.unlock(true, &err)
	if c.closed {
		return nil, ErrClosed
	}
	if limit <= 0 || owner == "" {
		return nil, nil
	}
	// Keep the limit earliest due instances (bounded insertion: O(n log k)
	// rather than sorting the whole backlog on every poll).
	less := func(a, b *Instance) bool {
		if !a.NextRunAt.Equal(b.NextRunAt) {
			return a.NextRunAt.Before(b.NextRunAt)
		}
		return a.ID < b.ID
	}
	if limit > len(c.active) {
		limit = len(c.active)
	}
	if limit == 0 {
		return nil, nil
	}
	due := make([]*Instance, 0, limit)
	for id := range c.active {
		in := c.byID[id]
		if in.NextRunAt.After(now) || (in.Owner != "" && in.LeaseUntil.After(now)) {
			continue
		}
		if len(due) == limit && !less(in, due[limit-1]) {
			continue
		}
		i := sort.Search(len(due), func(i int) bool { return less(in, due[i]) })
		if len(due) < limit {
			due = append(due, nil)
		}
		copy(due[i+1:], due[i:len(due)-1])
		due[i] = in
	}
	out := make([]*Instance, 0, len(due))
	for _, in := range due {
		prev := in.Owner
		r := &record{Op: opLease, ID: in.ID, Owner: owner, Until: now.Add(lease), Version: in.Version + 1}
		if err := c.commit(r); err != nil {
			return out, err
		}
		cl := in.Clone()
		if prev != owner {
			cl.PreviousOwner = prev
		}
		out = append(out, cl)
	}
	return out, nil
}

func (c *core) heartbeat(_ context.Context, id, owner string, until time.Time) (err error) {
	c.mu.Lock()
	defer c.unlock(true, &err)
	if c.closed {
		return ErrClosed
	}
	in, ok := c.byID[id]
	if !ok {
		return ErrNotFound
	}
	if owner == "" || in.Owner != owner {
		return ErrConflict
	}
	return c.commit(&record{Op: opLease, ID: id, Owner: owner, Until: until})
}

func (c *core) release(_ context.Context, id, owner string) (err error) {
	c.mu.Lock()
	defer c.unlock(true, &err)
	if c.closed {
		return ErrClosed
	}
	in, ok := c.byID[id]
	if !ok {
		return ErrNotFound
	}
	if owner == "" || in.Owner != owner {
		return ErrConflict
	}
	return c.commit(&record{Op: opLease, ID: id})
}

func (c *core) signal(_ context.Context, id, name string, payload json.RawMessage) (err error) {
	c.mu.Lock()
	defer c.unlock(true, &err)
	if c.closed {
		return ErrClosed
	}
	in, ok := c.byID[id]
	if !ok {
		return ErrNotFound
	}
	n := in.Clone()
	if n.Signals == nil {
		n.Signals = make(map[string]json.RawMessage)
	}
	if len(payload) == 0 {
		payload = json.RawMessage("null")
	}
	n.Signals[name] = cloneRaw(payload)
	n.SignalSeq++
	if n.Status.Runnable() {
		n.NextRunAt = time.Time{}
	}
	return c.commit(&record{Op: opPut, Inst: n})
}

func (c *core) list(_ context.Context, f Filter) ([]*Instance, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	var out []*Instance
	for _, in := range c.byID {
		if f.match(in) {
			out = append(out, in)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	for i, in := range out {
		out[i] = in.Clone()
	}
	return out, nil
}

func (c *core) stats(_ context.Context) (map[Status]int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	m := make(map[Status]int)
	for _, in := range c.byID {
		m[in.Status]++
	}
	return m, nil
}

// Memory is an in-memory Store. It is safe for concurrent use and is the
// reference implementation of the Store contract.
type Memory struct{ c *core }

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory { return &Memory{c: newCore()} }

// Create implements Store.
func (m *Memory) Create(ctx context.Context, in *Instance) (*Instance, bool, error) {
	return m.c.create(ctx, in)
}

// Get implements Store.
func (m *Memory) Get(ctx context.Context, id string) (*Instance, error) { return m.c.get(ctx, id) }

// Update implements Store.
func (m *Memory) Update(ctx context.Context, in *Instance, owner string) error {
	return m.c.update(ctx, in, owner)
}

// Acquire implements Store.
func (m *Memory) Acquire(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]*Instance, error) {
	return m.c.acquire(ctx, owner, now, lease, limit)
}

// Heartbeat implements Store.
func (m *Memory) Heartbeat(ctx context.Context, id, owner string, until time.Time) error {
	return m.c.heartbeat(ctx, id, owner, until)
}

// Release implements Store.
func (m *Memory) Release(ctx context.Context, id, owner string) error {
	return m.c.release(ctx, id, owner)
}

// Signal implements Store.
func (m *Memory) Signal(ctx context.Context, id, name string, payload json.RawMessage) error {
	return m.c.signal(ctx, id, name, payload)
}

// List implements Store.
func (m *Memory) List(ctx context.Context, f Filter) ([]*Instance, error) { return m.c.list(ctx, f) }

// Stats implements Store.
func (m *Memory) Stats(ctx context.Context) (map[Status]int, error) { return m.c.stats(ctx) }

// Close implements Store. Further calls return ErrClosed.
func (m *Memory) Close() error {
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	m.c.closed = true
	return nil
}
