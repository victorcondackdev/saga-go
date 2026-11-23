package store_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/victorcondackdev/saga-go/store"
)

func openWAL(t testing.TB, dir string, opts ...func(*store.WALOptions)) *store.WAL {
	t.Helper()
	o := store.WALOptions{Dir: dir, Sync: store.SyncNever}
	for _, f := range opts {
		f(&o)
	}
	w, err := store.OpenWAL(o)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// dump renders the full observable state of a store, leases included.
func dump(t testing.TB, s store.Store) string {
	t.Helper()
	all, err := s.List(context.Background(), store.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(all)
	return string(b)
}

// mutate applies op i, each producing exactly one log record.
func mutate(t testing.TB, s store.Store, i int) {
	t.Helper()
	ctx := context.Background()
	id := fmt.Sprint("s", i/5)
	var err error
	switch i % 5 {
	case 0:
		_, _, err = s.Create(ctx, inst(id, id))
	case 1:
		_, err = s.Acquire(ctx, "e1", t0.Add(time.Duration(i)*time.Hour), time.Minute, 1)
	case 2:
		err = s.Heartbeat(ctx, id, "e1", t0.Add(time.Duration(i)*time.Hour))
	case 3:
		var in *store.Instance
		in, err = s.Get(ctx, id)
		if err == nil {
			in.Status, in.Owner = store.StatusNeedsAttention, "" // park it so later acquires pick the next one
			in.State = json.RawMessage(fmt.Sprintf(`{"n":%d}`, i))
			in.History = append(in.History, store.Event{Seq: 2, Kind: store.EvStarted, At: t0})
			err = s.Update(ctx, in, "e1")
		}
	case 4:
		err = s.Signal(ctx, id, "sig", json.RawMessage(fmt.Sprint(i)))
	}
	if err != nil {
		t.Fatalf("op %d: %v", i, err)
	}
}

func segments(t testing.TB, dir string) []string {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	sort.Strings(m)
	return m
}

// boundaries returns the end offset of every complete record in a segment.
func boundaries(b []byte) []int {
	var out []int
	off := 8
	for off+8 <= len(b) {
		n := int(binary.LittleEndian.Uint32(b[off:]))
		if off+8+n > len(b) {
			break
		}
		off += 8 + n
		out = append(out, off)
	}
	return out
}

func copyDir(t testing.TB, src string) string {
	t.Helper()
	dst := t.TempDir()
	ents, _ := os.ReadDir(src)
	for _, e := range ents {
		b, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		os.WriteFile(filepath.Join(dst, e.Name()), b, 0o640)
	}
	return dst
}

// TestWALCrashRecoveryAtRandomOffsets cuts the log at random byte offsets,
// the signature of a crash mid-write, and checks that recovery yields
// exactly the state after the last complete record.
func TestWALCrashRecoveryAtRandomOffsets(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir)
	const ops = 60
	snaps := []string{dump(t, w)}
	for i := 0; i < ops; i++ {
		mutate(t, w, i)
		snaps = append(snaps, dump(t, w))
	}
	w.Close()
	seg := segments(t, dir)
	if len(seg) != 1 {
		t.Fatalf("want one segment, got %d", len(seg))
	}
	data, _ := os.ReadFile(seg[0])
	bounds := boundaries(data)
	if len(bounds) != ops {
		t.Fatalf("records %d, ops %d", len(bounds), ops)
	}
	rnd := rand.New(rand.NewPCG(1, 2))
	offsets := []int{0, 3, 8, 9, len(data) - 1, len(data)}
	for i := 0; i < 150; i++ {
		offsets = append(offsets, rnd.IntN(len(data)+1))
	}
	for _, off := range offsets {
		cut := copyDir(t, dir)
		if err := os.Truncate(filepath.Join(cut, filepath.Base(seg[0])), int64(off)); err != nil {
			t.Fatal(err)
		}
		complete := 0
		for complete < len(bounds) && bounds[complete] <= off {
			complete++
		}
		r := openWAL(t, cut)
		if got := dump(t, r); got != snaps[complete] {
			t.Fatalf("cut at %d: recovered state differs from state after %d records\n got %s\nwant %s", off, complete, got, snaps[complete])
		}
		if off > 8 && r.Recovery().TruncatedBytes != int64(off-max(8, boundOrZero(bounds, complete))) {
			t.Fatalf("cut at %d: truncated %d", off, r.Recovery().TruncatedBytes)
		}
		// The recovered log keeps working and survives another restart.
		mutate(t, r, 0+5*1000)
		want := dump(t, r)
		r.Close()
		r2 := openWAL(t, cut)
		if dump(t, r2) != want {
			t.Fatalf("cut at %d: state after append+reopen differs", off)
		}
		r2.Close()
	}
}

func boundOrZero(b []int, n int) int {
	if n == 0 {
		return 0
	}
	return b[n-1]
}

func TestWALCorruption(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir, func(o *store.WALOptions) { o.SegmentSize = 2048 })
	for i := 0; i < 80; i++ {
		mutate(t, w, i)
	}
	w.Close()
	seg := segments(t, dir)
	if len(seg) < 3 {
		t.Fatalf("want several segments, got %d", len(seg))
	}

	t.Run("bit flip in the newest segment truncates there", func(t *testing.T) {
		cut := copyDir(t, dir)
		last := filepath.Join(cut, filepath.Base(seg[len(seg)-1]))
		b, _ := os.ReadFile(last)
		bounds := boundaries(b)
		if len(bounds) < 2 {
			t.Skip("newest segment too small")
		}
		b[bounds[0]+10] ^= 0xff // inside the second record
		os.WriteFile(last, b, 0o640)
		r := openWAL(t, cut)
		defer r.Close()
		if r.Recovery().TruncatedBytes == 0 {
			t.Fatal("expected truncation")
		}
	})
	t.Run("bit flip in an older segment is reported", func(t *testing.T) {
		cut := copyDir(t, dir)
		first := filepath.Join(cut, filepath.Base(seg[0]))
		b, _ := os.ReadFile(first)
		b[20] ^= 0xff
		os.WriteFile(first, b, 0o640)
		if _, err := store.OpenWAL(store.WALOptions{Dir: cut}); err == nil {
			t.Fatal("corruption in a sealed segment went unnoticed")
		}
	})
	t.Run("valid frame with garbage payload is reported", func(t *testing.T) {
		cut := t.TempDir()
		frame := encode([]byte(`{"op":"lease","id":"nobody"}`))
		os.WriteFile(filepath.Join(cut, "00000000000000000001.wal"), append(magic(), frame...), 0o640)
		if _, err := store.OpenWAL(store.WALOptions{Dir: cut}); err == nil {
			t.Fatal("lease for unknown instance accepted")
		}
	})
}

func magic() []byte { return []byte{'S', 'A', 'G', 'A', 'W', 'A', 'L', 1} }

func encode(data []byte) []byte {
	// Mirrors the framing: len u32 | crc32c(len||data) u32 | data.
	hdr := make([]byte, 8)
	binary.LittleEndian.PutUint32(hdr, uint32(len(data)))
	crc := crc32c(append(append([]byte(nil), hdr[:4]...), data...))
	binary.LittleEndian.PutUint32(hdr[4:], crc)
	return append(hdr, data...)
}

func TestWALCompaction(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir, func(o *store.WALOptions) { o.SegmentSize = 4096; o.CompactAt = -1 })
	for i := 0; i < 50; i++ {
		mutate(t, w, i)
	}
	// Many updates of the same instances: the log grows, the state does not.
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		in, _ := w.Get(ctx, fmt.Sprint("s", i%10))
		in.State = json.RawMessage(fmt.Sprintf(`{"n":%d}`, i))
		if err := w.Update(ctx, in, in.Owner); err != nil {
			t.Fatal(err)
		}
	}
	want := dump(t, w)
	before := w.Size()
	oldSegs := segments(t, dir)
	saved := map[string][]byte{}
	for _, s := range oldSegs {
		saved[s], _ = os.ReadFile(s)
	}
	if err := w.Compact(); err != nil {
		t.Fatal(err)
	}
	if w.Size() >= before/2 {
		t.Fatalf("compaction did not shrink the log: %d -> %d", before, w.Size())
	}
	if dump(t, w) != want {
		t.Fatal("compaction changed the state")
	}
	mutate(t, w, 1000) // appends after compaction
	want = dump(t, w)
	w.Close()

	r := openWAL(t, dir)
	if dump(t, r) != want {
		t.Fatal("state after compaction and reopen differs")
	}
	r.Close()

	// A crash after the compacted segment was written but before the old
	// segments were deleted: both are on disk. Replay must agree.
	for p, b := range saved {
		os.WriteFile(p, b, 0o640)
	}
	r = openWAL(t, dir)
	if dump(t, r) != want {
		t.Fatal("state with leftover pre-compaction segments differs")
	}
	if r.Recovery().Instances != 11 {
		t.Fatalf("instances %d", r.Recovery().Instances)
	}
	r.Close()
}

func TestWALAutoCompaction(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir, func(o *store.WALOptions) { o.SegmentSize = 2048; o.CompactAt = 8192 })
	ctx := context.Background()
	w.Create(ctx, inst("a", "a"))
	for i := 0; i < 500; i++ {
		in, _ := w.Get(ctx, "a")
		in.State = json.RawMessage(fmt.Sprintf(`{"n":%d}`, i))
		if err := w.Update(ctx, in, ""); err != nil {
			t.Fatal(err)
		}
	}
	if w.Size() > 3*8192 {
		t.Fatalf("log not compacted automatically: %d bytes", w.Size())
	}
	want := dump(t, w)
	w.Close()
	r := openWAL(t, dir)
	defer r.Close()
	if dump(t, r) != want {
		t.Fatal("state differs after auto-compaction")
	}
}

func TestWALSyncAlwaysConcurrent(t *testing.T) {
	w := openWAL(t, t.TempDir(), func(o *store.WALOptions) { o.Sync = store.SyncAlways })
	defer w.Close()
	ctx := context.Background()
	done := make(chan error)
	for g := 0; g < 8; g++ {
		go func(g int) {
			for i := 0; i < 20; i++ {
				if _, _, err := w.Create(ctx, inst(fmt.Sprintf("g%d-%d", g, i), fmt.Sprintf("g%d-%d", g, i))); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}(g)
	}
	for g := 0; g < 8; g++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if st, _ := w.Stats(ctx); st[store.StatusPending] != 160 {
		t.Fatalf("stats %v", st)
	}
}

func TestWALOptions(t *testing.T) {
	if _, err := store.OpenWAL(store.WALOptions{}); err == nil {
		t.Fatal("empty dir accepted")
	}
	for _, s := range []string{"always", "interval", "never"} {
		if _, err := store.ParseSyncPolicy(s); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.ParseSyncPolicy("sometimes"); err == nil {
		t.Fatal("bad policy accepted")
	}
	f := filepath.Join(t.TempDir(), "file")
	os.WriteFile(f, nil, 0o640)
	if _, err := store.OpenWAL(store.WALOptions{Dir: f}); err == nil {
		t.Fatal("file as dir accepted")
	}
	w := openWAL(t, t.TempDir())
	w.Close()
	if err := w.Compact(); err == nil {
		t.Fatal("compact after close")
	}
}

// FuzzOpenWAL feeds arbitrary bytes as a segment: OpenWAL must either fail
// cleanly or produce a usable store; it must never panic.
func FuzzOpenWAL(f *testing.F) {
	ok, _ := json.Marshal(map[string]any{"op": "put", "inst": inst("a", "a")})
	f.Add(encode(ok))
	f.Add(append(encode(ok), encode([]byte(`{"op":"lease","id":"a","owner":"x"}`))...))
	f.Add(encode([]byte(`{"op":"put"}`)))
	f.Add(encode([]byte(`not json`)))
	f.Add([]byte{0xff, 0xff, 0xff, 0x7f, 1, 2, 3, 4})
	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, "00000000000000000001.wal"), append(magic(), data...), 0o640)
		w, err := store.OpenWAL(store.WALOptions{Dir: dir, Sync: store.SyncNever})
		if err != nil {
			return
		}
		defer w.Close()
		if _, _, err := w.Create(context.Background(), inst("fuzz-new", "fuzz-new-key")); err != nil {
			t.Fatalf("recovered store unusable: %v", err)
		}
	})
}
