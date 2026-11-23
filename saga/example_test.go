package saga_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/victorcondackdev/saga-go/saga"
	"github.com/victorcondackdev/saga-go/sagatest"
	"github.com/victorcondackdev/saga-go/store"
)

type Transfer struct {
	From, To string
	Amount   int64
	HoldID   string
	Log      []string
}

func Example() {
	ctx := context.Background()
	def := saga.Define[Transfer]("transfer").
		Step("hold", func(sc saga.Context, t *Transfer) error {
			t.HoldID = "hold-" + sc.IdempotencyKey()
			t.Log = append(t.Log, "held")
			return nil
		}, saga.Compensate(func(sc saga.Context, t *Transfer) error {
			t.Log = append(t.Log, "released")
			return nil
		})).
		Step("credit", func(sc saga.Context, t *Transfer) error {
			return saga.Permanent(errors.New("destination account closed"))
		}).
		MustBuild()

	eng := saga.New(store.NewMemory(), saga.WithLogger(saga.DiscardLogger()))
	if err := eng.Register(def); err != nil {
		panic(err)
	}
	id, err := saga.Start(ctx, eng, def, "transfer-1", Transfer{From: "a", To: "b", Amount: 100})
	if err != nil {
		panic(err)
	}
	eng.Tick(ctx)

	in, _ := eng.Get(ctx, id)
	t, _ := saga.StateOf[Transfer](in)
	fmt.Println(in.Status)
	fmt.Println(strings.Join(t.Log, " -> "))
	fmt.Println(in.Failure)
	// Output:
	// COMPENSATED
	// held -> released
	// step "credit" failed: permanent: destination account closed
}

func ExampleRenderTimeline() {
	ctx := context.Background()
	clock := sagatest.NewClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	calls := 0
	def := saga.Define[Transfer]("timeline").
		Step("reserve", func(sc saga.Context, t *Transfer) error { return nil }).
		Step("send", func(sc saga.Context, t *Transfer) error {
			calls++
			if calls == 1 {
				return errors.New("psp 503")
			}
			return nil
		}, saga.Retry(saga.RetryPolicy{Initial: time.Second, Jitter: 0.0000001})).
		MustBuild()
	eng := saga.New(store.NewMemory(), saga.WithClock(clock), saga.WithLogger(saga.DiscardLogger()), saga.WithRandSeed(1))
	eng.Register(def)
	id, _ := saga.Start(ctx, eng, def, "k", Transfer{})
	eng.Tick(ctx)
	clock.Advance(time.Second)
	eng.Tick(ctx)

	in, _ := eng.Get(ctx, id)
	in.ID = "sg_example" // ids are random
	saga.RenderTimeline(os.Stdout, in, 20)
	// Output:
	// saga sg_example  type=timeline  status=COMPLETED  span=1s
	//                                  |0s----------------1s|
	// * saga_created                   |+                   |   key=k
	// * saga_started                   |+                   |
	// reserve            #1  ok        |>                   | 0s
	// send               #1  retry     |!                   | 0s  psp 503
	// send               #2  ok        |                   >| 0s
	// * saga_completed                 |                   +|
}
