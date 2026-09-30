package signal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

type faultBoard struct {
	mu   sync.Mutex
	fail map[uuid.UUID]map[int]bool // vessel -> attempts (1-based) that fail
}

func newFaultBoard() *faultBoard { return &faultBoard{fail: make(map[uuid.UUID]map[int]bool)} }

func (b *faultBoard) failOnce(id uuid.UUID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fail[id] == nil {
		b.fail[id] = make(map[int]bool)
	}
	b.fail[id][len(b.fail[id])+1] = true
}

func (b *faultBoard) hook() func(uuid.UUID, int) error {
	return func(id uuid.UUID, attempt int) error {
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.fail[id] != nil && b.fail[id][attempt] {
			delete(b.fail[id], attempt)
			return errors.New("simulated field write failure")
		}
		return nil
	}
}

func (b *faultBoard) clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.fail = make(map[uuid.UUID]map[int]bool)
}

func newHarness(t *testing.T, vessels ...model.Vessel) (*Publications, *Store, journal.Store, model.ConvoyPlan, model.TransitCycle, *faultBoard) {
	t.Helper()
	now := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	store := NewStore()
	journalStore := journal.NewMemoryStore()
	recorder := journal.NewRecorder(journalStore, clock)
	publisher := NewPublisher(store, recorder, clock)
	faults := newFaultBoard()
	publisher.SetWriteFault(faults.hook())
	publisher.SetCompensateFault(faults.hook())
	registry := NewPublications(publisher, clock)
	if len(vessels) == 0 {
		vessels = []model.Vessel{
			{ID: uuid.MustParse("11111111-0000-0000-0000-000000000001"), Name: "River Star", CallSign: "RS-1", LengthM: 80},
			{ID: uuid.MustParse("22222222-0000-0000-0000-000000000002"), Name: "Canal Dawn", CallSign: "CD-2", LengthM: 90},
			{ID: uuid.MustParse("33333333-0000-0000-0000-000000000003"), Name: "Lock Breeze", CallSign: "LB-3", LengthM: 70},
		}
	}
	plan, err := model.NewConvoyPlan("chamber-1", model.DirectionUpstream, vessels, now)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	cycle, err := model.NewTransitCycle(plan.ChamberID, plan.ID, plan.Direction, 1, now)
	if err != nil {
		t.Fatalf("cycle: %v", err)
	}
	return registry, store, journalStore, plan, cycle, faults
}

func allProceed(t *testing.T, store *Store, cycle model.TransitCycle, plan model.ConvoyPlan) {
	t.Helper()
	for _, vessel := range plan.Vessels {
		state, ok := store.Get(vessel.ID)
		if !ok {
			t.Errorf("vessel %s missing from signal store", vessel.CallSign)
			continue
		}
		if state.Aspect != model.SignalProceed || state.CycleID != cycle.ID || state.Generation != cycle.Generation {
			t.Errorf("vessel %s state = %+v, want proceed stamped for cycle %s gen %d",
				vessel.CallSign, state, cycle.ID, cycle.Generation)
		}
	}
}

// Repeated publish requests for the same plan return the same result and write
// each vessel exactly once.
func TestEnsureIsIdempotent(t *testing.T) {
	registry, store, journalStore, plan, cycle, _ := newHarness(t)
	ctx := context.Background()

	first, err := registry.Ensure(ctx, plan, cycle, model.SignalProceed)
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if first.Status != model.PublicationCompleted || first.Published != len(plan.Vessels) {
		t.Fatalf("first publish = %+v", first)
	}
	second, err := registry.Ensure(ctx, plan, cycle, model.SignalProceed)
	if err != nil {
		t.Fatalf("duplicate publish: %v", err)
	}
	if second.PlanID != first.PlanID || second.Status != model.PublicationCompleted {
		t.Fatalf("duplicate returned different result: %+v", second)
	}
	allProceed(t, store, cycle, plan)

	events, err := journalStore.Events(ctx, plan.ChamberID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != len(plan.Vessels) {
		t.Fatalf("expected %d signal events (no duplicate writes), got %d", len(plan.Vessels), len(events))
	}
}

// A failed vessel write rolls back, leaves the publication partial, and a
// repeated request safely recovers the remaining vessel.
func TestPartialFailureRecovers(t *testing.T) {
	registry, store, journalStore, plan, cycle, faults := newHarness(t)
	ctx := context.Background()
	faults.failOnce(plan.Vessels[1].ID) // second vessel fails its first attempt

	first, err := registry.Ensure(ctx, plan, cycle, model.SignalProceed)
	if err != nil {
		t.Fatalf("publish should return the partial snapshot, got error %v", err)
	}
	if first.Status != model.PublicationPartial || first.Published != 2 {
		t.Fatalf("want partial with 2 published, got %s/%d", first.Status, first.Published)
	}
	if first.Vessels[1].Status != model.VesselFailed || first.Vessels[1].Attempts != 1 {
		t.Fatalf("vessel 2 progress = %+v", first.Vessels[1])
	}
	if _, ok := store.Get(plan.Vessels[1].ID); ok {
		t.Fatal("failed vessel write must be rolled back out of the signal store")
	}

	// Same request again: same job, retries only the failed vessel.
	second, err := registry.Ensure(ctx, plan, cycle, model.SignalProceed)
	if err != nil {
		t.Fatalf("retry publish: %v", err)
	}
	if second.Status != model.PublicationCompleted || second.Published != 3 {
		t.Fatalf("retry = %+v", second)
	}
	if second.Vessels[1].Attempts != 2 {
		t.Fatalf("failed vessel should record 2 attempts, got %d", second.Vessels[1].Attempts)
	}
	allProceed(t, store, cycle, plan)

	events, _ := journalStore.Events(ctx, plan.ChamberID, 0)
	if len(events) != len(plan.Vessels) {
		t.Fatalf("failed writes must not append journal events; got %d", len(events))
	}
}

// Background recovery (Tick) finishes the publication even if the operator
// never retries the request.
func TestReaperRecoversPartial(t *testing.T) {
	registry, store, _, plan, cycle, faults := newHarness(t)
	ctx := context.Background()
	faults.failOnce(plan.Vessels[0].ID)

	if _, err := registry.Ensure(ctx, plan, cycle, model.SignalProceed); err != nil {
		t.Fatal(err)
	}
	registry.Tick(ctx) // first tick still sees the one-shot fault cleared after attempt 1

	snapshot, ok := registry.Get(plan.ID)
	if !ok {
		t.Fatal("publication missing")
	}
	if snapshot.Status != model.PublicationCompleted {
		t.Fatalf("after tick status = %s, want completed", snapshot.Status)
	}
	allProceed(t, store, cycle, plan)
}

// A cancelled publication compensates every written vessel and can never be
// published again.
func TestCancelBlocksRepublish(t *testing.T) {
	registry, store, _, plan, cycle, _ := newHarness(t)
	ctx := context.Background()

	if _, err := registry.Ensure(ctx, plan, cycle, model.SignalProceed); err != nil {
		t.Fatal(err)
	}
	snapshot, found, err := registry.Cancel(ctx, plan.ID, "operator cancellation")
	if err != nil || !found {
		t.Fatalf("cancel: found=%v err=%v", found, err)
	}
	if snapshot.Status != model.PublicationCompensated {
		t.Fatalf("cancel status = %s", snapshot.Status)
	}
	for _, vessel := range plan.Vessels {
		if _, ok := store.Get(vessel.ID); ok {
			t.Fatalf("vessel %s signal still present after compensation", vessel.CallSign)
		}
		if status := snapshot.Vessels[0].Status; status != model.VesselCompensated {
			t.Fatalf("vessel status = %s", status)
		}
	}

	if _, err := registry.Ensure(ctx, plan, cycle, model.SignalProceed); !errors.Is(err, ErrPublicationCancelled) {
		t.Fatalf("republish after cancel err = %v, want ErrPublicationCancelled", err)
	}
}

// Cancelling a partial publication rolls back written vessels and treats never
// written vessels as already safe.
func TestCancelMidPublication(t *testing.T) {
	registry, store, _, plan, cycle, faults := newHarness(t)
	ctx := context.Background()
	faults.failOnce(plan.Vessels[1].ID)
	faults.failOnce(plan.Vessels[2].ID)

	partial, err := registry.Ensure(ctx, plan, cycle, model.SignalProceed)
	if err != nil {
		t.Fatal(err)
	}
	if partial.Status != model.PublicationPartial || partial.Published != 1 {
		t.Fatalf("partial = %+v", partial)
	}

	snapshot, _, err := registry.Cancel(ctx, plan.ID, "stop convoy")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if snapshot.Status != model.PublicationCompensated {
		t.Fatalf("status = %s, snapshot = %+v", snapshot.Status, snapshot)
	}
	for _, progress := range snapshot.Vessels {
		if progress.Status != model.VesselCompensated {
			t.Fatalf("vessel %s = %s, want compensated", progress.CallSign, progress.Status)
		}
	}
	if _, ok := store.Get(plan.Vessels[0].ID); ok {
		t.Fatal("published vessel must have its aspect removed on cancellation")
	}
}

// A compensation that fails once stays compensating and the next tick finishes
// the rollback.
func TestCompensationFailureRetried(t *testing.T) {
	registry, store, _, plan, cycle, faults := newHarness(t)
	ctx := context.Background()
	if _, err := registry.Ensure(ctx, plan, cycle, model.SignalProceed); err != nil {
		t.Fatal(err)
	}
	faults.failOnce(plan.Vessels[0].ID) // fails first compensation attempt

	snapshot, _, err := registry.Cancel(ctx, plan.ID, "stop convoy")
	if err == nil {
		t.Fatal("expected compensation error to surface while it keeps running")
	}
	if snapshot.Status != model.PublicationCompensating {
		t.Fatalf("status = %s, want compensating", snapshot.Status)
	}
	if _, ok := store.Get(plan.Vessels[0].ID); !ok {
		t.Fatal("uncompensated vessel aspect must remain until rollback is journaled")
	}

	registry.Tick(ctx)
	final, ok := registry.Get(plan.ID)
	if !ok {
		t.Fatal("publication missing")
	}
	if final.Status != model.PublicationCompensated {
		t.Fatalf("after tick status = %s", final.Status)
	}
	for _, vessel := range plan.Vessels {
		if _, ok := store.Get(vessel.ID); ok {
			t.Fatalf("vessel %s aspect survived compensation", vessel.CallSign)
		}
	}
}

// The lockage/generation fencing is preserved: a request stamped for another
// transit generation cannot drive the same publication.
func TestGenerationMismatchRejected(t *testing.T) {
	registry, _, _, plan, cycle, _ := newHarness(t)
	ctx := context.Background()
	if _, err := registry.Ensure(ctx, plan, cycle, model.SignalProceed); err != nil {
		t.Fatal(err)
	}
	newGeneration := cycle
	newGeneration.Generation = 2
	if _, err := registry.Ensure(ctx, plan, newGeneration, model.SignalProceed); !errors.Is(err, ErrPublicationGeneration) {
		t.Fatalf("err = %v, want ErrPublicationGeneration", err)
	}
}
