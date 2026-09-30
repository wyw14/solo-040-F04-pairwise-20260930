package schedule

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

func newServiceHarness(t *testing.T) (*Service, *signal.Store, model.ConvoyPlan, model.TransitCycle) {
	t.Helper()
	now := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	journalStore := journal.NewMemoryStore()
	recorder := journal.NewRecorder(journalStore, clock)
	gateState := gate.NewState("chamber-1")
	signalStore := signal.NewStore()
	queue := admission.NewQueue()
	controller := admission.NewController(queue, gateState, signalStore, clock)
	admissionService := admission.NewService(controller, recorder, clock)
	planner := NewPlanner(admissionService, recorder, clock)
	publisher := signal.NewPublisher(signalStore, recorder, clock)
	publications := signal.NewPublications(publisher, clock)
	service := NewService(planner, publications)

	ctx := context.Background()
	plan, err := service.Create(ctx, "chamber-1", model.DirectionUpstream, []model.Vessel{
		{Name: "River Star", CallSign: "RS-1", LengthM: 80},
		{Name: "Canal Dawn", CallSign: "CD-2", LengthM: 90},
	})
	if err != nil {
		t.Fatalf("create plan: %v", err)
	}
	cycle, err := model.NewTransitCycle(plan.ChamberID, plan.ID, plan.Direction, 1, now)
	if err != nil {
		t.Fatalf("cycle: %v", err)
	}
	return service, signalStore, plan, cycle
}

func TestServicePublishIsIdempotent(t *testing.T) {
	service, signalStore, plan, cycle := newServiceHarness(t)
	ctx := context.Background()

	first, err := service.Publish(ctx, plan.ID, cycle)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if first.Status != model.PublicationCompleted || first.Published != len(plan.Vessels) {
		t.Fatalf("first = %+v", first)
	}
	second, err := service.Publish(ctx, plan.ID, cycle)
	if err != nil {
		t.Fatalf("duplicate publish: %v", err)
	}
	if second.PlanID != first.PlanID || second.Status != model.PublicationCompleted {
		t.Fatalf("duplicate returned a different result: %+v", second)
	}
	if len(service.Publications()) != 1 {
		t.Fatalf("publications registry should hold exactly one job, got %d", len(service.Publications()))
	}
	for _, vessel := range plan.Vessels {
		state, ok := signalStore.Get(vessel.ID)
		if !ok || state.Aspect != model.SignalProceed {
			t.Fatalf("vessel %s not published", vessel.CallSign)
		}
	}
}

func TestServiceCancelCompensatesAndBlocksRepublish(t *testing.T) {
	service, signalStore, plan, cycle := newServiceHarness(t)
	ctx := context.Background()

	if _, err := service.Publish(ctx, plan.ID, cycle); err != nil {
		t.Fatal(err)
	}
	cancellation, err := service.Cancel(ctx, plan.ID, "operator cancellation")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if !cancellation.Plan.Cancelled || cancellation.Compensated != len(plan.Vessels) {
		t.Fatalf("cancellation = %+v", cancellation)
	}
	for _, vessel := range plan.Vessels {
		if _, ok := signalStore.Get(vessel.ID); ok {
			t.Fatalf("vessel %s signal survived cancellation", vessel.CallSign)
		}
	}

	if _, err := service.Publish(ctx, plan.ID, cycle); err == nil ||
		!strings.Contains(err.Error(), "cancelled") {
		t.Fatalf("publish after cancel err = %v, want cancellation rejection", err)
	}
}

// Cancelling before any publish request exists must still prevent a later
// publication from ever starting.
func TestServiceCancelBeforePublish(t *testing.T) {
	service, _, plan, cycle := newServiceHarness(t)
	ctx := context.Background()

	if _, err := service.Cancel(ctx, plan.ID, "abort before publish"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if _, err := service.Publish(ctx, plan.ID, cycle); err == nil ||
		!strings.Contains(err.Error(), "cancelled") {
		t.Fatalf("publish after prior cancel err = %v", err)
	}
	if publications := service.Publications(); len(publications) != 0 {
		t.Fatalf("no publication job should exist, got %d", len(publications))
	}
}

// Generation fencing: a cycle stamped with a newer generation cannot drive a
// publication bound to the original transit generation.
func TestServiceGenerationFencing(t *testing.T) {
	service, _, plan, cycle := newServiceHarness(t)
	ctx := context.Background()

	if _, err := service.Publish(ctx, plan.ID, cycle); err != nil {
		t.Fatal(err)
	}
	newGeneration := cycle
	newGeneration.Generation = cycle.Generation + 1
	if _, err := service.Publish(ctx, plan.ID, newGeneration); err == nil ||
		!strings.Contains(err.Error(), "generation") {
		t.Fatalf("generation mismatch err = %v", err)
	}
}
