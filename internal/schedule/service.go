package schedule

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

// Service exposes planning and signal-publication workflows to the API.
type Service struct {
	planner      *Planner
	publications *signal.Publications
}

func NewService(planner *Planner, publications *signal.Publications) *Service {
	return &Service{planner: planner, publications: publications}
}

func (s *Service) Create(ctx context.Context, chamberID string, direction model.Direction, vessels []model.Vessel) (model.ConvoyPlan, error) {
	return s.planner.Create(ctx, chamberID, direction, vessels)
}

// Publish returns the one publication result for a plan. The first request
// starts the per-vessel publication; every later (or concurrent) request for
// the same plan receives that same result, with unfinished vessels retried.
func (s *Service) Publish(ctx context.Context, planID uuid.UUID, cycle model.TransitCycle) (model.PlanPublication, error) {
	plan, ok := s.planner.Get(planID)
	if !ok {
		return model.PlanPublication{}, errors.New("convoy plan not found")
	}
	if plan.Cancelled {
		// A cancelled plan must never publish again, even if its publication
		// job had not been created yet.
		return model.PlanPublication{}, errors.New("cancelled convoy plan cannot be published")
	}
	if plan.ChamberID != cycle.ChamberID || plan.ID != cycle.PlanID {
		return model.PlanPublication{}, errors.New("plan and transit do not match")
	}
	return s.publications.Ensure(ctx, plan, cycle, model.SignalProceed)
}

func (s *Service) Cancel(ctx context.Context, planID uuid.UUID, note string) (Cancellation, error) {
	return s.planner.Cancel(ctx, planID, note, s.publications)
}

func (s *Service) Active(chamberID string) []model.ConvoyPlan {
	return s.planner.Active(chamberID)
}

func (s *Service) All() []model.ConvoyPlan { return s.planner.All() }

func (s *Service) Publication(planID uuid.UUID) (model.PlanPublication, bool) {
	return s.publications.Get(planID)
}

func (s *Service) Publications() []model.PlanPublication { return s.publications.List() }

// RecoverPublications resumes vessel writes or compensations interrupted by a
// failed request or a process restart, driven by the background reaper.
func (s *Service) RecoverPublications(ctx context.Context) { s.publications.Tick(ctx) }
