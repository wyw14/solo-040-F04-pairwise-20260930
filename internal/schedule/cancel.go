package schedule

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

type Cancellation struct {
	Plan         model.ConvoyPlan       `json:"plan"`
	RemovedQueue int                    `json:"removed_queue"`
	Compensated  int                    `json:"compensated"`
	Publication  *model.PlanPublication `json:"publication,omitempty"`
}

// Cancel reverts queue and field-signal effects as one observable operation.
// The plan is marked cancelled first so it can never be published again; any
// vessel whose compensation cannot finish immediately is rolled back by the
// publication registry's background recovery.
func (p *Planner) Cancel(ctx context.Context, planID uuid.UUID, note string, publications *signal.Publications) (Cancellation, error) {
	plan, err := p.require(planID)
	if err != nil {
		return Cancellation{}, err
	}
	if plan.Cancelled {
		// Idempotent: resume an unfinished compensation instead of failing.
		snapshot, _, compErr := publications.Cancel(context.WithoutCancel(ctx), planID, note)
		if compErr != nil {
			return Cancellation{Plan: plan, Publication: snapshotPtr(snapshot)}, compErr
		}
		return Cancellation{
			Plan: plan, Publication: snapshotPtr(snapshot),
			Compensated: compensatedCount(snapshot),
		}, nil
	}

	plan.Cancelled = true
	plan.CancelNote = note
	p.replace(plan)

	removed := p.admission.RemovePlan(plan.ChamberID, plan)
	for index := range removed {
		removed[index].Released = false
	}
	p.admission.Restore(plan.ChamberID, removed)

	snapshot, found, compErr := publications.Cancel(context.WithoutCancel(ctx), planID, note)
	compensated := 0
	var publication *model.PlanPublication
	if found {
		compensated = compensatedCount(snapshot)
		publication = &snapshot
	}
	_, recordErr := p.recorder.Record(context.WithoutCancel(ctx), plan.ChamberID, model.EventPlanCancelled, plan.ID, 0, map[string]any{
		"note": note, "restored_queue": len(removed),
		"compensated_signals": compensated, "publication_found": found,
	})
	if compErr != nil {
		// Compensation is still running in the background; keep the plan
		// cancelled and surface the pending error to the operator.
		return Cancellation{Plan: plan, RemovedQueue: len(removed), Compensated: compensated, Publication: publication},
			errors.Join(errors.New("signal compensation pending"), compErr)
	}
	return Cancellation{
		Plan: plan, RemovedQueue: len(removed), Compensated: compensated, Publication: publication,
	}, recordErr
}

func snapshotPtr(snapshot model.PlanPublication) *model.PlanPublication {
	if snapshot.PlanID == uuid.Nil {
		return nil
	}
	copy := snapshot
	return &copy
}

func compensatedCount(snapshot model.PlanPublication) int {
	count := 0
	for _, vessel := range snapshot.Vessels {
		if vessel.Status == model.VesselCompensated {
			count++
		}
	}
	return count
}
