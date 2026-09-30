package signal

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Publisher applies and rolls back one vessel signal at a time so a partial
// convoy publication can be resumed or compensated vessel by vessel.
type Publisher struct {
	store    *Store
	recorder *journal.Recorder
	now      func() time.Time
	// fault hooks simulate failed field/journal writes. They are only used from
	// tests; production leaves them nil.
	writeFault      func(vesselID uuid.UUID, attempt int) error
	compensateFault func(vesselID uuid.UUID, attempt int) error
}

func NewPublisher(store *Store, recorder *journal.Recorder, now func() time.Time) *Publisher {
	if now == nil {
		now = time.Now
	}
	return &Publisher{store: store, recorder: recorder, now: now}
}

// SetWriteFault installs a fault hook for vessel writes (tests only).
func (p *Publisher) SetWriteFault(hook func(vesselID uuid.UUID, attempt int) error) {
	p.writeFault = hook
}

// SetCompensateFault installs a fault hook for vessel compensations (tests only).
func (p *Publisher) SetCompensateFault(hook func(vesselID uuid.UUID, attempt int) error) {
	p.compensateFault = hook
}

// WriteVessel publishes one vessel aspect and records the change. The store
// update only survives when its journal event is appended, so a recorder
// failure rolls this single vessel back and returns an error. The previous
// aspect (when one existed) is returned for later compensation.
func (p *Publisher) WriteVessel(ctx context.Context, chamberID string, vessel model.Vessel, cycleID uuid.UUID, generation uint64, aspect model.SignalAspect, attempt int) (model.SignalState, bool, error) {
	if p.writeFault != nil {
		if err := p.writeFault(vessel.ID, attempt); err != nil {
			return model.SignalState{}, false, err
		}
	}
	previous, hadPrevious := p.store.Get(vessel.ID)
	state := p.store.Set(vessel.ID, cycleID, generation, aspect, p.now())
	if _, err := p.recorder.Record(ctx, chamberID, model.EventSignalChanged, cycleID, generation, state); err != nil {
		// Journal is the durable boundary: undo the in-memory write so a retry
		// starts from the last durably committed aspect.
		if hadPrevious {
			p.store.Set(previous.VesselID, previous.CycleID, previous.Generation, previous.Aspect, p.now())
		} else {
			p.store.Remove(vessel.ID)
		}
		return model.SignalState{}, false, err
	}
	return previous, hadPrevious, nil
}

// CompensateVessel restores one vessel to its previous aspect (or removes it
// when it had none) and records the compensation marker.
func (p *Publisher) CompensateVessel(ctx context.Context, chamberID string, vesselID, cycleID uuid.UUID, generation uint64, previous *model.SignalState, attempt int) error {
	if p.compensateFault != nil {
		if err := p.compensateFault(vesselID, attempt); err != nil {
			return err
		}
	}
	if previous != nil {
		p.store.Set(previous.VesselID, previous.CycleID, previous.Generation, previous.Aspect, p.now())
	} else {
		p.store.Remove(vesselID)
	}
	_, err := p.recorder.Record(ctx, chamberID, model.EventSignalChanged, cycleID, generation, map[string]any{
		"vessel_id": vesselID, "compensated": true,
	})
	return err
}
