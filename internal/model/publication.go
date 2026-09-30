package model

import (
	"time"

	"github.com/google/uuid"
)

// PublicationStatus is the overall, externally visible state of one plan's
// convoy signal publication.
type PublicationStatus string

const (
	// PublicationPending means no vessel write has started yet.
	PublicationPending PublicationStatus = "pending"
	// PublicationPublishing means at least one vessel still has to be written.
	PublicationPublishing PublicationStatus = "publishing"
	// PublicationCompleted means every vessel signal was written and journaled.
	PublicationCompleted PublicationStatus = "completed"
	// PublicationPartial means the last attempt failed for at least one vessel;
	// the background reaper will keep retrying the remaining vessels.
	PublicationPartial PublicationStatus = "partial"
	// PublicationCompensating means the publication was cancelled and already
	// written signals are being rolled back.
	PublicationCompensating PublicationStatus = "compensating"
	// PublicationCompensated means every written signal was rolled back.
	PublicationCompensated PublicationStatus = "compensated"
)

// Terminal reports whether the publication reached its final outcome. A
// compensation that could not finish stays PublicationCompensating and keeps
// being retried, so it is deliberately not terminal.
func (s PublicationStatus) Terminal() bool {
	return s == PublicationCompleted || s == PublicationCompensated
}

// VesselPublishStatus is the per-vessel write state inside a publication.
type VesselPublishStatus string

const (
	VesselPending      VesselPublishStatus = "pending"
	VesselPublishing   VesselPublishStatus = "publishing"
	VesselPublished    VesselPublishStatus = "published"
	VesselFailed       VesselPublishStatus = "failed"
	VesselCompensating VesselPublishStatus = "compensating"
	VesselCompensated  VesselPublishStatus = "compensated"
)

// VesselPublishProgress is the operation page's view of a single vessel write.
type VesselPublishProgress struct {
	VesselID   string              `json:"vessel_id"`
	VesselName string              `json:"vessel_name"`
	CallSign   string              `json:"call_sign"`
	Status     VesselPublishStatus `json:"status"`
	Attempts   int                 `json:"attempts"`
	Error      string              `json:"error,omitempty"`
	UpdatedAt  time.Time           `json:"updated_at"`
}

func (p VesselPublishProgress) Clone() VesselPublishProgress { return p }

// PlanPublication is the idempotent, externally visible result of publishing
// the signals for one convoy plan. Repeated requests for the same plan return
// the same snapshot while unfinished vessels are recovered in the background.
type PlanPublication struct {
	PlanID     uuid.UUID               `json:"plan_id"`
	ChamberID  string                  `json:"chamber_id"`
	CycleID    uuid.UUID               `json:"cycle_id"`
	Generation uint64                  `json:"generation"`
	Aspect     SignalAspect            `json:"aspect"`
	Status     PublicationStatus       `json:"status"`
	Total      int                     `json:"total"`
	Published  int                     `json:"published"`
	Vessels    []VesselPublishProgress `json:"vessels"`
	CancelNote string                  `json:"cancel_note,omitempty"`
	LastError  string                  `json:"last_error,omitempty"`
	CreatedAt  time.Time               `json:"created_at"`
	UpdatedAt  time.Time               `json:"updated_at"`
}
