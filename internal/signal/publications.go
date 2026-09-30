package signal

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// vesselJob tracks one vessel write inside a plan publication.
type vesselJob struct {
	vessel      model.Vessel
	status      model.VesselPublishStatus
	attempts    int
	compAttempt int // compensation attempts, counted separately from writes
	errMsg      string
	previous    *model.SignalState // aspect captured before this publication wrote it
	updatedAt   time.Time
}

// planJob is the idempotent publication result for one convoy plan. Repeated
// publish requests for the same plan always converge on the same job.
type planJob struct {
	planID     uuid.UUID
	chamberID  string
	cycleID    uuid.UUID
	generation uint64
	aspect     model.SignalAspect
	status     model.PublicationStatus
	cancelled  bool
	vessels    []vesselJob
	cancelNote string
	lastError  string
	createdAt  time.Time
	updatedAt  time.Time
}

// Publications is the registry of in-flight and finished plan publications.
// It deduplicates concurrent or retried publish requests, exposes per-vessel
// progress to the operations page, and lets a background reaper finish writes
// (or compensation) that failed on a previous attempt.
type Publications struct {
	driveMu sync.Mutex // serialises drivePublish/driveCompensate for a job
	mu      sync.Mutex
	jobs    map[uuid.UUID]*planJob
	order   []uuid.UUID
	pub     *Publisher
	now     func() time.Time
}

func NewPublications(publisher *Publisher, now func() time.Time) *Publications {
	if now == nil {
		now = time.Now
	}
	return &Publications{
		jobs: make(map[uuid.UUID]*planJob), pub: publisher,
		now: func() time.Time { return now().UTC() },
	}
}

var (
	ErrPublicationNotFound   = errors.New("plan publication not found")
	ErrPublicationCancelled  = errors.New("plan publication was cancelled and cannot be republished")
	ErrPublicationGeneration = errors.New("publish request belongs to another transit generation")
)

// Ensure returns the publication job for a plan, starting it on first request.
// Every later request for the same plan receives the same result; unfinished
// vessels are retried instead of republishing completed ones.
func (r *Publications) Ensure(ctx context.Context, plan model.ConvoyPlan, cycle model.TransitCycle, aspect model.SignalAspect) (model.PlanPublication, error) {
	if plan.ChamberID != cycle.ChamberID || plan.ID != cycle.PlanID {
		return model.PlanPublication{}, errors.New("plan and transit do not match")
	}
	r.mu.Lock()
	job, exists := r.jobs[plan.ID]
	if !exists {
		job = &planJob{
			planID: plan.ID, chamberID: plan.ChamberID, cycleID: cycle.ID,
			generation: cycle.Generation, aspect: aspect, status: model.PublicationPending,
			createdAt: r.now(), updatedAt: r.now(),
		}
		for _, vessel := range plan.Vessels {
			job.vessels = append(job.vessels, vesselJob{
				vessel: vessel, status: model.VesselPending, updatedAt: r.now(),
			})
		}
		r.jobs[plan.ID] = job
		r.order = append(r.order, plan.ID)
		r.mu.Unlock()
	} else {
		if job.cycleID != cycle.ID || job.generation != cycle.Generation {
			r.mu.Unlock()
			return model.PlanPublication{}, ErrPublicationGeneration
		}
		if job.cancelled {
			r.mu.Unlock()
			return model.PlanPublication{}, ErrPublicationCancelled
		}
		needsDrive := job.status != model.PublicationCompleted
		r.mu.Unlock()
		if needsDrive {
			r.drivePublish(ctx, job)
		}
		snapshot, _ := r.Get(plan.ID)
		return snapshot, nil
	}
	r.drivePublish(ctx, job)
	snapshot, _ := r.Get(plan.ID)
	return snapshot, nil
}

// Get returns a detached snapshot of one publication.
func (r *Publications) Get(planID uuid.UUID) (model.PlanPublication, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	job, ok := r.jobs[planID]
	if !ok {
		return model.PlanPublication{}, false
	}
	return r.snapshotLocked(job), true
}

// List returns every publication snapshot in creation order.
func (r *Publications) List() []model.PlanPublication {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]model.PlanPublication, 0, len(r.order))
	for _, id := range r.order {
		if job, ok := r.jobs[id]; ok {
			result = append(result, r.snapshotLocked(job))
		}
	}
	return result
}

// Cancel marks a publication for compensation and rolls back every vessel that
// was published. Vessels that never durably wrote need no field action. It is
// idempotent: cancelling again resumes an unfinished compensation and returns
// the same result.
func (r *Publications) Cancel(ctx context.Context, planID uuid.UUID, note string) (model.PlanPublication, bool, error) {
	r.mu.Lock()
	job, ok := r.jobs[planID]
	if !ok {
		r.mu.Unlock()
		return model.PlanPublication{}, false, nil
	}
	if !job.cancelled {
		job.cancelled = true
		job.cancelNote = note
		job.lastError = ""
		// A vessel still queued or in a failed forward state never durably
		// changed the field, so there is nothing to roll back for it.
		allIdle := true
		for index := range job.vessels {
			entry := &job.vessels[index]
			if entry.status == model.VesselPending || entry.status == model.VesselFailed {
				entry.status = model.VesselCompensated
				entry.errMsg = ""
				entry.updatedAt = r.now()
			}
			if entry.status != model.VesselCompensated {
				allIdle = false
			}
		}
		if allIdle {
			job.status = model.PublicationCompensated
		} else {
			job.status = model.PublicationCompensating
		}
		job.updatedAt = r.now()
	}
	r.mu.Unlock()
	err := r.driveCompensate(ctx, job)
	snapshot, _ := r.Get(planID)
	return snapshot, true, err
}

// Tick resumes any publication left unfinished by a failed HTTP attempt, both
// forward (remaining vessel writes) and backward (unfinished compensation).
func (r *Publications) Tick(ctx context.Context) {
	r.mu.Lock()
	jobs := make([]*planJob, 0, len(r.order))
	for _, id := range r.order {
		if job, ok := r.jobs[id]; ok {
			jobs = append(jobs, job)
		}
	}
	r.mu.Unlock()
	for _, job := range jobs {
		r.mu.Lock()
		cancelled, status := job.cancelled, job.status
		r.mu.Unlock()
		switch {
		case cancelled && status == model.PublicationCompensating:
			_ = r.driveCompensate(ctx, job)
		case !cancelled && status != model.PublicationCompleted:
			r.drivePublish(ctx, job)
		}
	}
}

func (r *Publications) drivePublish(ctx context.Context, job *planJob) {
	r.driveMu.Lock()
	defer r.driveMu.Unlock()
	for index := range job.vessels {
		entry := &job.vessels[index]
		r.mu.Lock()
		if job.cancelled || job.status == model.PublicationCompleted {
			r.mu.Unlock()
			return // cancellation superseded this forward run
		}
		if entry.status != model.VesselPending && entry.status != model.VesselFailed {
			r.mu.Unlock()
			continue
		}
		entry.status = model.VesselPublishing
		entry.attempts++
		entry.updatedAt = r.now()
		job.status = model.PublicationPublishing
		job.updatedAt = entry.updatedAt
		r.mu.Unlock()

		previous, hadPrevious, err := r.pub.WriteVessel(ctx, job.chamberID, entry.vessel, job.cycleID, job.generation, job.aspect, entry.attempts)

		r.mu.Lock()
		entry.updatedAt = r.now()
		if job.cancelled {
			// Cancel raced with this write. WriteVessel already undid the store
			// change on journal failure; on success it is a real published
			// aspect the compensator must now roll back. Normalise every vessel
			// that never durably wrote so compensation can reach its terminal
			// state once published vessels are rolled back.
			if err == nil {
				entry.status = model.VesselPublished
				if hadPrevious {
					entry.previous = &previous
				} else {
					entry.previous = nil
				}
			} else {
				entry.status = model.VesselCompensated
				entry.errMsg = ""
			}
			r.normaliseCancelledLocked(job)
			r.mu.Unlock()
			return
		}
		if err != nil {
			entry.status = model.VesselFailed
			entry.errMsg = err.Error()
			job.lastError = err.Error()
			r.recomputePublishLocked(job)
			r.mu.Unlock()
			if ctx.Err() != nil {
				return // request gave up; the reaper resumes from this vessel
			}
			continue // report this vessel's failure and keep writing the convoy
		}
		entry.status = model.VesselPublished
		entry.errMsg = ""
		if hadPrevious {
			entry.previous = &previous
		} else {
			entry.previous = nil
		}
		r.recomputePublishLocked(job)
		r.mu.Unlock()
	}
}

// normaliseCancelledLocked maps vessels that never durably wrote (pending,
// publishing or failed) straight to compensated, since there is no field
// aspect for them to roll back, then derives the overall status.
func (r *Publications) normaliseCancelledLocked(job *planJob) {
	for index := range job.vessels {
		entry := &job.vessels[index]
		if entry.status == model.VesselPending || entry.status == model.VesselPublishing || entry.status == model.VesselFailed {
			entry.status = model.VesselCompensated
			entry.errMsg = ""
			entry.updatedAt = r.now()
		}
	}
	r.recomputeCompensateLocked(job)
}

func (r *Publications) driveCompensate(ctx context.Context, job *planJob) error {
	r.driveMu.Lock()
	defer r.driveMu.Unlock()
	for index := len(job.vessels) - 1; index >= 0; index-- {
		entry := &job.vessels[index]
		r.mu.Lock()
		if !job.cancelled {
			r.mu.Unlock()
			return nil
		}
		if entry.status != model.VesselPublished && entry.status != model.VesselCompensating {
			r.mu.Unlock()
			continue
		}
		entry.status = model.VesselCompensating
		entry.compAttempt++
		entry.updatedAt = r.now()
		job.status = model.PublicationCompensating
		job.updatedAt = entry.updatedAt
		r.mu.Unlock()

		err := r.pub.CompensateVessel(ctx, job.chamberID, entry.vessel.ID, job.cycleID, job.generation, entry.previous, entry.compAttempt)

		r.mu.Lock()
		entry.updatedAt = r.now()
		if err != nil {
			entry.errMsg = err.Error()
			entry.status = model.VesselPublished // still needs rollback
			job.lastError = err.Error()
			job.status = model.PublicationCompensating
			r.mu.Unlock()
			return err
		}
		entry.errMsg = ""
		entry.status = model.VesselCompensated
		r.recomputeCompensateLocked(job)
		r.mu.Unlock()
	}
	return nil
}

// recomputePublishLocked derives the overall status from vessel states after a
// forward drive. It never runs for cancelled jobs.
func (r *Publications) recomputePublishLocked(job *planJob) {
	published, attempted := 0, 0
	for _, entry := range job.vessels {
		if entry.status == model.VesselPublished {
			published++
		}
		if entry.status != model.VesselPending {
			attempted++
		}
	}
	switch {
	case published == len(job.vessels):
		job.status = model.PublicationCompleted
		job.lastError = ""
	case attempted == 0:
		job.status = model.PublicationPending
	default:
		job.status = model.PublicationPartial
	}
	job.updatedAt = r.now()
}

func (r *Publications) recomputeCompensateLocked(job *planJob) {
	for _, entry := range job.vessels {
		if entry.status != model.VesselCompensated {
			job.status = model.PublicationCompensating
			job.updatedAt = r.now()
			return
		}
	}
	job.status = model.PublicationCompensated
	job.lastError = ""
	job.updatedAt = r.now()
}

func (r *Publications) snapshotLocked(job *planJob) model.PlanPublication {
	snapshot := model.PlanPublication{
		PlanID: job.planID, ChamberID: job.chamberID, CycleID: job.cycleID,
		Generation: job.generation, Aspect: job.aspect, Status: job.status,
		Vessels: make([]model.VesselPublishProgress, 0, len(job.vessels)),
		Total:   len(job.vessels), CancelNote: job.cancelNote, LastError: job.lastError,
		CreatedAt: job.createdAt, UpdatedAt: job.updatedAt,
	}
	for _, entry := range job.vessels {
		if entry.status == model.VesselPublished {
			snapshot.Published++
		}
		snapshot.Vessels = append(snapshot.Vessels, model.VesselPublishProgress{
			VesselID: entry.vessel.ID.String(), VesselName: entry.vessel.Name,
			CallSign: entry.vessel.CallSign, Status: entry.status,
			Attempts: entry.attempts, Error: entry.errMsg, UpdatedAt: entry.updatedAt,
		})
	}
	return snapshot
}
