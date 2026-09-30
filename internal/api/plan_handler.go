package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

type vesselInput struct {
	Name     string  `json:"name"`
	CallSign string  `json:"call_sign"`
	LengthM  float64 `json:"length_m"`
}

type planInput struct {
	ChamberID string          `json:"chamber_id"`
	Direction model.Direction `json:"direction"`
	Vessels   []vesselInput   `json:"vessels"`
}

func createPlanHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input planInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		vessels := make([]model.Vessel, 0, len(input.Vessels))
		for _, item := range input.Vessels {
			vessel, err := model.NewVessel(item.Name, item.CallSign, item.LengthM)
			if err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			vessels = append(vessels, vessel)
		}
		plan, err := runtime.Schedule.Create(r.Context(), input.ChamberID, input.Direction, vessels)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		cycleValue, err := runtime.CycleService.Start(r.Context(), plan)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{"plan": plan, "cycle": cycleValue})
	}
}

// plansHandler returns every plan (including cancelled ones) alongside their
// idempotent publication results, so the operations page can render progress.
func plansHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		active := make(map[string][]model.ConvoyPlan, len(runtime.Chambers))
		for _, chamberID := range runtime.Chambers {
			active[chamberID] = runtime.Schedule.Active(chamberID)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"active":       active,
			"plans":        runtime.Schedule.All(),
			"publications": runtime.Schedule.Publications(),
		})
	}
}

func publishSignalsHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		planID, err := uuid.Parse(chi.URLParam(r, "planID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		plan, ok := runtime.Planner.Get(planID)
		if !ok {
			writeError(w, http.StatusNotFound, errors.New("plan not found"))
			return
		}
		if plan.Cancelled {
			writeError(w, http.StatusConflict, errors.New("cancelled convoy plan cannot be published"))
			return
		}
		cycleValue, ok := runtime.Cycles.Current(plan.ChamberID)
		if !ok {
			writeError(w, http.StatusConflict, errors.New("active transit not found"))
			return
		}
		publication, err := runtime.Schedule.Publish(r.Context(), planID, cycleValue)
		if err != nil {
			switch {
			case errors.Is(err, signal.ErrPublicationGeneration):
				writeError(w, http.StatusConflict, err)
			case errors.Is(err, signal.ErrPublicationCancelled):
				writeError(w, http.StatusConflict, err)
			default:
				writeError(w, http.StatusConflict, err)
			}
			return
		}
		// Same result for repeated requests; 202 while vessels remain pending.
		status := http.StatusOK
		if publication.Status != model.PublicationCompleted {
			status = http.StatusAccepted
		}
		writeJSON(w, status, publication)
	}
}

func publicationHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		planID, err := uuid.Parse(chi.URLParam(r, "planID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		publication, ok := runtime.Schedule.Publication(planID)
		if !ok {
			writeError(w, http.StatusNotFound, errors.New("publication not found"))
			return
		}
		writeJSON(w, http.StatusOK, publication)
	}
}

func cancelPlanHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		planID, err := uuid.Parse(chi.URLParam(r, "planID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		plan, ok := runtime.Planner.Get(planID)
		if !ok {
			writeError(w, http.StatusNotFound, errors.New("plan not found"))
			return
		}
		result, err := runtime.Schedule.Cancel(r.Context(), planID, "operator cancellation")
		// The plan is already cancelled, so take its own transit cycle terminal
		// regardless of whether signal compensation finished synchronously.
		// Guard on plan+identity so a newer transit generation is never touched.
		if current, isCurrent := runtime.Cycles.Current(plan.ChamberID); isCurrent &&
			current.PlanID == plan.ID && !current.Stage.Terminal() {
			_, _ = runtime.Cycles.Cancel(r.Context(), plan.ChamberID, "operator cancellation")
		}
		if err != nil {
			// Compensation is still finishing in the background; the page
			// polls the publication until it reaches "compensated".
			writeJSON(w, http.StatusAccepted, map[string]any{
				"cancellation": result, "pending_error": err.Error(),
			})
			return
		}
		writeJSON(w, http.StatusOK, result)
	}
}
