package api

import (
	"context"
	"log/slog"
	"time"

	"github.com/wyw14/cry-96/internal/schedule"
)

// publicationReaperInterval bounds how long a partially written publication
// waits before unfinished vessels (forward) or compensation (backward) is
// retried after a failed HTTP attempt.
const publicationReaperInterval = 2 * time.Second

// startPublicationReaper performs an immediate recovery pass and then keeps
// retrying unfinished publications until they reach a terminal outcome.
func startPublicationReaper(ctx context.Context, service *schedule.Service) {
	service.RecoverPublications(ctx)
	go func() {
		ticker := time.NewTicker(publicationReaperInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				// Let compensations finish even though the request context ended.
				service.RecoverPublications(context.WithoutCancel(ctx))
				return
			case <-ticker.C:
				func() {
					defer func() {
						if recovered := recover(); recovered != nil {
							slog.Error("publication recovery tick panicked", "error", recovered)
						}
					}()
					service.RecoverPublications(ctx)
				}()
			}
		}
	}()
}
