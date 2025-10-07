package queuer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/siherrmann/queuer/core"
	"github.com/siherrmann/queuer/helper"
	"github.com/siherrmann/queuer/model"
)

func (q *Queuer) masterTicker(ctx context.Context, oldMaster *model.Master, masterSettings *model.MasterSettings) error {
	if oldMaster == nil {
		return helper.NewError("old master check", fmt.Errorf("old master is nil"))
	}

	if oldMaster.Settings.RetentionArchive == 0 {
		err := q.dbJob.AddRetentionArchive(masterSettings.RetentionArchive)
		if err != nil {
			return helper.NewError("adding retention archive", err)
		}
	} else if oldMaster.Settings.RetentionArchive != masterSettings.RetentionArchive {
		err := q.dbJob.RemoveRetentionArchive()
		if err != nil {
			return helper.NewError("removing retention archive", err)
		}

		err = q.dbJob.AddRetentionArchive(masterSettings.RetentionArchive)
		if err != nil {
			return helper.NewError("adding retention archive", err)
		}
	}

	ctxInner, cancel := context.WithCancel(ctx)
	ticker, err := core.NewTicker(
		masterSettings.MasterPollInterval,
		func() {
			q.workerMu.RLock()
			worker := q.worker
			q.workerMu.RUnlock()

			_, err := q.dbMaster.UpdateMaster(worker, masterSettings)
			if err != nil {
				err := q.pollMasterTicker(ctx, masterSettings)
				if err != nil {
					q.log.Error("Error restarting poll master ticker", slog.String("error", err.Error()))
				}
				cancel()
				return
			}

			err = q.checkStaleWorkers()
			if err != nil {
				q.log.Error("Error checking for stale workers", slog.String("error", err.Error()))
			}

			// Here we can add any additional logic that needs to run periodically while the worker is master.
			// This could include stale jobs, cleaning up the job database etc.
		},
	)
	if err != nil {
		return helper.NewError("creating ticker", err)
	}

	q.log.Info("Starting master poll ticker...")
	go ticker.Go(ctxInner)

	return nil
}

func (q *Queuer) checkStaleWorkers() error {
	staleThreshold := 2 * time.Minute
	cutoffTime := time.Now().UTC().Add(-staleThreshold)

	workers, err := q.dbWorker.SelectAllWorkers(0, 100)
	if err != nil {
		return helper.NewError("fetching workers for stale check", err)
	}

	staleCount := 0
	for _, worker := range workers {
		if (worker.Status == model.WorkerStatusReady || worker.Status == model.WorkerStatusRunning) && worker.UpdatedAt.Before(cutoffTime) {
			q.log.Warn("Found stale worker, marking as stopped", slog.String("worker_rid", worker.RID.String()), slog.String("last_update", worker.UpdatedAt.Format(time.RFC3339)))

			// Update worker status to stopped
			worker.Status = model.WorkerStatusStopped
			err := q.dbWorker.UpdateStaleWorker(worker.RID)
			if err != nil {
				q.log.Error("Error updating stale worker status", slog.String("worker_rid", worker.RID.String()), slog.String("error", err.Error()))
			} else {
				staleCount++
			}
		}
	}

	if staleCount > 0 {
		q.log.Info("Updated stale workers", slog.Int("count", staleCount))
	}

	return nil
}
