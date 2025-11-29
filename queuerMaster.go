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

	if masterSettings.JobDeleteThreshold == 0 {
		err := q.dbJob.RemoveRetentionArchive()
		if err != nil {
			return helper.NewError("removing retention archive", err)
		}
	} else if oldMaster.Settings.JobDeleteThreshold != masterSettings.JobDeleteThreshold {
		err := q.dbJob.RemoveRetentionArchive()
		if err != nil {
			q.log.Warn("Error removing retention archive (expected if none existed)", slog.String("error", err.Error()))
		}

		err = q.dbJob.AddRetentionArchive(masterSettings.JobDeleteThreshold)
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

			err = q.deleteStaleWorkers()
			if err != nil {
				q.log.Error("Error deleting stale workers", slog.String("error", err.Error()))
			}

			err = q.checkStaleJobs()
			if err != nil {
				q.log.Error("Error checking for stale jobs", slog.String("error", err.Error()))
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
	staleCount, err := q.dbWorker.UpdateStaleWorkers(staleThreshold)
	if err != nil {
		return helper.NewError("updating stale workers", err)
	}

	if staleCount > 0 {
		q.log.Info("Updated stale workers", slog.Int("count", staleCount))
	}

	return nil
}

func (q *Queuer) deleteStaleWorkers() error {
	deleteThreshold := 10 * time.Minute
	deletedCount, err := q.dbWorker.DeleteStaleWorkers(deleteThreshold)
	if err != nil {
		return helper.NewError("deleting stale workers", err)
	}

	if deletedCount > 0 {
		q.log.Info("Deleted stale workers", slog.Int("count", deletedCount))
	}

	return nil
}

func (q *Queuer) checkStaleJobs() error {
	staleCount, err := q.dbJob.UpdateStaleJobs()
	if err != nil {
		return helper.NewError("updating stale jobs", err)
	}

	if staleCount > 0 {
		q.log.Info("Updated stale jobs", slog.Int("count", staleCount))
	}

	return nil
}
