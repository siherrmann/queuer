package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMasterSettingsMarshalAndUnmarshalJSON(t *testing.T) {
	t.Run("Marshal and Unmarshal valid MasterSettings", func(t *testing.T) {
		settings := &MasterSettings{
			JobDeleteThreshold: 30,
		}

		data, err := settings.Value()
		assert.NoError(t, err, "Failed to marshal MasterSettings")

		var unmarshalledSettings MasterSettings
		err = unmarshalledSettings.Scan(data)
		assert.NoError(t, err, "Failed to unmarshal MasterSettings")
		assert.Equal(t, settings.JobDeleteThreshold, unmarshalledSettings.JobDeleteThreshold, "RetentionArchive should match")
	})

	t.Run("Unmarshal MasterSettings already correct type", func(t *testing.T) {
		settings := &MasterSettings{
			JobDeleteThreshold: 30,
		}

		var unmarshalledSettings MasterSettings
		err := unmarshalledSettings.Scan(*settings)
		assert.NoError(t, err, "Failed to unmarshal MasterSettings")
		assert.Equal(t, settings.JobDeleteThreshold, unmarshalledSettings.JobDeleteThreshold, "RetentionArchive should match")
	})

	t.Run("Unmarshal invalid MasterSettings json", func(t *testing.T) {
		var settings MasterSettings
		err := settings.Scan("invalid json")
		assert.Error(t, err, "Expected error while unmarshalling invalid MasterSettings json")
	})
}

func TestMasterSettingsSetDefault(t *testing.T) {
	t.Run("SetDefault sets all zero values", func(t *testing.T) {
		settings := &MasterSettings{}
		settings.SetDefault()

		assert.Equal(t, 5*time.Minute, settings.MasterLockTimeout, "MasterLockTimeout should be set to default")
		assert.Equal(t, 1*time.Minute, settings.MasterPollInterval, "MasterPollInterval should be set to default")
		assert.Equal(t, 7*24*time.Hour, settings.JobDeleteThreshold, "RetentionArchive should be set to default")
		assert.Equal(t, 5*time.Minute, settings.WorkerStaleThreshold, "WorkerStaleThreshold should be set to default")
		assert.Equal(t, 24*time.Hour, settings.WorkerDeleteThreshold, "WorkerDeleteThreshold should be set to default")
		assert.Equal(t, 1*time.Hour, settings.JobStaleThreshold, "JobStaleThreshold should be set to default")
	})

	t.Run("SetDefault does not override existing values", func(t *testing.T) {
		settings := &MasterSettings{
			MasterLockTimeout:     10 * time.Minute,
			MasterPollInterval:    2 * time.Minute,
			JobDeleteThreshold:    14 * 24 * time.Hour,
			WorkerStaleThreshold:  10 * time.Minute,
			WorkerDeleteThreshold: 48 * time.Hour,
			JobStaleThreshold:     2 * time.Hour,
		}
		settings.SetDefault()

		assert.Equal(t, 10*time.Minute, settings.MasterLockTimeout, "MasterLockTimeout should not be overridden")
		assert.Equal(t, 2*time.Minute, settings.MasterPollInterval, "MasterPollInterval should not be overridden")
		assert.Equal(t, 14*24*time.Hour, settings.JobDeleteThreshold, "RetentionArchive should not be overridden")
		assert.Equal(t, 10*time.Minute, settings.WorkerStaleThreshold, "WorkerStaleThreshold should not be overridden")
		assert.Equal(t, 48*time.Hour, settings.WorkerDeleteThreshold, "WorkerDeleteThreshold should not be overridden")
		assert.Equal(t, 2*time.Hour, settings.JobStaleThreshold, "JobStaleThreshold should not be overridden")
	})

	t.Run("SetDefault sets defaults for partially populated settings", func(t *testing.T) {
		settings := &MasterSettings{
			MasterLockTimeout:  10 * time.Minute,
			JobDeleteThreshold: 14 * 24 * time.Hour,
		}
		settings.SetDefault()

		assert.Equal(t, 10*time.Minute, settings.MasterLockTimeout, "MasterLockTimeout should not be overridden")
		assert.Equal(t, 1*time.Minute, settings.MasterPollInterval, "MasterPollInterval should be set to default")
		assert.Equal(t, 14*24*time.Hour, settings.JobDeleteThreshold, "RetentionArchive should not be overridden")
		assert.Equal(t, 5*time.Minute, settings.WorkerStaleThreshold, "WorkerStaleThreshold should be set to default")
		assert.Equal(t, 24*time.Hour, settings.WorkerDeleteThreshold, "WorkerDeleteThreshold should be set to default")
		assert.Equal(t, 1*time.Hour, settings.JobStaleThreshold, "JobStaleThreshold should be set to default")
	})
}
