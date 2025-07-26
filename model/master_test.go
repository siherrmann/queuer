package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMasterSettingsMarshalAndUnmarshalJSON(t *testing.T) {
	t.Run("Marshal and Unmarshal valid MasterSettings", func(t *testing.T) {
		settings := &MasterSettings{
			RetentionArchive: 30,
		}

		data, err := settings.Value()
		assert.NoError(t, err, "Failed to marshal MasterSettings")

		var unmarshalledSettings MasterSettings
		err = unmarshalledSettings.Scan(data)
		assert.NoError(t, err, "Failed to unmarshal MasterSettings")
		assert.Equal(t, settings.RetentionArchive, unmarshalledSettings.RetentionArchive, "RetentionArchive should match")
	})

	t.Run("Unmarshal MasterSettings already correct type", func(t *testing.T) {
		settings := &MasterSettings{
			RetentionArchive: 30,
		}

		var unmarshalledSettings MasterSettings
		err := unmarshalledSettings.Scan(*settings)
		assert.NoError(t, err, "Failed to unmarshal MasterSettings")
		assert.Equal(t, settings.RetentionArchive, unmarshalledSettings.RetentionArchive, "RetentionArchive should match")
	})

	t.Run("Unmarshal invalid MasterSettings json", func(t *testing.T) {
		var settings MasterSettings
		err := settings.Scan("invalid json")
		assert.Error(t, err, "Expected error while unmarshalling invalid MasterSettings json")
	})
}
