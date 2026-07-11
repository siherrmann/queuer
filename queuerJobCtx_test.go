package queuer

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/siherrmann/queuer/core"
	"github.com/siherrmann/queuer/model"
	"github.com/stretchr/testify/assert"
)

func TestOptionsWithContext(t *testing.T) {
	q := &Queuer{}

	t.Run("Nil context does not modify options", func(t *testing.T) {
		options := &model.Options{}
		var ctx context.Context
		res := q.OptionsWithContext(ctx, options)
		assert.Equal(t, options, res)
	})

	t.Run("Context without JobRIDContextKey does not modify options", func(t *testing.T) {
		options := &model.Options{}
		ctx := context.Background()
		res := q.OptionsWithContext(ctx, options)
		assert.Equal(t, options, res)
		assert.Nil(t, res.ParentRID)
	})

	t.Run("Context with JobRIDContextKey modifies options", func(t *testing.T) {
		jobRID := uuid.New()
		ctx := context.WithValue(context.Background(), core.JobRIDContextKey, jobRID)

		res := q.OptionsWithContext(ctx, nil)
		assert.NotNil(t, res)
		assert.NotNil(t, res.ParentRID)
		assert.Equal(t, jobRID, *res.ParentRID)
	})
}

func TestQueuerContextWrapper(t *testing.T) {
	q := &Queuer{}

	t.Run("WithContext returns wrapper", func(t *testing.T) {
		ctx := context.Background()
		wrapper := q.WithContext(ctx)
		assert.NotNil(t, wrapper)
		assert.Equal(t, q, wrapper.q)
		assert.Equal(t, ctx, wrapper.ctx)
	})

	t.Run("OptionsWithContext preserves existing options", func(t *testing.T) {
		jobRID := uuid.New()
		ctx := context.WithValue(context.Background(), core.JobRIDContextKey, jobRID)

		existingOptions := &model.Options{
			Metadata: map[string]interface{}{"key": "value"},
		}
		res := q.OptionsWithContext(ctx, existingOptions)
		assert.NotNil(t, res)
		assert.Equal(t, "value", res.Metadata["key"])
		assert.NotNil(t, res.ParentRID)
		assert.Equal(t, jobRID, *res.ParentRID)
	})
}
