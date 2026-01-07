package helper

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLenSyncMap(t *testing.T) {
	m := &sync.Map{}
	got := LenSyncMap(m)
	assert.Zero(t, got, "expected length of empty sync.Map to be 0")

	// Add some entries
	m.Store("key1", "value1")
	m.Store("key2", "value2")
	m.Store("key3", "value3")
	got = LenSyncMap(m)
	assert.Equal(t, 3, got, "expected length of sync.Map to be 3")

	// Delete one entry
	m.Delete("key2")
	got = LenSyncMap(m)
	assert.Equal(t, 2, got, "expected length of sync.Map to be 2")
}
