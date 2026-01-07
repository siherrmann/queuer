package helper

import "sync"

// LenSyncMap returns the number of entries in a sync.Map.
func LenSyncMap(m *sync.Map) int {
	var i int
	m.Range(func(k, v interface{}) bool {
		i++
		return true
	})
	return i
}
