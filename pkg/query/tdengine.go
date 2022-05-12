package query

import (
	"fmt"
	"sync"
)

// TDengine encodes a TDengine request. This will be serialized for use
// by the run_queries_tdengine program.
type TDengine struct {
	HumanLabel       []byte
	HumanDescription []byte
	SqlQuery         []byte
	id               uint64
}

// TDenginePool is a sync.Pool of TDengine Query types
var TDenginePool = sync.Pool{
	New: func() interface{} {
		return &TDengine{
			HumanLabel:       make([]byte, 0, 1024),
			HumanDescription: make([]byte, 0, 1024),
			SqlQuery:         make([]byte, 0, 1024),
		}
	},
}

// NewTDengine returns a new TDengine Query instance
func NewTDengine() *TDengine {
	return TDenginePool.Get().(*TDengine)
}

// GetID returns the ID of this Query
func (q *TDengine) GetID() uint64 {
	return q.id
}

// SetID sets the ID for this Query
func (q *TDengine) SetID(n uint64) {
	q.id = n
}

// String produces a debug-ready description of a Query.
func (q *TDengine) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, Query: %s",
		q.HumanLabel, q.HumanDescription, q.SqlQuery)
}

// HumanLabelName returns the human-readable name of this Query
func (q *TDengine) HumanLabelName() []byte {
	return q.HumanLabel
}

// HumanDescriptionName returns the human-readable description of this Query
func (q *TDengine) HumanDescriptionName() []byte {
	return q.HumanDescription
}

// Release resets and returns this Query to its pool
func (q *TDengine) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0
	q.SqlQuery = q.SqlQuery[:0]

	TDenginePool.Put(q)
}
