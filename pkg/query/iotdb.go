package query

import (
	"fmt"
	"sync"
)

// IoTDB encodes a IoTDB request. This will be serialized for use
// by the run_queries_tdengine program.
type IoTDB struct {
	HumanLabel       []byte
	HumanDescription []byte
	SqlQuery         []byte
	id               uint64
}

// IotDBPool is a sync.Pool of IoTDB Query types
var IotDBPool = sync.Pool{
	New: func() interface{} {
		return &IoTDB{
			HumanLabel:       make([]byte, 0, 1024),
			HumanDescription: make([]byte, 0, 1024),
			SqlQuery:         make([]byte, 0, 1024),
		}
	},
}

// NewIotDB returns a new IoTDB Query instance
func NewIotDB() *IoTDB {
	return IotDBPool.Get().(*IoTDB)
}

// GetID returns the ID of this Query
func (q *IoTDB) GetID() uint64 {
	return q.id
}

// SetID sets the ID for this Query
func (q *IoTDB) SetID(n uint64) {
	q.id = n
}

// String produces a debug-ready description of a Query.
func (q *IoTDB) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, Query: %s",
		q.HumanLabel, q.HumanDescription, q.SqlQuery)
}

// HumanLabelName returns the human-readable name of this Query
func (q *IoTDB) HumanLabelName() []byte {
	return q.HumanLabel
}

// HumanDescriptionName returns the human-readable description of this Query
func (q *IoTDB) HumanDescriptionName() []byte {
	return q.HumanDescription
}

// Release resets and returns this Query to its pool
func (q *IoTDB) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0
	q.SqlQuery = q.SqlQuery[:0]

	IotDBPool.Put(q)
}
