package iot

import (
	"github.com/cnosdb/tsdb-comparisons/cmd/generate_queries/uses/common"
	"github.com/cnosdb/tsdb-comparisons/cmd/generate_queries/utils"
	"github.com/cnosdb/tsdb-comparisons/pkg/query"
)

// LastLocSingleTruck contains info for filling in last location query for a single truck.
type LastLocSingleTruck struct {
	core utils.QueryGenerator
}

// NewLastLocSingleTruck creates a new last location query filler.
func NewLastLocSingleTruck(core utils.QueryGenerator) utils.QueryFiller {
	return &LastLocSingleTruck{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *LastLocSingleTruck) Fill(q query.Query) query.Query {
	fc, ok := i.core.(LastLocByTruckFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.LastLocByTruck(q, 1)
	return q
}
