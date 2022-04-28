package iot

import (
	"github.com/cnosdb/tsdb-comparisons/cmd/generate_queries/uses/common"
	"github.com/cnosdb/tsdb-comparisons/cmd/generate_queries/utils"
	"github.com/cnosdb/tsdb-comparisons/pkg/query"
)

// TrucksWithLowFuel contains info for filling in trucks with low fuel queries.
type TrucksWithLowFuel struct {
	core utils.QueryGenerator
}

// NewTruckWithLowFuel creates a new trucks with low fuel query filler.
func NewTruckWithLowFuel(core utils.QueryGenerator) utils.QueryFiller {
	return &TrucksWithLowFuel{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *TrucksWithLowFuel) Fill(q query.Query) query.Query {
	fc, ok := i.core.(TruckLowFuelFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.TrucksWithLowFuel(q)
	return q
}
