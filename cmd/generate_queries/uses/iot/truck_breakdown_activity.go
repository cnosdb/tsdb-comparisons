package iot

import (
	"github.com/cnosdb/tsdb-comparisons/cmd/generate_queries/uses/common"
	"github.com/cnosdb/tsdb-comparisons/cmd/generate_queries/utils"
	"github.com/cnosdb/tsdb-comparisons/pkg/query"
)

// TruckBreakdownFrequency contains info for filling in truck breakdown frequency queries.
type TruckBreakdownFrequency struct {
	core utils.QueryGenerator
}

// NewTruckBreakdownFrequency creates a new truck breakdown frequency query filler.
func NewTruckBreakdownFrequency(core utils.QueryGenerator) utils.QueryFiller {
	return &TruckBreakdownFrequency{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *TruckBreakdownFrequency) Fill(q query.Query) query.Query {
	fc, ok := i.core.(TruckBreakdownFrequencyFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.TruckBreakdownFrequency(q)
	return q
}
