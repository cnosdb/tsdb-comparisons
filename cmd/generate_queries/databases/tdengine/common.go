package tdengine

import (
	"time"

	"github.com/cnosdb/tsdb-comparisons/cmd/generate_queries/uses/iot"
	"github.com/cnosdb/tsdb-comparisons/cmd/generate_queries/utils"
	"github.com/cnosdb/tsdb-comparisons/pkg/query"
)

// BaseGenerator contains settings specific for TimescaleDB
type BaseGenerator struct {
}

// GenerateEmptyQuery returns an empty query.TDengine.
func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewTDengine()
}

// fillInQuery fills the query struct with data.
func (g *BaseGenerator) fillInQuery(qi query.Query, humanLabel, humanDesc, sql string) {
	q := qi.(*query.TDengine)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.SqlQuery = []byte(sql)
}

// NewIoT creates a new iot use case query generator.
func (g *BaseGenerator) NewIoT(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := iot.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	i := &IoT{
		BaseGenerator: g,
		Core:          core,
	}

	return i, nil
}
