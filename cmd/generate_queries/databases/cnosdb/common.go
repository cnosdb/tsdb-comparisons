package cnosdb

import (
	"time"

	"github.com/cnosdb/tsdb-comparisons/cmd/generate_queries/uses/iot"
	"github.com/cnosdb/tsdb-comparisons/cmd/generate_queries/utils"
	"github.com/cnosdb/tsdb-comparisons/pkg/query"
)

// BaseGenerator contains settings specific for cnosdb database.
type BaseGenerator struct {
}

// GenerateEmptyQuery returns an empty query.HTTP.
func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewHTTP()
}

// fillInQuery fills the query struct with data.
func (g *BaseGenerator) fillInQuery(qi query.Query, humanLabel, humanDesc, cnosql string) {
	q := qi.(*query.HTTP)
	q.HumanLabel = []byte(humanLabel)
	q.RawQuery = []byte(cnosql)
	q.HumanDescription = []byte(humanDesc)
	q.Method = []byte("POST")
	q.Path = []byte("/api/v1/sql")
	q.Body = []byte(cnosql)
}

// NewIoT creates a new iot use case query generator.
func (g *BaseGenerator) NewIoT(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := iot.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	devops := &IoT{
		BaseGenerator: g,
		Core:          core,
	}

	return devops, nil
}
