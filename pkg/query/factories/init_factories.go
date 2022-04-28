package factories

import (
	"github.com/cnosdb/tsdb-comparisons/cmd/generate_queries/databases/cnosdb"
	"github.com/cnosdb/tsdb-comparisons/cmd/generate_queries/databases/influx"
	"github.com/cnosdb/tsdb-comparisons/cmd/generate_queries/databases/timescaledb"
	
	"github.com/cnosdb/tsdb-comparisons/pkg/query/config"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets/constants"
)

func InitQueryFactories(config *config.QueryGeneratorConfig) map[string]interface{} {
	factories := make(map[string]interface{})
	factories[constants.FormatInflux] = &influx.BaseGenerator{}
	factories[constants.FormatCnosDB] = &cnosdb.BaseGenerator{}
	factories[constants.FormatTimescaleDB] = &timescaledb.BaseGenerator{
		UseJSON:       config.TimescaleUseJSON,
		UseTags:       config.TimescaleUseTags,
		UseTimeBucket: config.TimescaleUseTimeBucket,
	}
	
	return factories
}
