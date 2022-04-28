package initializers

import (
	"fmt"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets/cnosdb"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets/constants"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets/influx"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets/timescaledb"
	"strings"
)

func GetTarget(format string) targets.ImplementedTarget {
	switch format {
	case constants.FormatTimescaleDB:
		return timescaledb.NewTarget()
	case constants.FormatInflux:
		return influx.NewTarget()
	case constants.FormatCnosDB:
		return cnosdb.NewTarget()
	}
	supportedFormatsStr := strings.Join(constants.SupportedFormats(), ",")
	panic(fmt.Sprintf("Unrecognized format %s, supported: %s", format, supportedFormatsStr))
}
