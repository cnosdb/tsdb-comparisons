package cnosdb

import (
	"github.com/blagojts/viper"
	"github.com/cnosdb/tsdb-comparisons/pkg/data/serialize"
	"github.com/cnosdb/tsdb-comparisons/pkg/data/source"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets/constants"
	"github.com/spf13/pflag"
	"time"
)

func NewTarget() targets.ImplementedTarget {
	return &cnosdbTarget{}
}

type cnosdbTarget struct {
}

func (t *cnosdbTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"urls", "http://localhost:8086", "CnosDBURLs, comma-separated. Will be used in a round-robin fashion.")
	flagSet.Int(flagPrefix+"replication-factor", 1, "Cluster replication factor (only applies to clustered databases).")
	flagSet.String(flagPrefix+"consistency", "all", "Write consistency. Must be one of: any, one, quorum, all.")
	flagSet.Duration(flagPrefix+"backoff", time.Second, "Time to sleep between requests when server indicates backpressure is needed.")
	flagSet.Bool(flagPrefix+"gzip", true, "Whether to gzip encode requests (default true).")
}

func (t *cnosdbTarget) TargetName() string {
	return constants.FormatCnosDB
}

func (t *cnosdbTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *cnosdbTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("not implemented")
}
