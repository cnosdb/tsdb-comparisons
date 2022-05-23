package iotdb

import (
	"github.com/blagojts/viper"
	"github.com/cnosdb/tsdb-comparisons/pkg/data/serialize"
	"github.com/cnosdb/tsdb-comparisons/pkg/data/source"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets/constants"
	"github.com/spf13/pflag"
)

type tdengineTarget struct {
}

func NewTarget() targets.ImplementedTarget {
	return &tdengineTarget{}
}

func (t *tdengineTarget) TargetName() string {
	return constants.FormatIOTDB
}

func (t *tdengineTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *tdengineTarget) Benchmark(
	targetDB string, dataSourceConfig *source.DataSourceConfig, v *viper.Viper,
) (targets.Benchmark, error) {
	var loadingOptions LoadingOptions
	if err := v.Unmarshal(&loadingOptions); err != nil {
		return nil, err
	}
	return NewBenchmark(targetDB, &loadingOptions, dataSourceConfig)
}

func (t *tdengineTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"host", "127.0.0.1", "Hostname of TimescaleDB (tdengine) instance")
	flagSet.String(flagPrefix+"port", "6041", "Which port to connect to on the database host")
	flagSet.String(flagPrefix+"user", "root", "User to connect to tdengine as")
	flagSet.String(flagPrefix+"pass", "taosdata", "Password for user connecting to tdengine")
}
