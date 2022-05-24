package iotdb

import (
	"github.com/blagojts/viper"
	"github.com/cnosdb/tsdb-comparisons/pkg/data/serialize"
	"github.com/cnosdb/tsdb-comparisons/pkg/data/source"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets/constants"
	"github.com/spf13/pflag"
)

type iotdbTarget struct {
}

func NewTarget() targets.ImplementedTarget {
	return &iotdbTarget{}
}

func (t *iotdbTarget) TargetName() string {
	return constants.FormatIOTDB
}

func (t *iotdbTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *iotdbTarget) Benchmark(
	targetDB string, dataSourceConfig *source.DataSourceConfig, v *viper.Viper,
) (targets.Benchmark, error) {
	var loadingOptions LoadingOptions
	if err := v.Unmarshal(&loadingOptions); err != nil {
		return nil, err
	}
	return NewBenchmark(targetDB, &loadingOptions, dataSourceConfig)
}

func (t *iotdbTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"host", "127.0.0.1", "Hostname of iotdb instance")
	flagSet.String(flagPrefix+"port", "6667", "Which port to connect to on the database host")
	flagSet.String(flagPrefix+"user", "root", "User to connect to iotdb as")
	flagSet.String(flagPrefix+"pass", "root", "Password for user connecting to iotdb")
}
