// load_tdengine loads a TDEngine instance with data from stdin.
//
// If the database exists beforehand, it will be *DROPPED*.
package main

import (
	"fmt"

	"github.com/blagojts/viper"
	"github.com/cnosdb/tsdb-comparisons/internal/utils"
	"github.com/cnosdb/tsdb-comparisons/load"
	"github.com/cnosdb/tsdb-comparisons/pkg/data/source"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets/tdengine"
	"github.com/spf13/pflag"
)

// Parse args:
func initProgramOptions() (*tdengine.LoadingOptions, load.BenchmarkRunner, *load.BenchmarkRunnerConfig) {
	target := tdengine.NewTarget()
	loaderConf := load.BenchmarkRunnerConfig{}
	loaderConf.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)
	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&loaderConf); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
	opts := tdengine.LoadingOptions{}
	viper.SetTypeByDefaultValue(true)
	opts.Host = viper.GetString("host")
	opts.Port = viper.GetString("port")
	opts.User = viper.GetString("user")
	opts.Pass = viper.GetString("pass")
	opts.ConnDB = viper.GetString("db-name")
	opts.LogBatches = viper.GetBool("log-batches")
	opts.ProfileFile = viper.GetString("write-profile")

	loader := load.GetBenchmarkRunner(loaderConf)
	return &opts, loader, &loaderConf
}

func main() {
	opts, loader, loaderConf := initProgramOptions()

	// If specified, generate a performance profile
	if len(opts.ProfileFile) > 0 {
		go profileCPUAndMem(opts.ProfileFile)
	}

	benchmark, err := tdengine.NewBenchmark(loaderConf.DBName, opts, &source.DataSourceConfig{
		Type: source.FileDataSourceType,
		File: &source.FileDataSourceConfig{Location: loaderConf.FileName},
	})
	if err != nil {
		panic(err)
	}

	loader.RunBenchmark(benchmark)
}
