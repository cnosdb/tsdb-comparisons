// generate_data generates time series data from pre-specified use cases.
//
// Supported formats:
// InfluxDB bulk load format
// CnosDB bulk load format
// TimescaleDB pseudo-CSV format (the same as for ClickHouse)

// Supported use cases:
// devops: scale is the number of hosts to simulate, with log messages
//         every log-interval seconds.
// cpu-only: same as `devops` but only generate metrics for CPU
package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	
	"github.com/blagojts/viper"
	"github.com/cnosdb/tsdb-comparisons/internal/inputs"
	"github.com/cnosdb/tsdb-comparisons/internal/utils"
	"github.com/cnosdb/tsdb-comparisons/pkg/data/usecases/common"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets/initializers"
	"github.com/spf13/pflag"
)

var (
	profileFile string
	dg          = &inputs.DataGenerator{}
	config      = &common.DataGeneratorConfig{}
)

// Parse args:
func init() {
	config.AddToFlagSet(pflag.CommandLine)
	
	pflag.String("profile-file", "", "File to which to write go profiling data")
	
	pflag.Parse()
	
	err := utils.SetupConfigFile()
	
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	
	if err := viper.Unmarshal(&config.BaseConfig); err != nil {
		panic(fmt.Errorf("unable to decode base config: %s", err))
	}
	
	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
	
	profileFile = viper.GetString("profile-file")
}

func main() {
	if len(profileFile) > 0 {
		defer startMemoryProfile(profileFile)()
	}
	target := initializers.GetTarget(config.Format)
	err := dg.Generate(config, target)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
}

// startMemoryProfile sets up memory profiling to be written to profileFile. It
// returns a function to cleanup/write that should be deferred by the caller
func startMemoryProfile(profileFile string) func() {
	f, err := os.Create(profileFile)
	if err != nil {
		log.Fatal("could not create memory profile: ", err)
	}
	
	stop := func() {
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}
	
	// Catches ctrl+c signals
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		<-c
		
		fmt.Fprintln(os.Stderr, "\ncaught interrupt, stopping profile")
		stop()
		
		os.Exit(0)
	}()
	
	return stop
}
