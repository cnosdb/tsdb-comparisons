// tsbs_generate_queries generates queries for various use cases. Its output will
// be consumed by the corresponding tsbs_run_queries_ program.
package main

import (
	"fmt"
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/internal/inputs"
	internalUtils "github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query/config"
	"os"
)

var useCaseMatrix = map[string]map[string]utils.QueryFillerMaker{
	"iot": {
		iot.LabelLastLoc:                       iot.NewLastLocPerTruck,
		iot.LabelLastLocSingleTruck:            iot.NewLastLocSingleTruck,
		iot.LabelLowFuel:                       iot.NewTruckWithLowFuel,
		iot.LabelHighLoad:                      iot.NewTruckWithHighLoad,
		iot.LabelStationaryTrucks:              iot.NewStationaryTrucks,
		iot.LabelLongDrivingSessions:           iot.NewTrucksWithLongDrivingSession,
		iot.LabelLongDailySessions:             iot.NewTruckWithLongDailySession,
		iot.LabelAvgVsProjectedFuelConsumption: iot.NewAvgVsProjectedFuelConsumption,
		iot.LabelAvgDailyDrivingDuration:       iot.NewAvgDailyDrivingDuration,
		iot.LabelAvgDailyDrivingSession:        iot.NewAvgDailyDrivingSession,
		iot.LabelAvgLoad:                       iot.NewAvgLoad,
		iot.LabelDailyActivity:                 iot.NewDailyTruckActivity,
		iot.LabelBreakdownFrequency:            iot.NewTruckBreakdownFrequency,
	},
}

var conf = &config.QueryGeneratorConfig{}

// Parse args:
func init() {
	useCaseMatrix["cpu-only"] = useCaseMatrix["devops"]
	// Change the Usage function to print the use case matrix of choices:
	oldUsage := pflag.Usage
	pflag.Usage = func() {
		oldUsage()

		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "The use case matrix of choices is:\n")
		for uc, queryTypes := range useCaseMatrix {
			for qt := range queryTypes {
				fmt.Fprintf(os.Stderr, "  use case: %s, query type: %s\n", uc, qt)
			}
		}
	}

	conf.AddToFlagSet(pflag.CommandLine)

	pflag.Parse()

	err := internalUtils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&conf.BaseConfig); err != nil {
		panic(fmt.Errorf("unable to decode base config: %s", err))
	}

	if err := viper.Unmarshal(&conf); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
}

func main() {
	qg := inputs.NewQueryGenerator(useCaseMatrix)
	err := qg.Generate(conf)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
}
