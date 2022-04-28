package usecases

import (
	"fmt"
	"github.com/cnosdb/tsdb-comparisons/internal/utils"
	"github.com/cnosdb/tsdb-comparisons/pkg/data/usecases/common"
	"github.com/cnosdb/tsdb-comparisons/pkg/data/usecases/iot"
)

const errCannotParseTimeFmt = "cannot parse time from string '%s': %v"

func GetSimulatorConfig(dgc *common.DataGeneratorConfig) (common.SimulatorConfig, error) {
	var ret common.SimulatorConfig
	var err error
	tsStart, err := utils.ParseUTCTime(dgc.TimeStart)
	if err != nil {
		return nil, fmt.Errorf(errCannotParseTimeFmt, dgc.TimeStart, err)
	}
	tsEnd, err := utils.ParseUTCTime(dgc.TimeEnd)
	if err != nil {
		return nil, fmt.Errorf(errCannotParseTimeFmt, dgc.TimeEnd, err)
	}
	
	switch dgc.Use {
	
	case common.UseCaseIoT:
		ret = &iot.SimulatorConfig{
			Start: tsStart,
			End:   tsEnd,
			
			InitGeneratorScale:   dgc.InitialScale,
			GeneratorScale:       dgc.Scale,
			GeneratorConstructor: iot.NewTruck,
		}
	default:
		err = fmt.Errorf("unknown use case: '%s'", dgc.Use)
	}
	return ret, err
}
