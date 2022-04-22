package usecases

import (
	"fmt"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/data/usecases/iot"
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
