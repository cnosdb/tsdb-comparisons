package iotdb

import (
	"fmt"
	"strings"
	"time"

	"github.com/cnosdb/tsdb-comparisons/cmd/generate_queries/databases"
	"github.com/cnosdb/tsdb-comparisons/cmd/generate_queries/uses/iot"
	"github.com/cnosdb/tsdb-comparisons/pkg/query"
)

// IoT produces TDengine-specific queries for all the iot query types.
type IoT struct {
	*iot.Core
	*BaseGenerator
}

// NewIoT makes an IoT object ready to generate Queries.
func NewIoT(start, end time.Time, scale int, g *BaseGenerator) *IoT {
	c, err := iot.NewCore(start, end, scale)
	databases.PanicIfErr(err)
	return &IoT{
		Core:          c,
		BaseGenerator: g,
	}
}

func (i *IoT) getTruckWhereString(nTrucks int) string {
	names, err := i.GetRandomTrucks(nTrucks)
	if err != nil {
		panic(err.Error())
	}
	for j := range names {
		names[j] = fmt.Sprintf(`"%s"`, names[j])
	}
	return fmt.Sprintf("name in (%s)", strings.Join(names, " ,"))
}

// LastLocByTruck finds the truck location for nTrucks.
// ReWrite
func (i *IoT) LastLocByTruck(qi query.Query, nTrucks int) {
	sql := fmt.Sprintf(`select name, driver, latitude, longitude from 
	(select last(*) from readings where %s group by name,driver )t1;`,
		i.getTruckWhereString(nTrucks))

	humanLabel := "TDengine last location by specific truck"
	humanDesc := fmt.Sprintf("%s: random %4d trucks", humanLabel, nTrucks)

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// LastLocPerTruck finds all the truck locations along with truck and driver names.
// ReWrite
func (i *IoT) LastLocPerTruck(qi query.Query) {

	sql := fmt.Sprintf(`select name, driver, latitude, longitude from 
	(select last(*) from readings where fleet="%s" group by name,driver )t1;`,
		i.GetRandomFleet())

	humanLabel := "TDengine last location per truck"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// TrucksWithLowFuel finds all trucks with low fuel (less than 10%).
// ReWrite
func (i *IoT) TrucksWithLowFuel(qi query.Query) {
	sql := fmt.Sprintf(`select ts,name,driver,fuel_state from 
		(select last(*) from diagnostics where fuel_state <=0.1
         and fleet="%s" group by name, driver)t1; `,
		i.GetRandomFleet())

	humanLabel := "TDengine trucks with low fuel"
	humanDesc := fmt.Sprintf("%s: under 10 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// TrucksWithHighLoad finds all trucks that have load over 90%.
// TODO: current_load >= 0.9*load_capacity not support
func (i *IoT) TrucksWithHighLoad(qi query.Query) {
	sql := fmt.Sprintf(`SELECT ts, name, driver, current_load, load_capacity from 
		(select last(*) from diagnostics where current_load >= 0.9*load_capacity 
		and fleet="%s"
		group by name, driver, load_capacity) limit 10;`,
		i.GetRandomFleet())
	//
	humanLabel := "TDengine trucks with high load"
	humanDesc := fmt.Sprintf("%s: over 90 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// StationaryTrucks finds all trucks that have low average velocity in a time window.
// TODO: not support mean_velocity < 1
func (i *IoT) StationaryTrucks(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.StationaryDuration)
	sql := fmt.Sprintf(`SELECT avg(velocity) as mean_velocity, name, driver, fleet
		 FROM readings 
		 WHERE ts > '%s' AND ts <= '%s' 
		 AND fleet = '%s' AND mean_velocity < 1
	     INTERVAL(10m)
		 GROUP BY name,driver,fleet`,
		interval.Start().Format(time.RFC3339),
		interval.End().Format(time.RFC3339),
		i.GetRandomFleet())

	humanLabel := "TDengine stationary trucks"
	humanDesc := fmt.Sprintf("%s: with low avg velocity in last 10 minutes", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// TrucksWithLongDrivingSessions finds all trucks that have not stopped at least 20 mins in the last 4 hours.
// TODO not support mean_velocity > 1
func (i *IoT) TrucksWithLongDrivingSessions(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.LongDrivingSessionDuration)
	sql := fmt.Sprintf(`SELECT name,driver 
		FROM(SELECT count(*) AS ten_min 
		 FROM(SELECT avg(velocity) AS mean_velocity 
		  FROM readings 
		  WHERE fleet = '%s' AND ts > '%s' AND ts <= '%s'
          INTERVAL(10m)
		  GROUP BY name,driver) 
		 WHERE mean_velocity > 1 
		 GROUP BY name,driver) 
		WHERE ten_min_mean_velocity > %d`,
		i.GetRandomFleet(),
		interval.Start().Format(time.RFC3339),
		interval.End().Format(time.RFC3339),
		// Calculate number of 10 min intervals that is the max driving duration for the session if we rest 5 mins per hour.
		tenMinutePeriods(5, iot.LongDrivingSessionDuration))

	humanLabel := "TDengine trucks with longer driving sessions"
	humanDesc := fmt.Sprintf("%s: stopped less than 20 mins in 4 hour period", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// TrucksWithLongDailySessions finds all trucks that have driven more than 10 hours in the last 24 hours.
// TODO
func (i *IoT) TrucksWithLongDailySessions(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.DailyDrivingDuration)
	sql := fmt.Sprintf(`SELECT name,driver 
		FROM(SELECT count(*) AS ten_min 
		 FROM(SELECT avg(velocity) AS mean_velocity 
		  FROM readings 
		  WHERE fleet = '%s' AND ts > '%s' AND ts <= '%s'
          INTERVAL(10m)
		  GROUP BY name,driver) 
		 WHERE mean_velocity > 1 
		 GROUP BY name,driver) 
		WHERE ten_min_mean_velocity > %d`,
		i.GetRandomFleet(),
		interval.Start().Format(time.RFC3339),
		interval.End().Format(time.RFC3339),
		// Calculate number of 10 min intervals that is the max driving duration for the session if we rest 35 mins per hour.
		tenMinutePeriods(35, iot.DailyDrivingDuration))

	humanLabel := "TDengine trucks with longer daily sessions"
	humanDesc := fmt.Sprintf("%s: drove more than 10 hours in the last 24 hours", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// AvgVsProjectedFuelConsumption calculates average and projected fuel consumption per fleet.
// TODO: not support avg(nominal_fuel_consumption)
func (i *IoT) AvgVsProjectedFuelConsumption(qi query.Query) {
	sql := `SELECT avg(fuel_consumption) AS mean_fuel_consumption, avg(nominal_fuel_consumption) AS nominal_fuel_consumption 
		FROM readings 
		WHERE velocity > 1 
		GROUP BY fleet`

	humanLabel := "TDengine average vs projected fuel consumption per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// AvgDailyDrivingDuration finds the average driving duration per driver.
// ok
func (i *IoT) AvgDailyDrivingDuration(qi query.Query) {
	start := i.Interval.Start().Format(time.RFC3339)
	end := i.Interval.End().Format(time.RFC3339)
	sql := fmt.Sprintf(`SELECT count(mv)/6 as hours_driven 
		FROM (SELECT avg(velocity) as mv 
		 FROM readings 
		 WHERE ts > '%s' AND ts < '%s'
		 INTERVAL(10m)
		 GROUP BY fleet, name, driver) 
		WHERE ts > '%s' AND ts < '%s' 
		INTERVAL(1d)`,
		start,
		end,
		start,
		end,
	)

	humanLabel := "TDengine average driver driving duration per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// AvgDailyDrivingSession finds the saverage driving session without stopping per driver per day.
// TODO
func (i *IoT) AvgDailyDrivingSession(qi query.Query) {
	start := i.Interval.Start().Format(time.RFC3339)
	end := i.Interval.End().Format(time.RFC3339)
	// TODO: not support
	sql := fmt.Sprintf(`SELECT elapsed 
		INTO random_measure2_1 
		FROM (SELECT difference(difka), elapsed(difka, 1m) 
		 FROM (SELECT difka 
		  FROM (SELECT difference(mv) AS difka 
		   FROM (SELECT floor(avg(velocity)/10)/floor(avg(velocity)/10) AS mv 
		    FROM readings 
		    WHERE name!='' AND ts > '%s' AND ts < '%s' 
		    INTERVAL(10m)
		    GROUP BY name fill(0)) 
		   GROUP BY name) 
		  WHERE difka!=0 
		  GROUP BY name) 
		 GROUP BY name) 
		WHERE difference = -2 
		GROUP BY name; 
		SELECT avg(elapsed) 
		FROM random_measure2_1 
		WHERE ts > '%s' AND ts < '%s' 
		GROUP BY time(1d),name`,
		start,
		end,
		start,
		end,
	)

	humanLabel := "TDengine average driver driving session without stopping per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// AvgLoad finds the average load per truck model per fleet.
// TODO: not support current_load/load_capacity
func (i *IoT) AvgLoad(qi query.Query) {
	sql := `SELECT avg(current_load/load_capacity) AS mean_load_percentage 
		 FROM diagnostics 
		 GROUP BY name, fleet, model`

	humanLabel := "TDengine average load per truck model per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// DailyTruckActivity returns the number of hours trucks has been active (not out-of-commission) per day per fleet per model.
// TODO: not support ms<1
func (i *IoT) DailyTruckActivity(qi query.Query) {
	start := i.Interval.Start().Format(time.RFC3339)
	end := i.Interval.End().Format(time.RFC3339)
	sql := fmt.Sprintf(`SELECT count(ms)/144 
		FROM (SELECT avg(status) AS ms 
		 FROM diagnostics 
		 WHERE ts >= '%s' AND ts < '%s' 
         INTERVAL(10m)
		 GROUP BY model, fleet) 
		WHERE ts >= '%s' AND ts < '%s' AND ms<1 
		INTERVAL(1d)
		GROUP BY  model, fleet`,
		start,
		end,
		start,
		end,
	)

	humanLabel := "TDengine daily truck activity per fleet per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// TruckBreakdownFrequency calculates the amount of times a truck model broke down in the last period.
// TODO
func (i *IoT) TruckBreakdownFrequency(qi query.Query) {
	start := i.Interval.Start().Format(time.RFC3339)
	end := i.Interval.End().Format(time.RFC3339)
	sql := fmt.Sprintf(`SELECT count(state_changed) 
		FROM (SELECT difference(broken_down) AS state_changed 
		 FROM (SELECT floor(2*(sum(nzs)/count(nzs)))/floor(2*(sum(nzs)/count(nzs))) AS broken_down 
		  FROM (SELECT model, status/status AS nzs 
		   FROM diagnostics 
		   WHERE ts >= '%s' AND ts < '%s') 
		  WHERE ts >= '%s' AND ts < '%s'
	      INTERVAL(10m)
		  GROUP BY time(10m),model) 
		 GROUP BY model) 
		WHERE state_changed = 1 
		GROUP BY model`,
		start,
		end,
		start,
		end,
	)

	humanLabel := "TDengine truck breakdown frequency per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// tenMinutePeriods calculates the number of 10 minute periods that can fit in
// the time duration if we subtract the minutes specified by minutesPerHour value.
// E.g.: 4 hours - 5 minutes per hour = 3 hours and 40 minutes = 22 ten minute periods
func tenMinutePeriods(minutesPerHour float64, duration time.Duration) int {
	durationMinutes := duration.Minutes()
	leftover := minutesPerHour * duration.Hours()
	return int((durationMinutes - leftover) / 10)
}
