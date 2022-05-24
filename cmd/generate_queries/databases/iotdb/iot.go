package iotdb

import (
	"fmt"
	"strings"
	"time"

	"github.com/cnosdb/tsdb-comparisons/cmd/generate_queries/databases"
	"github.com/cnosdb/tsdb-comparisons/cmd/generate_queries/uses/iot"
	"github.com/cnosdb/tsdb-comparisons/pkg/query"
)

// IoT produces iotdb-specific queries for all the iot query types.
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
// ok
func (i *IoT) LastLocByTruck(qi query.Query, nTrucks int) {
	sql := fmt.Sprintf(`select last_value(latitude), last_value(longitude) from root.*.*.truck_%d.**;`,
		nTrucks)
	humanLabel := "iotdb last location by specific truck"
	humanDesc := fmt.Sprintf("%s: random %4d trucks", humanLabel, nTrucks)

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// LastLocPerTruck finds all the truck locations along with truck and driver names.
// ReWrite
func (i *IoT) LastLocPerTruck(qi query.Query) {

	sql := fmt.Sprintf(`select last_value(latitude), last_value(longitude) from root.*.*.*.%s.** group by level=3,5;`,
		i.GetRandomFleet())
	humanLabel := "iotdb last location per truck"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// TrucksWithLowFuel finds all trucks with low fuel (less than 10%).
// ok
func (i *IoT) TrucksWithLowFuel(qi query.Query) {
	sql := fmt.Sprintf(`SELECT LAST_VALUE(fuel_state) FROM root.benchmark.diagnostics.*.%s.** WHERE fuel_state <= 1.0 group by level=3,4,5;`,
		i.GetRandomFleet())

	humanLabel := "iotdb trucks with low fuel"
	humanDesc := fmt.Sprintf("%s: under 10 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// TrucksWithHighLoad finds all trucks that have load over 90%.
// Not support subquery
func (i *IoT) TrucksWithHighLoad(qi query.Query) {
	sql := fmt.Sprintf(`SELECT ts, name, driver, current_load, load_capacity from 
		(select last(*) from diagnostics where current_load >= 0.9*load_capacity 
		and fleet="%s"
		group by name, driver, load_capacity) limit 10;`,
		i.GetRandomFleet())
	//
	humanLabel := "iotdb trucks with high load"
	humanDesc := fmt.Sprintf("%s: over 90 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// StationaryTrucks finds all trucks that have low average velocity in a time window.
// Not support subquery
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

	humanLabel := "iotdb stationary trucks"
	humanDesc := fmt.Sprintf("%s: with low avg velocity in last 10 minutes", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// TrucksWithLongDrivingSessions finds all trucks that have not stopped at least 20 mins in the last 4 hours.
// Not support subquery
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

	humanLabel := "iotdb trucks with longer driving sessions"
	humanDesc := fmt.Sprintf("%s: stopped less than 20 mins in 4 hour period", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// TrucksWithLongDailySessions finds all trucks that have driven more than 10 hours in the last 24 hours.
// Not support subquery
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

	humanLabel := "iotdb trucks with longer daily sessions"
	humanDesc := fmt.Sprintf("%s: drove more than 10 hours in the last 24 hours", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// AvgVsProjectedFuelConsumption calculates average and projected fuel consumption per fleet.
func (i *IoT) AvgVsProjectedFuelConsumption(qi query.Query) {
	sql := `select avg(fuel_consumption), avg(nominal_fuel_consumption) from root.** group by level=4`
	humanLabel := "iotdb average vs projected fuel consumption per fleet"
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// AvgDailyDrivingDuration finds the average driving duration per driver.
// Not support subquery
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

	humanLabel := "iotdb average driver driving duration per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// AvgDailyDrivingSession finds the average driving session without stopping per driver per day.
// Not support subquery
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

	humanLabel := "iotdb average driver driving session without stopping per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// AvgLoad finds the average load per truck model per fleet.
// Not support subquery
func (i *IoT) AvgLoad(qi query.Query) {
	sql := `SELECT avg(current_load/load_capacity) AS mean_load_percentage 
		 FROM diagnostics 
		 GROUP BY name, fleet, model`

	humanLabel := "iotdb average load per truck model per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// DailyTruckActivity returns the number of hours trucks has been active (not out-of-commission) per day per fleet per model.
// Not support subquery
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

	humanLabel := "iotdb daily truck activity per fleet per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// TruckBreakdownFrequency calculates the amount of times a truck model broke down in the last period.
// Not support subquery
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

	humanLabel := "iotdb truck breakdown frequency per model"
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
