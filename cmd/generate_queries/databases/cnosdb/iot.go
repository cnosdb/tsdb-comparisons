package cnosdb

import (
	"fmt"
	"strings"
	"time"

	"github.com/cnosdb/tsdb-comparisons/cmd/generate_queries/databases"
	"github.com/cnosdb/tsdb-comparisons/cmd/generate_queries/uses/iot"
	"github.com/cnosdb/tsdb-comparisons/pkg/query"
)

// IoT produces cnosdb-specific queries for all the iot query types.
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

func (i *IoT) getTrucksWhereWithNames(names []string) string {
	nameClauses := []string{}
	for _, s := range names {
		nameClauses = append(nameClauses, fmt.Sprintf("\"name\" = '%s'", s))
	}

	combinedHostnameClause := strings.Join(nameClauses, " or ")
	return "(" + combinedHostnameClause + ")"
}

func (i *IoT) getTruckWhereString(nTrucks int) string {
	names, err := i.GetRandomTrucks(nTrucks)
	if err != nil {
		panic(err.Error())
	}
	return i.getTrucksWhereWithNames(names)
}

// LastLocByTruck finds the truck location for nTrucks.
func (i *IoT) LastLocByTruck(qi query.Query, nTrucks int) {
	cnosql := fmt.Sprintf(`SELECT "name", "driver", "latitude", "longitude" 
		FROM "readings" 
		WHERE %s 
		ORDER BY "time" 
		LIMIT 1`,
		i.getTruckWhereString(nTrucks))

	humanLabel := "cnosdb last location by specific truck"
	humanDesc := fmt.Sprintf("%s: random %4d trucks", humanLabel, nTrucks)

	i.fillInQuery(qi, humanLabel, humanDesc, cnosql)
}

// LastLocPerTruck finds all the truck locations along with truck and driver names.
func (i *IoT) LastLocPerTruck(qi query.Query) {

	cnosql := fmt.Sprintf(`SELECT min("latitude"), min("longitude") 
		FROM "readings" 
		WHERE "fleet"='%s' 
		GROUP BY "name","driver" 
		ORDER BY "name","driver" 
		LIMIT 1`,
		i.GetRandomFleet())

	humanLabel := "cnosdb last location per truck"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, cnosql)
}

// TrucksWithLowFuel finds all trucks with low fuel (less than 10%).
func (i *IoT) TrucksWithLowFuel(qi query.Query) {
	cnosql := fmt.Sprintf(`SELECT "name", min("driver"), min("fuel_state") 
		FROM "diagnostics" 
		WHERE "fuel_state" <= 0.1 AND "fleet" = '%s' 
		GROUP BY "name" 
		ORDER BY "name" DESC 
		LIMIT 1`,
		i.GetRandomFleet())

	humanLabel := "cnosdb trucks with low fuel"
	humanDesc := fmt.Sprintf("%s: under 10 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, cnosql)
}

// TrucksWithHighLoad finds all trucks that have load over 90%.
func (i *IoT) TrucksWithHighLoad(qi query.Query) {
	cnosql := fmt.Sprintf(`SELECT "name", min("driver"), min("current_load"), min("load_capacity") 
		FROM (SELECT "name", "driver", min("current_load") AS "current_load" ,min("load_capacity") AS "load_capacity" 
		 FROM "diagnostics" WHERE fleet = '%s' 
		 GROUP BY "name","driver" 
		 ORDER BY "name","driver" DESC 
		 LIMIT 1) 
		WHERE "current_load" >= 0.9 * "load_capacity" 
		GROUP BY "name" 
		ORDER BY "name" DESC`,
		i.GetRandomFleet())

	humanLabel := "cnosdb trucks with high load"
	humanDesc := fmt.Sprintf("%s: over 90 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, cnosql)
}

// StationaryTrucks finds all trucks that have low average velocity in a time window.
func (i *IoT) StationaryTrucks(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.StationaryDuration)
	cnosql := fmt.Sprintf(`SELECT "name", min("driver") 
		FROM(SELECT "name", "driver", "fleet", avg("velocity") as mean_velocity 
		 FROM "readings" 
		 WHERE time > '%s' AND time <= '%s' 
		 GROUP BY DATE_BIN(INTERVAL '10 minutes', time, TIMESTAMP '1970-01-01T00:00:00Z'), "name", "driver", "fleet"   
		 LIMIT 1) 
		WHERE "fleet" = '%s' AND "mean_velocity" < 1  
		GROUP BY "name"`,
		interval.Start().Format(time.RFC3339),
		interval.End().Format(time.RFC3339),
		i.GetRandomFleet())

	humanLabel := "cnosdb stationary trucks"
	humanDesc := fmt.Sprintf("%s: with low avg velocity in last 10 minutes", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, cnosql)
}

// TrucksWithLongDrivingSessions finds all trucks that have not stopped at least 20 mins in the last 4 hours.
func (i *IoT) TrucksWithLongDrivingSessions(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.LongDrivingSessionDuration)
	cnosql := fmt.Sprintf(`SELECT "name","driver" 
		FROM(SELECT "name", "driver", count(*) AS ten_min_mean_velocity 
		 FROM(SELECT "name", "driver", avg("velocity") AS mean_velocity 
		  FROM readings 
		  WHERE "fleet" = '%s' AND time > '%s' AND time <= '%s' 
		  GROUP BY DATE_BIN(INTERVAL '10 minutes', time, TIMESTAMP '1970-01-01T00:00:00Z'), "name", "driver")  
		 WHERE "mean_velocity" > 1 
		 GROUP BY "name","driver") 
		WHERE ten_min_mean_velocity > %d`,
		i.GetRandomFleet(),
		interval.Start().Format(time.RFC3339),
		interval.End().Format(time.RFC3339),
		// Calculate number of 10 min intervals that is the max driving duration for the session if we rest 5 mins per hour.
		tenMinutePeriods(5, iot.LongDrivingSessionDuration))

	humanLabel := "cnosdb trucks with longer driving sessions"
	humanDesc := fmt.Sprintf("%s: stopped less than 20 mins in 4 hour period", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, cnosql)
}

// TrucksWithLongDailySessions finds all trucks that have driven more than 10 hours in the last 24 hours.
func (i *IoT) TrucksWithLongDailySessions(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.DailyDrivingDuration)
	cnosql := fmt.Sprintf(`SELECT "name","driver" 
		FROM(SELECT "name", "driver", count(*) AS ten_min_mean_velocity 
		 FROM(SELECT "name", "driver", avg("velocity") AS mean_velocity 
		  FROM readings 
		  WHERE "fleet" = '%s' AND time > '%s' AND time <= '%s' 
		  GROUP BY DATE_BIN(INTERVAL '10 minutes', time, TIMESTAMP '1970-01-01T00:00:00Z'), "name", "driver")  
		 WHERE "mean_velocity" > 1 
		 GROUP BY "name","driver") 
		WHERE ten_min_mean_velocity > %d`,
		i.GetRandomFleet(),
		interval.Start().Format(time.RFC3339),
		interval.End().Format(time.RFC3339),
		// Calculate number of 10 min intervals that is the max driving duration for the session if we rest 35 mins per hour.
		tenMinutePeriods(35, iot.DailyDrivingDuration))

	humanLabel := "cnosdb trucks with longer daily sessions"
	humanDesc := fmt.Sprintf("%s: drove more than 10 hours in the last 24 hours", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, cnosql)
}

// AvgVsProjectedFuelConsumption calculates average and projected fuel consumption per fleet.
func (i *IoT) AvgVsProjectedFuelConsumption(qi query.Query) {
	cnosql := `SELECT avg("fuel_consumption") AS "mean_fuel_consumption", avg("nominal_fuel_consumption") AS "nominal_fuel_consumption" 
		FROM "readings" 
		WHERE "velocity" > 1 
		GROUP BY "fleet"`

	humanLabel := "cnosdb average vs projected fuel consumption per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, cnosql)
}

// AvgDailyDrivingDuration finds the average driving duration per driver.
func (i *IoT) AvgDailyDrivingDuration(qi query.Query) {
	start := i.Interval.Start().Format(time.RFC3339)
	end := i.Interval.End().Format(time.RFC3339)
	cnosql := fmt.Sprintf(`SELECT count("mv")/6 as "hours driven" 
		FROM (SELECT DATE_BIN(INTERVAL '10 minutes', time, TIMESTAMP '1970-01-01T00:00:00Z') as "time", "fleet", "name", "driver", avg("velocity") as "mv" 
		 FROM "readings" 
		 WHERE time > '%s' AND time < '%s' 
		 GROUP BY DATE_BIN(INTERVAL '10 minutes', time, TIMESTAMP '1970-01-01T00:00:00Z'), "fleet", "name", "driver")
		WHERE time > '%s' AND time < '%s' 
		GROUP BY DATE_BIN(INTERVAL '1 day', time, TIMESTAMP '1970-01-01T00:00:00Z'), "fleet", "name", "driver"`,
		start,
		end,
		start,
		end,
	)

	humanLabel := "cnosdb average driver driving duration per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, cnosql)
}

// AvgDailyDrivingSession finds the average driving session without stopping per driver per day.
func (i *IoT) AvgDailyDrivingSession(qi query.Query) {
	start := i.Interval.Start().Format(time.RFC3339)
	end := i.Interval.End().Format(time.RFC3339)
	cnosql := fmt.Sprintf(`WITH driver_status
		AS (
			SELECT name, time_window(time, '10m') as ten_minutes, avg(velocity) > 5 AS driving
			FROM readings
			WHERE name!='' AND time > '%s' AND time < '%s'
			GROUP BY name, ten_minutes
			ORDER BY name, ten_minutes.start
			), driver_status_change
		AS (
			SELECT name, start, lead(start) OVER (PARTITION BY name ORDER BY start) AS stop, driving
			FROM (
				SELECT name, ten_minutes.start AS start, driving, lag(driving) OVER (PARTITION BY name ORDER BY ten_minutes.start) AS prev_driving
				FROM driver_status
				) x
			WHERE x.driving <> x.prev_driving
			)
		SELECT name, time_window(start, '24h') AS day, avg(stop::bigint - start::bigint) AS duration
		FROM driver_status_change
		WHERE name IS NOT NULL
		AND driving = true
		GROUP BY name, day
		ORDER BY name, day.start`,
		start,
		end,
	)

	humanLabel := "cnosdb average driver driving session without stopping per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, cnosql)
}

// AvgLoad finds the average load per truck model per fleet.
func (i *IoT) AvgLoad(qi query.Query) {
	start := i.Interval.Start().Format(time.RFC3339)
	end := i.Interval.End().Format(time.RFC3339)
	cnosql := fmt.Sprintf(`SELECT avg(current_load/load_capacity)
		FROM diagnostics
		WHERE time >= '%s' AND time < '%s' 
		GROUP BY fleet, model
		limit 10`,
		start,
		end,
	)

	humanLabel := "cnosdb average load per truck model per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, cnosql)
}

// DailyTruckActivity returns the number of hours trucks has been active (not out-of-commission) per day per fleet per model.
func (i *IoT) DailyTruckActivity(qi query.Query) {
	start := i.Interval.Start().Format(time.RFC3339)
	end := i.Interval.End().Format(time.RFC3339)
	cnosql := fmt.Sprintf(`SELECT count(ms)/144 
			FROM 
		(SELECT mean(status) AS ms, time_window(time, '10m') AS window, model, fleet
			FROM diagnostics
			WHERE time >= '%s' AND time < '%s' 
			GROUP BY window, model, fleet) 
			WHERE ms < 1 
			GROUP BY time_window(window.start, '1d'), model, fleet`,
		start,
		end,
	)

	humanLabel := "cnosdb daily truck activity per fleet per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, cnosql)
}

// TruckBreakdownFrequency calculates the amount of times a truck model broke down in the last period.
func (i *IoT) TruckBreakdownFrequency(qi query.Query) {
	start := i.Interval.Start().Format(time.RFC3339)
	end := i.Interval.End().Format(time.RFC3339)
	cnosql := fmt.Sprintf(`WITH base 
		AS (
			SELECT time, model, status/status AS nzs 
			FROM "diagnostics" 
			where model is not null
			and time >= '%s' AND time < '%s'
		), breakdown_per_truck_per_ten_minutes
		AS (
			SELECT time_window(TIME, '10m') AS ten_minutes, model, count(nzs) / count(*) < 0.5 AS broken_down
			FROM base
			GROUP BY ten_minutes, model
			), breakdowns_per_truck
		AS (
			SELECT model, broken_down, lead(broken_down) OVER (
					PARTITION BY model ORDER BY ten_minutes.start
					) AS next_broken_down
			FROM breakdown_per_truck_per_ten_minutes
			)
		SELECT model, count(*)
		FROM breakdowns_per_truck
		WHERE broken_down = false AND next_broken_down = true
		GROUP BY model`,
		start,
		end,
	)

	humanLabel := "cnosdb truck breakdown frequency per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, cnosql)
}

// tenMinutePeriods calculates the number of 10 minute periods that can fit in
// the time duration if we subtract the minutes specified by minutesPerHour value.
// E.g.: 4 hours - 5 minutes per hour = 3 hours and 40 minutes = 22 ten minute periods
func tenMinutePeriods(minutesPerHour float64, duration time.Duration) int {
	durationMinutes := duration.Minutes()
	leftover := minutesPerHour * duration.Hours()
	return int((durationMinutes - leftover) / 10)
}
