|query-type|Function|状态|
|-|-|-|
|low-fuel| 	TrucksWithLowFuel|	ok|
|high-load|	TrucksWithHighLoad|	需要和influx一样把一些tags改成field|
|stationary-trucks|	StationaryTrucks|	where条件里不支持别名|
|long-daily-sessions|	TrucksWithLongDailySessions|	where条件里不支持别名|
|long-driving-sessions|	TrucksWithLongDrivingSessions|	where条件里不支持别名|
|avg-vs-projected-fuel-consumption|	AvgVsProjectedFuelConsumption|	需要和influx一样把一些tags改成field|
|avg-daily-driving-duration|	AvgDailyDrivingDuration|	ok|
|avg-daily-driving-session|	AvgDailyDrivingSession|	嵌套查询层数太多|
|avg-load|	AvgLoad|	需要和influx一样把一些tags改成field|
|daily-activity|	DailyTruckActivity|	where条件里不支持别名|
|breakdown-frequency|	TruckBreakdownFrequency|	嵌套查询层数太多|
|single-last-loc|	LastLocByTruck|	ok|
|last-loc|	LastLocPerTruck|	ok|