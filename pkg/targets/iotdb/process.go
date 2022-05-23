package iotdb

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iotdb-client-go/client"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets"
)

type insertData struct {
	tags   string
	fields string
}

type processor struct {
	opts   *LoadingOptions
	dbName string

	session *client.Session
}

func newProcessor(opts *LoadingOptions, dbName string) *processor {
	return &processor{
		opts:   opts,
		dbName: dbName,
	}
}

func (p *processor) Init(_ int, doLoad, hashWorkers bool) {
	config := &client.Config{
		Host:     p.opts.Host,
		Port:     p.opts.Port,
		UserName: p.opts.User,
		Password: p.opts.Pass}
	p.session = client.NewSession(config)
	if err := p.session.Open(false, 0); err != nil {
		fmt.Printf("Connect to iotdb %+v failed %v\n", config, err)
		panic("")
	}
}

func (p *processor) Close(doLoad bool) {}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batches := b.(*hypertableArr)
	rowCnt := 0
	metricCnt := uint64(0)
	for hypertable, rows := range batches.m {
		rowCnt += len(rows)
		if doLoad {
			start := time.Now()
			metricCnt += p.processCSI(hypertable, rows)

			if p.opts.LogBatches {
				now := time.Now()
				took := now.Sub(start)
				batchSize := len(rows)
				fmt.Printf("BATCH: batchsize %d row rate %f/sec (took %v)\n", batchSize, float64(batchSize)/float64(took.Seconds()), took)
			}
		}
	}
	batches.m = map[string][]*insertData{}
	batches.cnt = 0
	return metricCnt, uint64(rowCnt)
}

// tags,name=truck_0,fleet=South,driver=Trish,model=H-2,device_version=v2.3,load_capacity=1500,fuel_capacity=150,nominal_fuel_consumption=12
// diagnostics,1640995200000 000000,1,,0
// INSERT INTO diagnostics USING diagnostics_super TAGS
// ("truck_1", "South", "Albert", "F-150", "v1.5",2000,200,15) VALUES (now, 11.2, 12.19,1);

// insert into root.ln.wf02.wt02(time,s5) values(1,true)
func (p *processor) processCSI(hypertable string, rows []*insertData) uint64 {
	colLen := len(tableCols[hypertable])
	tagRows, dataRows, numMetrics := p.splitTagsAndMetrics(rows, colLen)

	sqls := make([]string, len(rows))
	tagVals := p.insertTags(tagRows)
	for i, tagvals := range tagVals {
		fields := strings.Join(tableCols[hypertable], ",") + "," + strings.Join(tableCols[tagsKey], ",")

		sql := fmt.Sprintf("insert into root.%s.%s (timestamp, %s) values (%s,%s)",
			p.dbName, hypertable, fields, dataRows[i], tagvals)

		sqls[i] = sql

		//fmt.Printf("===%s\n", sql)
	}

	p.session.ExecuteBatchStatement(sqls)

	return numMetrics
}

func (p *processor) insertTags(tagRows [][]string) []string {
	tagCols := tableCols[tagsKey]
	values := make([]string, 0)
	commonTagsLen := len(tagCols)

	for _, val := range tagRows {
		sqlValues := convertValsToBasedOnType(val[:commonTagsLen], p.opts.TagColumnTypes[:commonTagsLen], "'", "NULL")

		values = append(values, strings.Join(sqlValues, ","))
	}

	return values
}

func (p *processor) splitTagsAndMetrics(rows []*insertData, dataCols int) ([][]string, []string, uint64) {
	tagRows := make([][]string, 0, len(rows))
	dataRows := make([]string, 0, len(rows))
	numMetrics := uint64(0)
	commonTagsLen := len(tableCols[tagsKey])

	for _, data := range rows {
		tags := strings.SplitN(data.tags, ",", commonTagsLen+1)
		for i := 0; i < commonTagsLen; i++ {
			tags[i] = strings.Split(tags[i], "=")[1]
		}

		metrics := strings.Split(data.fields, ",")
		numMetrics += uint64(len(metrics) - 1) // 1 field is timestamp

		timeInt, _ := strconv.ParseInt(metrics[0], 10, 64)
		metrics[0] = strconv.FormatInt(timeInt/1000000, 10)

		dataRows = append(dataRows, strings.Join(metrics, ","))
		tagRows = append(tagRows, tags[:commonTagsLen])
	}

	return tagRows, dataRows, numMetrics
}

func convertValsToBasedOnType(values []string, types []string, quotemark string, null string) []string {
	sqlVals := make([]string, len(values))
	for i, val := range values {
		if val == "" {
			sqlVals[i] = null
			continue
		}

		switch types[i] {
		case "string":
			sqlVals[i] = quotemark + val + quotemark
		default:
			sqlVals[i] = val
		}
	}

	return sqlVals
}
