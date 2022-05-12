package tdengine

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cnosdb/tsdb-comparisons/pkg/targets"
)

type insertData struct {
	tags   string
	fields string
}

type processor struct {
	opts   *LoadingOptions
	dbName string

	client  *http.Client
	httpurl string
}

func newProcessor(opts *LoadingOptions, dbName string) *processor {
	return &processor{
		opts:    opts,
		dbName:  dbName,
		httpurl: opts.HttpURL(),
	}
}

func (p *processor) Init(_ int, doLoad, hashWorkers bool) {
	tr := &http.Transport{
		MaxIdleConns:        128,
		MaxIdleConnsPerHost: 128,
		MaxConnsPerHost:     1024 * 4,
		IdleConnTimeout:     time.Second * 60,
	}

	p.client = &http.Client{
		Transport: tr,
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
func (p *processor) processCSI(hypertable string, rows []*insertData) uint64 {
	colLen := len(tableCols[hypertable])
	tagRows, dataRows, numMetrics := p.splitTagsAndMetrics(rows, colLen)

	httpbody := "INSERT INTO "
	tagVals := p.insertTags(tagRows)
	for i, str := range tagVals {
		tablname := "t_" + md5Str(str)[0:10]
		httpbody += fmt.Sprintf("%s USING %s TAGS (%s) VALUES (%s) ", tablname, hypertable, str, dataRows[i])
	}
	httpClientExecSQL(p.client, p.httpurl, httpbody, p.opts.User, p.opts.Pass)

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

func md5Str(str string) string {
	h := md5.New()
	h.Write([]byte(str))
	return hex.EncodeToString(h.Sum(nil))
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
