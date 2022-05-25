package iotdb

import (
	"container/list"
	"fmt"
	"strconv"
	"strings"
	"sync"
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

	pool *sessionPool
}

type sessionPool struct {
	lock   sync.Locker
	pool   *list.List
	config *client.Config
}

func (p *sessionPool) Get() *client.Session {
	p.lock.Lock()
	defer p.lock.Unlock()

	elm := p.pool.Front()
	if elm != nil {
		p.pool.Remove(elm)

		return elm.Value.(*client.Session)
	}

	session := client.NewSession(p.config)
	if err := session.Open(false, 0); err != nil {
		fmt.Printf("Connect to iotdb %+v failed %v\n", p.config, err)
		panic("")
	}

	return session
}

func (p *sessionPool) Put(client *client.Session) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.pool.PushBack(client)
}

func newProcessor(opts *LoadingOptions, dbName string) *processor {
	return &processor{
		opts:   opts,
		dbName: dbName,
	}
}

func (p *processor) Init(_ int, doLoad, hashWorkers bool) {
	p.pool = &sessionPool{
		config: &client.Config{
			Host:     p.opts.Host,
			Port:     p.opts.Port,
			UserName: p.opts.User,
			Password: p.opts.Pass},

		pool: list.New(),
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
	for i, tagvals := range tagRows {
		sql := fmt.Sprintf("insert into root.%s.%s.%s (timestamp, %s) values (%s)",
			p.dbName, hypertable, strings.Join(tagvals, "."),
			strings.Join(tableCols[hypertable], ","), dataRows[i])

		sqls[i] = sql

		//fmt.Printf("===%s\n", sql)
	}

	session := p.pool.Get()
	session.ExecuteBatchStatement(sqls)
	p.pool.Put(session)

	return numMetrics
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
