package main

import (
	"bytes"
	"fmt"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets"
	"github.com/valyala/fasthttp"
	"time"
)

const backingOffChanCap = 100

// allows for testing
var printFn = fmt.Printf

type processor struct {
	backingOffChan chan bool
	backingOffDone chan struct{}
	httpWriter     *HTTPWriter
}

func (p *processor) Init(numWorker int, _, _ bool) {
	daemonURL := daemonURLs[numWorker%len(daemonURLs)]
	cfg := HTTPWriterConfig{
		DebugInfo: fmt.Sprintf("worker #%d, dest url: %s", numWorker, daemonURL),
		Host:      daemonURL,
		Database:  loader.DatabaseName(),
		Auth:      config.Auth,
	}
	w := NewHTTPWriter(cfg, consistency)
	p.initWithHTTPWriter(numWorker, w)
}

func (p *processor) initWithHTTPWriter(numWorker int, w *HTTPWriter) {
	p.backingOffChan = make(chan bool, backingOffChanCap)
	p.backingOffDone = make(chan struct{})
	p.httpWriter = w
	go p.processBackoffMessages(numWorker)
}

func (p *processor) Close(_ bool) {
	close(p.backingOffChan)
	<-p.backingOffDone
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)

	// Write the batch: try until backoff is not needed.
	if doLoad {
		var err error
		for {
			if useGzip {
				compressedBatch := bufPool.Get().(*bytes.Buffer)
				fasthttp.WriteGzip(compressedBatch, batch.buf.Bytes())
				//file, e := os.OpenFile("text.txt", os.O_CREATE, 0755)
				//if e != nil {
				//	panic(fmt.Sprintf("open file error %s", e))
				//}
				//t := batch.buf.Bytes()
				//_, err := file.WriteString(*(*string)(unsafe.Pointer(&t)))
				//if err != nil {
				//	panic(fmt.Sprintf("open file error %s", e.Error()))
				//}
				//fmt.Println(*(*string)(unsafe.Pointer(&t)))
				points := parserLine(compressedBatch.Bytes())
				_, err = p.httpWriter.WriteLineProtocol(points, true)
				// Return the compressed batch buffer to the pool.
				compressedBatch.Reset()
				bufPool.Put(compressedBatch)
			} else {
				points := parserLine(batch.buf.Bytes())
				_, err = p.httpWriter.WriteLineProtocol(points, false)
			}

			if err == errBackoff {
				p.backingOffChan <- true
				time.Sleep(backoff)
			} else {
				p.backingOffChan <- false
				break
			}
		}
		if err != nil {
			fatal("Error writing: %s\n", err.Error())
		}
	}
	metricCnt := batch.metrics
	rowCnt := batch.rows

	// Return the batch buffer to the pool.
	batch.buf.Reset()
	bufPool.Put(batch.buf)
	return metricCnt, uint64(rowCnt)
}

func (p *processor) processBackoffMessages(workerID int) {
	var totalBackoffSecs float64
	var start time.Time
	last := false
	for this := range p.backingOffChan {
		if this && !last {
			start = time.Now()
			last = true
		} else if !this && last {
			took := time.Now().Sub(start)
			printFn("[worker %d] backoff took %.02fsec\n", workerID, took.Seconds())
			totalBackoffSecs += took.Seconds()
			last = false
			start = time.Now()
		}
	}
	printFn("[worker %d] backoffs took a total of %fsec of runtime\n", workerID, totalBackoffSecs)
	p.backingOffDone <- struct{}{}
}
