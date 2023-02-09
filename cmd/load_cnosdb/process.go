package main

import (
	"fmt"
	"time"

	"github.com/cnosdb/tsdb-comparisons/pkg/targets"
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

			_, err = p.httpWriter.WriteLineProtocol(batch.buf.Bytes(), false)

			if err == errBackoff {
				p.backingOffChan <- true
				time.Sleep(backoff)
			} else {
				p.backingOffChan <- false
				break
			}
		}
		if err != nil {
			fmt.Printf("Error writing: %s\n", err.Error())
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
