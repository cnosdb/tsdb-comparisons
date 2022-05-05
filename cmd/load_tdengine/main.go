package main

import (
	"fmt"
	"github.com/cnosdb/tsdb-comparisons/load"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets"
	"github.com/taosdata/driver-go/v2/af"
	"sync"
	"time"

	_ "github.com/taosdata/driver-go/v2/taosRestful"
)

// Program option vars:
var (
	daemonURLs        []string
	replicationFactor int
	backoff           time.Duration
	useGzip           bool
	doAbortOnExist    bool
	conn              af.Connector
	password          string
	username          string
	port              int
)

// Global vars
var (
	loader  load.BenchmarkRunner
	config  load.BenchmarkRunnerConfig
	bufPool sync.Pool
	target  targets.ImplementedTarget
)

func test() {
	conn, err := af.Open("localhost", "root", "taosdata", "", 6030)
	if err != nil {
		fmt.Println("fail to connect, err:", err)
	}
	defer conn.Close()
	prepareDatabase(conn)
	var lines = []string{
		"meters,location=Beijing.Haidian,groupid=2 current=11.8,voltage=221,phase=0.28 1648432611249",
		"meters,location=Beijing.Haidian,groupid=2 current=13.4,voltage=223,phase=0.29 1648432611250",
		"meters,location=Beijing.Haidian,groupid=3 current=10.8,voltage=223,phase=0.29 1648432611249",
		"meters,location=Beijing.Haidian,groupid=3 current=11.3,voltage=221,phase=0.35 1648432611250",
	}

	err = conn.InfluxDBInsertLines(lines, "ms")
	fmt.Println("insert error:", err)
}

func prepareDatabase(conn *af.Connector) {
	_, err := conn.Exec("CREATE DATABASE test")
	if err != nil {
		panic(err)
	}
	_, err = conn.Exec("USE test")
	if err != nil {
		panic(err)
	}
}

func main() {
	conn, err := af.Open("localhost", "root", "taosdata", "", 6030)
	if err != nil {
		fmt.Println("fail to connect, err:", err)
	}
	defer conn.Close()
	conn.Exec("show databases;")
}
