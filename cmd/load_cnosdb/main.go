// bulk_load_cnosdb loads an CnosDB daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/blagojts/viper"
	"github.com/cnosdb/tsdb-comparisons/internal/utils"
	"github.com/cnosdb/tsdb-comparisons/load"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets/constants"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets/initializers"
	"github.com/spf13/pflag"
)

// Program option vars:
var (
	daemonURLs        []string
	replicationFactor int
	backoff           time.Duration
	useGzip           bool
	doAbortOnExist    bool
	consistency       string
	basicAuth         string
)

// Global vars
var (
	loader  load.BenchmarkRunner
	config  load.BenchmarkRunnerConfig
	bufPool sync.Pool
	target  targets.ImplementedTarget
)

var consistencyChoices = map[string]struct{}{
	"any":    {},
	"one":    {},
	"quorum": {},
	"all":    {},
}

// allows for testing
var fatal = log.Fatalf

// Parse args:
func init() {
	target = initializers.GetTarget(constants.FormatCnosDB)
	config = load.BenchmarkRunnerConfig{}
	config.AddToFlagSet(pflag.CommandLine)
	pflag.CommandLine.String("username", "root", "Basic access authentication username")
	pflag.CommandLine.String("password", "", "Basic access authentication password")

	target.TargetSpecificFlags("", pflag.CommandLine)
	var csvDaemonURLs string

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	csvDaemonURLs = viper.GetString("urls")
	replicationFactor = viper.GetInt("replication-factor")
	consistency = viper.GetString("consistency")
	backoff = viper.GetDuration("backoff")
	useGzip = viper.GetBool("gzip")

	if _, ok := consistencyChoices[consistency]; !ok {
		log.Fatalf("invalid consistency settings")
	}

	daemonURLs = strings.Split(csvDaemonURLs, ",")
	if len(daemonURLs) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	config.HashWorkers = false
	loader = load.GetBenchmarkRunner(config)
}

type benchmark struct{}

func (b *benchmark) GetDataSource() targets.DataSource {
	return &fileDataSource{scanner: bufio.NewScanner(load.GetBufferedReader(config.FileName))}
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(_ uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{}
}

func main() {
	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	username := viper.GetString("username")
	password := viper.GetString("password")
	if username != "" || password != "" {
		basicAuth = "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
	}

	loader.RunBenchmark(&benchmark{})
}
