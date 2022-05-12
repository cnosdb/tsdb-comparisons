package main

import (
	"database/sql/driver"
	"fmt"
	"github.com/blagojts/viper"
	"github.com/cnosdb/tsdb-comparisons/internal/utils"
	"github.com/cnosdb/tsdb-comparisons/pkg/query"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/taosdata/driver-go/v2/af"
	"io"
	"time"
)

var (
	runner *query.BenchmarkRunner
)

// Program option vars:
var (
	host     string
	user     string
	password string
	port     int
)

type processor struct {
	conn          *af.Connector
	debug         bool
	printResponse bool
}

func (p *processor) Init(workerNum int) {
	var err error
	db := runner.DatabaseName()
	p.conn, err = af.Open(host, user, password, db, port)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"user":     user,
			"password": password,
			"host":     host,
			"port":     port,
			"db":       db,
		}).Fatal(err)
	}
	p.debug = runner.DebugLevel() > 0
	p.printResponse = runner.DoPrintResponses()
}

func (p *processor) ProcessQuery(q query.Query, isWarm bool) ([]*query.Stat, error) {
	tq := q.(*query.TDengine)
	start := time.Now()
	qry := string(tq.SqlQuery)
	if p.debug {
		logrus.Debug(qry)
	}
	rows, err := p.conn.Query(qry)
	if err != nil {
		logrus.WithField("query", qry).Debug(err)
		return nil, err
	}
	cols := rows.Columns()
	length := len(cols)
	cache := make([]driver.Value, length)
	var rowsPrint [][]driver.Value
	if p.printResponse {
		for {
			err = rows.Next(cache)
			if err == io.EOF {
				break
			}
			temp := make([]driver.Value, length)
			copy(temp, cache)
			rowsPrint = append(rowsPrint, temp)
		}
		logrus.WithFields(logrus.Fields{
			"columns": cols,
			"rows":    rowsPrint,
		}).Info()
	} else {
		for {
			err = rows.Next(cache)
			if err == io.EOF {
				break
			}
		}
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	took := float64(time.Since(start).Nanoseconds()) / 1e6
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took)

	return []*query.Stat{stat}, nil
}

func init() {
	logrus.SetLevel(logrus.DebugLevel)

	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)
	pflag.String("host", "localhost", "TDengine host")
	pflag.String("user", "root", "User for login into TDengine")
	pflag.String("password", "taosdata", "Password for login to TDengine")
	pflag.Int("port", 6030, "TCP port for login to TDengine")
	pflag.Parse()

	err := utils.SetupConfigFile()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	if err = viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	host = viper.GetString("host")
	user = viper.GetString("user")
	password = viper.GetString("password")
	port = viper.GetInt("port")

	runner = query.NewBenchmarkRunner(config)
}

func main() {
	runner.Run(&query.TDenginePool, func() query.Processor {
		return &processor{}
	})
}
