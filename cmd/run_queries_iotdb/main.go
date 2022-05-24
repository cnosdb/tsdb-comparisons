package main

import (
	"fmt"
	"github.com/apache/iotdb-client-go/client"
	"github.com/blagojts/viper"
	"github.com/cnosdb/tsdb-comparisons/internal/utils"
	"github.com/cnosdb/tsdb-comparisons/pkg/query"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"strconv"
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
	session       *client.Session
	debug         bool
	printResponse bool
}

func (p *processor) Init(workerNum int) {
	var err error
	db := runner.DatabaseName()
	p.session = client.NewSession(&client.Config{
		Host:     host,
		Port:     strconv.Itoa(port),
		UserName: user,
		Password: password,
	})
	err = p.session.Open(false, 0)
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
	tq := q.(*query.IoTDB)
	start := time.Now()
	qry := string(tq.SqlQuery)
	if p.debug {
		logrus.Debug(qry)
	}
	dataSet, err := p.session.ExecuteQueryStatement(qry, 1000)
	if err != nil {
		logrus.WithField("query", qry).Debug(err)
		return nil, err
	}
	if p.printResponse {
		showTimestamp := !dataSet.IsIgnoreTimeStamp()
		if showTimestamp {
			fmt.Print("Time\t\t\t\t")
		}
		for i := 0; i < dataSet.GetColumnCount(); i++ {
			fmt.Printf("%s\t", dataSet.GetColumnName(i))
		}
		fmt.Println()

		for next, err := dataSet.Next(); err == nil && next; next, err = dataSet.Next() {
			if showTimestamp {
				fmt.Printf("%s\t", dataSet.GetText(client.TimestampColumnName))
			}
			for i := 0; i < dataSet.GetColumnCount(); i++ {
				columnName := dataSet.GetColumnName(i)
				v := dataSet.GetValue(columnName)
				if v == nil {
					v = "null"
				}
				fmt.Printf("%v\t\t", v)
			}
			fmt.Println()
		}
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
	pflag.String("host", "127.0.0.1", "iotdb host")
	pflag.String("user", "root", "User for login into iotdb")
	pflag.String("password", "root", "Password for login to iotdb")
	pflag.Int("port", 6667, "TCP port for login to iotdb")
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
	runner.Run(&query.IotDBPool, func() query.Processor {
		return &processor{}
	})
}
