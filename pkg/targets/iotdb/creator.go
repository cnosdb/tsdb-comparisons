package iotdb

import (
	"fmt"
	"log"

	"github.com/apache/iotdb-client-go/client"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets"

	_ "github.com/jackc/pgx/v4/stdlib"
)

const (
	tagsKey = "tags"
)

// allows for testing
var fatal = log.Fatalf

var tableCols = make(map[string][]string)

type LoadingOptions struct {
	Host       string `yaml:"host"`
	User       string
	Pass       string
	Port       string
	ConnDB     string `yaml:"admin-db-name" mapstructure:"admin-db-name"`
	LogBatches bool   `yaml:"log-batches" mapstructure:"log-batches"`

	ProfileFile    string   `yaml:"write-profile" mapstructure:"write-profile"`
	TagColumnTypes []string `yaml:",omitempty" mapstructure:",omitempty"`
}

type dbCreator struct {
	ds   targets.DataSource
	opts *LoadingOptions

	session client.Session
}

func (d *dbCreator) Init() {
	d.ds.Headers()

	config := &client.Config{
		Host:     d.opts.Host,
		Port:     d.opts.Port,
		UserName: d.opts.User,
		Password: d.opts.Pass,
	}
	d.session = client.NewSession(config)
	if err := d.session.Open(false, 0); err != nil {
		fmt.Printf("Connect to iotdb %+v failed %v\n", config, err)
		panic("")
	}
}

func (d *dbCreator) DBExists(dbName string) bool {
	return true
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	_, err := d.session.DeleteStorageGroup("root." + dbName)

	return err
}

func (d *dbCreator) CreateDB(dbName string) error {
	_, err := d.session.SetStorageGroup("root." + dbName)

	return err
}

func (d *dbCreator) PostCreateDB(dbName string) error {
	headers := d.ds.Headers()
	tagNames := headers.TagKeys
	tagTypes := headers.TagTypes

	// tableCols is a global map. Globally cache the available tags
	tableCols[tagsKey] = tagNames
	// tagTypes holds the type of each tag value (as strings from Go types (string, float32...))
	d.opts.TagColumnTypes = tagTypes

	for tableName, columns := range headers.FieldKeys {
		// tableCols is a global map. Globally cache the available columns for the given table
		tableCols[tableName] = columns

		path := "root." + dbName + "." + tableName
		d.session.DeleteTimeseries([]string{path})
		//fmt.Printf("=== path %s\n", path)

		for i, name := range tagNames {
			dataType := client.TEXT
			encoding := client.PLAIN
			compressor := client.SNAPPY
			if tagTypes[i] == "float32" {
				dataType = client.FLOAT
				encoding = client.GORILLA
			}
			d.session.CreateTimeseries(path+"."+name, dataType, encoding, compressor, nil, nil)
		}

		for _, name := range columns {
			dataType := client.FLOAT
			encoding := client.GORILLA
			compressor := client.SNAPPY
			d.session.CreateTimeseries(path+"."+name, dataType, encoding, compressor, nil, nil)
		}
	}
	return nil
}
