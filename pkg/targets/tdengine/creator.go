package tdengine

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/cnosdb/tsdb-comparisons/pkg/targets"

	_ "github.com/jackc/pgx/v4/stdlib"
)

const (
	tagsKey      = "tags"
	TimeValueIdx = "TIME-VALUE"
	ValueTimeIdx = "VALUE-TIME"
)

// allows for testing
var fatal = log.Fatalf

var tableCols = make(map[string][]string)

type LoadingOptions struct {
	PostgresConnect string `yaml:"postgres" mapstructure:"postgres"`
	Host            string `yaml:"host"`
	User            string
	Pass            string
	Port            string
	ConnDB          string `yaml:"admin-db-name" mapstructure:"admin-db-name"`

	UseHypertable bool `yaml:"use-hypertable" mapstructure:"use-hypertable"`
	LogBatches    bool `yaml:"log-batches" mapstructure:"log-batches"`
	UseJSON       bool `yaml:"use-jsonb-tags" mapstructure:"use-jsonb-tags"`
	InTableTag    bool `yaml:"in-table-partition-tag" mapstructure:"in-table-partition-tag"`

	NumberPartitions  int           `yaml:"partitions" mapstructure:"partitions"`
	PartitionColumn   string        `yaml:"partition-column" mapstructure:"partition-column"`
	ReplicationFactor int           `yaml:"replication-factor" mapstructure:"replication-factor"`
	ChunkTime         time.Duration `yaml:"chunk-time" mapstructure:"chunk-time"`

	TimeIndex          bool   `yaml:"time-index" mapstructure:"time-index"`
	TimePartitionIndex bool   `yaml:"time-partition-index" mapstructure:"time-partition-index"`
	PartitionIndex     bool   `yaml:"partition-index" mapstructure:"partition-index"`
	FieldIndex         string `yaml:"field-index" mapstructure:"field-index"`
	FieldIndexCount    int    `yaml:"field-index-count" mapstructure:"field-index-count"`

	ProfileFile          string `yaml:"write-profile" mapstructure:"write-profile"`
	ReplicationStatsFile string `yaml:"write-replication-stats" mapstructure:"write-replication-stats"`

	CreateMetricsTable bool     `yaml:"create-metrics-table" mapstructure:"create-metrics-table"`
	ForceTextFormat    bool     `yaml:"force-text-format" mapstructure:"force-text-format"`
	TagColumnTypes     []string `yaml:",omitempty" mapstructure:",omitempty"`
	UseInsert          bool     `yaml:"use-insert" mapstructure:"use-insert"`
}

func (opts *LoadingOptions) HttpURL() string {
	return fmt.Sprintf("http://%s:%s/rest/sql/%s", opts.Host, opts.Port, opts.ConnDB)
}

type dbCreator struct {
	ds      targets.DataSource
	httpurl string
	connDB  string
	opts    *LoadingOptions
}

func (d *dbCreator) Init() {
	d.ds.Headers()
}

func (d *dbCreator) DBExists(dbName string) bool {
	return true
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	client := &http.Client{}

	httpClientExecSQL(client, d.httpurl, "DROP DATABASE "+dbName)

	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	client := &http.Client{}

	httpClientExecSQL(client, d.httpurl, "CREATE DATABASE "+dbName)

	return nil
}

func (d *dbCreator) PostCreateDB(dbName string) error {
	headers := d.ds.Headers()
	tagNames := headers.TagKeys
	tagTypes := headers.TagTypes

	// tableCols is a global map. Globally cache the available tags
	tableCols[tagsKey] = tagNames
	// tagTypes holds the type of each tag value (as strings from Go types (string, float32...))
	d.opts.TagColumnTypes = tagTypes

	client := &http.Client{}
	for tableName, columns := range headers.FieldKeys {
		// tableCols is a global map. Globally cache the available columns for the given table
		tableCols[tableName] = columns

		httpClientExecSQL(client, d.httpurl, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

		createSql := fmt.Sprintf("CREATE STABLE %s (ts TIMESTAMP, %s) TAGS (%s)",
			tableName, generateFieldsStr(columns), generateTagsStr(tagNames, tagTypes))
		httpClientExecSQL(client, d.httpurl, createSql)

	}
	return nil
}

// CREATE STABLE diagnostics(ts TIMESTAMP, fuel_state FLOAT, current_load FLOAT, status FLOAT)
// TAGS(name BINARY(64), fleet BINARY(64),driver BINARY(64),model BINARY(64),device_version BINARY(64),load_capacity FLOAT,fuel_capacity FLOAT,nominal_fuel_consumption FLOAT);
func generateTagsStr(tagNames, tagTypes []string) string {
	tagColumnDefinitions := make([]string, len(tagNames))
	for i, tagName := range tagNames {
		tagType := serializedTypeToPgType(tagTypes[i])
		tagColumnDefinitions[i] = fmt.Sprintf("%s %s", tagName, tagType)
	}

	return strings.Join(tagColumnDefinitions, ", ")
}

func generateFieldsStr(filedNames []string) string {
	cols := make([]string, len(filedNames))
	for i, tagName := range filedNames {
		cols[i] = tagName + " FLOAT"
	}

	return strings.Join(cols, ", ")
}

func serializedTypeToPgType(serializedType string) string {
	switch serializedType {
	case "string":
		return "BINARY(128)"
	case "float32":
		return "FLOAT"
	default:
		panic(fmt.Sprintf("unrecognized type %s", serializedType))
	}
}

func httpClientExecSQL(client *http.Client, url, sqlcmd string) error {
	body := strings.NewReader(sqlcmd)
	req, _ := http.NewRequest("POST", url, body)
	req.SetBasicAuth("root", "taosdata")
	resp, err := client.Do(req)

	if err != nil {
		fmt.Println(err)
		return err
	}

	fmt.Printf("URL: %s ### SQL: %s\n\n", url, sqlcmd)

	defer resp.Body.Close()

	return nil
}
