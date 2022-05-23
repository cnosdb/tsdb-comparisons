package iotdb

import (
	"github.com/cnosdb/tsdb-comparisons/internal/inputs"
	"github.com/cnosdb/tsdb-comparisons/pkg/data/source"
	"github.com/cnosdb/tsdb-comparisons/pkg/targets"
)

type benchmark struct {
	opts *LoadingOptions
	ds   targets.DataSource

	dbName string
}

func NewBenchmark(dbName string, opts *LoadingOptions, dataSourceConfig *source.DataSourceConfig) (targets.Benchmark, error) {
	var ds targets.DataSource
	if dataSourceConfig.Type == source.FileDataSourceType {
		ds = newFileDataSource(dataSourceConfig.File.Location)
	} else {
		dataGenerator := &inputs.DataGenerator{}
		simulator, err := dataGenerator.CreateSimulator(dataSourceConfig.Simulator)
		if err != nil {
			return nil, err
		}
		ds = newSimulationDataSource(simulator)
	}

	return &benchmark{
		opts:   opts,
		ds:     ds,
		dbName: dbName,
	}, nil
}

func (b *benchmark) GetDataSource() targets.DataSource {
	return b.ds
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	if maxPartitions > 1 {
		return &hostnameIndexer{partitions: maxPartitions}
	}
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return newProcessor(b.opts, b.dbName)
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{
		ds:      b.ds,
		opts:    b.opts,
		connDB:  b.opts.ConnDB,
		httpurl: b.opts.HttpURL(),
	}
}
