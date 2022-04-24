# TSDB-COMPARISIONS
本仓库中包含几个时序数据库的基准测试代码，包括TimescaleDB，InfluxDB以及CnosDB。这些代码基于[TSBS](https://github.com/timescale/tsbs)构造。

目前支持的数据库包括:

+ CnosDB[(相关文档)](docs/cnosdb.md)

+ InfluxDB [(相关文档)](docs/influx.md)

+ TimescaleDB [(相关文档)](docs/timescaledb.md)


## 概述

**tsdb-comparisons**是一组Go程序，用于生成数据集，然后对各种数据库的读写性能进行基准测试。其目的是使tsdb-comparisons具有可扩展性，这样各种用例(例如，物联网、金融等)、查询类型和数据库都可以被包括进来并进行基准测试。为此，我们希望帮助未来的数据库管理员找到适合他们的需求和工作负载的最佳数据库。此外，如果你是一个时间序列数据库的开发人员，并希望在tsdb-comparisons中包括你的数据库，请随意打开一个pull request来添加它!


## 当前用例

目前。tsdb-comparisons支持物联网这个用例

### 物联网 (IoT)
旨在模拟物联网环境中的数据加载。这个用例模拟来自一组属于一个虚构的卡车公司的卡车的数据流。此用例模拟来自每个卡车的诊断数据和指标，并引入环境因素，如无序数据和批处理摄入(针对离线一段时间的卡车)。它还跟踪卡车元数据，并使用该元数据将指标和诊断作为查询集的一部分联系在一起。

作为该用例的一部分，生成的查询将包括实时卡车状态和分析，后者将查看时间序列数据，以更好地预测卡车的行为。这个用例的比例因子将基于被跟踪的卡车的数量。

## TSBS-COMPARISONS测试了什么

TSBS-COMPARISONS用于对批量负载性能和查询执行性能进行基准测试。(它目前不测量并发插入和查询的性能，这是未来的优先级。)为了以公平的方式实现这一点，要插入的数据和要运行的查询是预先生成的，本地的Go客户端被尽可能地使用来连接到每个数据库。

虽然数据是随机生成的，但TSBS数据和查询是完全确定的。通过向生成程序提供相同的PRNG(伪随机数生成器)种子，每个数据库都装载了相同的数据，并使用相同的查询进行查询。

## 安装

TSBS-COMPARISONS是Go程序的集合(带有一些辅助的bash和Python脚本)。获取和安装Go程序最简单的方法是使用`go get`指令，然后使用`make all`来安装所有二进制文件:
```bash
# Fetch TSBS and its dependencies
$ go get github.com/cnosdb/tsbs-comparisons
$ cd $GOPATH/src/github.com/cnosdb/tsbs-comparisons
$ make
```

## 怎样使用TSBS-COMPARISONS

使用TSBS进行基准测试涉及3个阶段:数据和查询生成、数据加载/插入和查询执行。

### 数据和查询生成

为了使基准测试结果不受动态生成数据或查询的影响，使用TSDB-COMPARISONS可以首先生成希望进行基准测试的数据和查询，然后可以(重新)使用它作为基准测试阶段的输入。

#### 数据生成

所需变量:
1. 一个用例. 即： `iot` 
1. 确定性生成的PRNG种子. 例如： `123`
1. 要生成的设备数量。例如： `4000`
1. 开始时间的时间戳。例如： `2016-01-01T00:00:00Z`
1. 结束时间的时间戳。例如： `2016-01-04T00:00:00Z`
1. 每台设备每次读数之间的间隔时间，以秒为单位。例如： `10s`
1. 要为哪一个数据库生成。例如： `timescaledb`
 (从`influx`、`timescaledb` 以及 `cnosdb`中选择)

根据以上步骤，您现在可以生成一个数据集(或多个数据集，如果您选择为多个数据库生成)，可以使用`tsbs_generate_data`工具对所选数据库的数据加载进行基准测试
```bash
$ tsbs_generate_data --use-case="iot" --seed=123 --scale=4000 \
    --timestamp-start="2016-01-01T00:00:00Z" \
    --timestamp-end="2016-01-04T00:00:00Z" \
    --log-interval="10s" --format="timescaledb" \
    | gzip > /tmp/timescaledb-data.gz

# Each additional database would be a separate call.
```
注意:我们通过管道将输出输出到gzip以减少磁盘空间。这也要求您在运行测试时通过gunzip管道。

上面的示例将生成一个伪csv文件，可用于将数据批量加载到TimescaleDB中。每个数据库都有自己的存储数据的格式，以便其相应的加载器更容易地写入数据。上面的配置将生成100多行(1B度量)，这通常是一个很好的起点。将时间周期增加一天将增加约33M行，因此，例如，30天将产生10亿行(10B指标)

##### IoT用例

IoT用例生成的数据可能包含无序、缺失或空的条目，以便更好地表示与用例相关的真实场景。使用指定的种子意味着我们可以以确定性和可重现的方式进行多次数据生成。

#### 查询生成

所需变量：
1. 与数据生成中使用的用例、种子、设备的#和开始时间相同。
1. 数据生成结束一秒后的时间。 例如： 对于`2016-01-04T00:00:00Z`来说，使用`2016-01-04T00:00:01Z`
1. 要生成的查询数量。 例如： `1000`
1. 以及您想要查询的类型。 例如： `single-groupby-1-1-1` or `last-loc`

对于最后一步，有许多查询方式可供选择。这些查询方式在附录中。此外，文件`scripts/generate_queries.sh`包含所有这些查询的列表，作为环境变量`QUERY_TYPES`的默认值。如果您正在生成多种类型的查询，我们建议您使用helper脚本。

对于给定类型只生成一组查询:
```bash
$ tsbs_generate_queries --use-case="iot" --seed=123 --scale=4000 \
    --timestamp-start="2016-01-01T00:00:00Z" \
    --timestamp-end="2016-01-04T00:00:01Z" \
    --queries=1000 --query-type="breakdown-frequency" --format="timescaledb" \
    | gzip > /tmp/timescaledb-queries-breakdown-frequency.gz
```

_注意:我们通过管道将输出输出到gzip以减少磁盘空间。这也要求您在运行测试时通过gunzip管道。_

为多种类型生成查询集
```bash
$ FORMATS="timescaledb" SCALE=4000 SEED=123 \
    TS_START="2016-01-01T00:00:00Z" \
    TS_END="2016-01-04T00:00:01Z" \
    QUERIES=1000 QUERY_TYPES="last-loc low-fuel avg-load" \
    BULK_DATA_DIR="/tmp/bulk_queries" scripts/generate_queries.sh
```

查询类型的完整列表可以在文档末尾的附录I中找到。

### 基准测试插入/写性能

TSDB-COMPARISONS有两种方法来测试插入/写入性能:
* 用`tsbs_load`指令动态模拟并载入。
* 预先生成数据或文件并将其用`tsbs_load`载入或者用`tsbs_load_*`指令载入特定数据库。

#### 使用统一的可执行指令 `tsbs_load`

`tsbs_load`指令可以将数据载入任何所支持的数据库。
可以使用一个已经生成的数据文件作为输入，或者动态模拟数据。

首先，生成一个配置yaml文件，其中有每个属性的默认值:
```shell script
$ tsbs_load config --target=<db-name> --data-source=[FILE|SIMULATOR]
```
例如，为TimescaleDB生成一个示例，从文件加载数据
```shell script
$ tsbs_load config --target=timescaledb --data-source=FILE
Wrote example config to: ./config.yaml
```

然后你可以运行tsbs_load和生成的配置文件:
```shell script
$ tsbs_load load timescaledb --config=./config.yaml
```

#### 使用数据库特定的`tsbs_load_*`可执行文件

TSBS measures insert/write performance by taking the data generated in
the previous step and using it as input to a database-specific command
line program. To the extent that insert programs can be shared, we have
made an effort to do that (e.g., the TimescaleDB loader can
be used with a regular PostgreSQL database if desired). Each loader does
share some common flags -- e.g., batch size (number of readings inserted
together), workers (number of concurrently inserting clients), connection
details (host & ports), etc -- but they also have database-specific tuning
flags. To find the flags for a particular database, use the `-help` flag
(e.g., `tsbs_load_timescaledb -help`).

TSDB-COMPARISONS通过获取上一步生成的数据并将其作为特定于数据库的命令行程序的输入来测量插入/写入性能。使用`-help`查看更多详细信息(例如：`tsbs_load_timescaledb -help`)。

下面是一个将数据加载到需要SSL的远程timeescaledb实例的示例，使用上面的指令创建的gzip数据集:

```bash
cat /tmp/timescaledb-data.gz | gunzip | tsbs_load_timescaledb \
--postgres="sslmode=require" --host="my.tsdb.host" --port=5432 --pass="password" \
--user="benchmarkuser" --admin-db-name=defaultdb --workers=8  \
--in-table-partition-tag=true --chunk-time=8h --write-profile= \
--field-index-count=1 --do-create-db=true --force-text-format=false \
--do-abort-on-exist=false
```

为了更简单的测试，特别是本地测试，我们还提供了`scripts/load/load_<database>.sh`，并为一些数据库设置了合理的默认标志。因此，要加载到TimescaleDB，请确保TimescaleDB正在运行，然后使用:
```bash
# Will insert using 2 clients, batch sizes of 10k, from a file
# named `timescaledb-data.gz` in directory `/tmp`
$ NUM_WORKERS=2 BATCH_SIZE=10000 BULK_DATA_DIR=/tmp \
    scripts/load/load_timescaledb.sh
```

这将创建一个名为`benchmark`的新数据库，数据存储在其中。它将覆盖数据库，如果它存在;如果您不希望发生这种情况，请为上述命令提供一个不同的`DATABASE_NAME`。

使用`load_timeescaledb .sh`写入远程主机的示例:
```bash
# Will insert using 2 clients, batch sizes of 10k, from a file
# named `timescaledb-data.gz` in directory `/tmp`
$ NUM_WORKERS=2 BATCH_SIZE=10000 BULK_DATA_DIR=/tmp DATABASE_HOST=remotehostname
DATABASE_USER=user DATABASE \
    scripts/load/load_timescaledb.sh
```

---

默认情况下，关于加载性能的统计信息每10秒打印一次，当加载完整的数据集时，看起来是这样的:
```text
time,per. metric/s,metric total,overall metric/s,per. row/s,row total,overall row/s
# ...
1518741528,914996.143291,9.652000E+08,1096817.886674,91499.614329,9.652000E+07,109681.788667
1518741548,1345006.018902,9.921000E+08,1102333.152918,134500.601890,9.921000E+07,110233.315292
1518741568,1149999.844750,1.015100E+09,1103369.385320,114999.984475,1.015100E+08,110336.938532

Summary:
loaded 1036800000 metrics in 936.525765sec with 8 workers (mean rate 1107070.449780/sec)
loaded 103680000 rows in 936.525765sec with 8 workers (mean rate 110707.044978/sec)
```

除了最后两行以外的所有行都包含CSV格式的数据，在标题中包含列名。这些列名对应于:
* 时间戳
* 每秒的指标值
* 总插入指标
* 每秒的总指标值
* 周期内每秒的行数
* 总行数
* 每秒总行数

最后两行总结了插入了多少指标(以及适用的行)、所花费的时间以及插入的平均速率。

### 查询执行性能的基准测试

要测量TSBS中的查询执行性能，首先需要使用前面的部分加载数据，并像前面描述的那样生成查询。一旦数据加载并生成查询，只需使用测试数据库对应的生成的二进制文件`tsbs_run_queries_` :
```bash
$ cat /tmp/queries/timescaledb-cpu-max-all-eight-hosts-queries.gz | \
    gunzip | tsbs_run_queries_timescaledb --workers=8 \
        --postgres="host=localhost user=postgres sslmode=disable"
```

您可以更改flag`--workers`的值，以控制同时运行的并行查询的级别。结果输出看起来像这样:
```text
run complete after 1000 queries with 8 workers:
TimescaleDB max cpu all fields, rand    8 hosts, rand 12hr by 1h:
min:    51.97ms, med:   757.55, mean:  2527.98ms, max: 28188.20ms, stddev:  2843.35ms, sum: 5056.0sec, count: 2000
all queries                                                     :
min:    51.97ms, med:   757.55, mean:  2527.98ms, max: 28188.20ms, stddev:  2843.35ms, sum: 5056.0sec, count: 2000
wall clock time: 633.936415sec
```

输出为您提供了查询的描述和多个measurement分组(根据数据库的不同可能有所不同)。

---

为了更容易地测试多个查询，我们提供了`scripts/generate_run_script.py`，它创建一个bash脚本，其中包含在一行中运行多个查询类型的命令。它生成的查询应该放在一个文件中，每行有一个查询，并为脚本提供路径。例如，如果你有一个名为queries.txt的文件，看起来像这样:

```text
last-loc
avg-load
high-load
long-driving-session
```

你可以生成一个名为`query_test.sh`的运行脚本:
```bash
# Generate run script for TimescaleDB, using queries in `queries.txt`
# with the generated query files in /tmp/queries for 8 workers
$ python generate_run_script.py -d timescaledb -o /tmp/queries \
    -w 8 -f queries.txt > query_test.sh
```

生成的脚本文件如下所示:
```bash
#!/bin/bash
# Queries
cat /tmp/queries/timescaledb-last-loc-queries.gz | gunzip | query_benchmarker_timescaledb --workers=8 --limit=1000 --hosts="localhost" --postgres="user=postgres sslmode=disable"  | tee query_timescaledb_timescaledb-last-loc-queries.out

cat /tmp/queries/timescaledb-avg-load-queries.gz | gunzip | query_benchmarker_timescaledb --workers=8 --limit=1000 --hosts="localhost" --postgres="user=postgres sslmode=disable"  | tee query_timescaledb_timescaledb-avg-load-queries.out

cat /tmp/queries/timescaledb-high-load-queries.gz | gunzip | query_benchmarker_timescaledb --workers=8 --limit=1000 --hosts="localhost" --postgres="user=postgres sslmode=disable"  | tee query_timescaledb_timescaledb-high-load-queries.out

cat /tmp/queries/timescaledb-long-driving-session-queries.gz | gunzip | query_benchmarker_timescaledb --workers=8 --limit=1000 --hosts="localhost" --postgres="user=postgres sslmode=disable"  | tee query_timescaledb_timescaledb-long-driving-session-queries.out
```

### 查询验证（可选）

此外，每个tsbs_run_queries_二进制文件都允许打印实际的查询结果，以便在不同的数据库之间比较结果是否相同。使用flag`-print-responses`将返回结果。

## Appendix I: Query types <a name="appendix-i-query-types"></a>

### Devops / cpu-only
|Query type|Description|
|:---|:---|
|single-groupby-1-1-1| Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 1 hour
|single-groupby-1-1-12| Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 12 hours
|single-groupby-1-8-1| Simple aggregrate (MAX) on one metric for 8 hosts, every 5 mins for 1 hour
|single-groupby-5-1-1| Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 1 hour
|single-groupby-5-1-12| Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 12 hours
|single-groupby-5-8-1| Simple aggregrate (MAX) on 5 metrics for 8 hosts, every 5 mins for 1 hour
|cpu-max-all-1| Aggregate across all CPU metrics per hour over 1 hour for a single host
|cpu-max-all-8| Aggregate across all CPU metrics per hour over 1 hour for eight hosts
|double-groupby-1| Aggregate on across both time and host, giving the average of 1 CPU metric per host per hour for 24 hours
|double-groupby-5| Aggregate on across both time and host, giving the average of 5 CPU metrics per host per hour for 24 hours
|double-groupby-all| Aggregate on across both time and host, giving the average of all (10) CPU metrics per host per hour for 24 hours
|high-cpu-all| All the readings where one metric is above a threshold across all hosts
|high-cpu-1| All the readings where one metric is above a threshold for a particular host
|lastpoint| The last reading for each host
|groupby-orderby-limit| The last 5 aggregate readings (across time) before a randomly chosen endpoint

### IoT
|Query type|Description|
|:---|:---|
|avg-vs-projected-fuel-consumption|Calculate average vs. projected fuel consumption per fleet
|avg-daily-driving-duration|Calculate average daily driving duration per driver
|avg-daily-driving-session|Calculate average daily driving session per driver
