spark-lineage
=============

Spark SQL listener to report lineage data to a variety of outputs, e.g. Amazon Kinesis. Heavily inspired by the
[Spark Atlas Connector](https://github.com/hortonworks-spark/spark-atlas-connector), but intended to be more generic to
help those who can't or won't use Atas.


Installation
------------

Build the jar:

```bash
$ ./gradlew shadowJar
```

Copy the output from `build/lib/jars` to your cluster.


Configuration
-------------

The listener is configured via a file, `lineage.properties`, that needs to be in the class path of the spark driver,
 e.g. by placing it in the spark config directory. Here's an example using the `KinesisReporter`:

```
org.chojin.spark.lineage.reporters=org.chojin.spark.lineage.reporters.KinesisReporter
org.chojin.spark.lineage.reporter.kinesis.stream=my-kinesis-stream
org.chojin.spark.lineage.reporter.kinesis.region=us-east-1
org.chojin.spark.lineage.reporter.kinesis.shard=0
org.chojin.spark.lineage.reporter.kinesis.compression=deflate
```

Then, configure `spark.sql.queryExecutionListeners` to include the `SparkSqlLineageListener`:

```bash
$ spark-shell \
  --jars file:///path/to/spark-lineage-0.0.1-SNAPSHOT.jar \
  --conf spark.sql.queryExecutionListeners=org.chojin.spark.lineage.SparkSqlLineageListener
```

Reporters
---------

One or more reporter classes can be specified as a comma-separated list in the parameter
`org.chojin.spark.lineage.reporters`.

### Kinesis

Class name: `org.chojin.spark.lineage.reporters.KinesisReporter`

Write an optionally compressed JSON blob to an AWS Kinesis stream.

#### Options

Options start with the prefix `org.chojin.spark.lineage.reporter.kinesis.`

- `stream` - stream name
- `region` - region (e.g. `us-east-2`)
- `shard` - shard/partition to which to write; multiple shards are not supported at this time
- `compression` - optional compression to apply to the JSON blob; any standard spark compression codec is supported,
  e.g. `gzip`, `deflate`

### DynamoDB

Class name: `org.chojin.spark.lineage.reporters.DynamodbReporter`

Write a structured record to an AWS DynamoDB table.

#### Options

Options start with the prefix `org.chojin.spark.lineage.reporter.dynamodb.`

- `table` - table name
- `region` - region (e.g. `us-east-2`)
