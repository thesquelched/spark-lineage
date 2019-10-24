spark-lineage
=============

Spark SQL listener to report lineage data to a variety of outputs, e.g. Amazon Kinesis.


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
org.chojin.spark.lineage.reporter=org.chojin.spark.lineage.reporters.KinesisReporter
org.chojin.spark.lineage.reporter.stream=my-kinesis-stream
org.chojin.spark.lineage.reporter.region=us-east-1
org.chojin.spark.lineage.reporter.shard=0
org.chojin.spark.lineage.reporter.compression=deflate
```

Then, configure `spark.sql.queryExecutionListeners` to include the `SparkSqlLineageListener`:

```bash
$ spark-shell \
  --jars file:///path/to/spark-lineage-0.0.1-SNAPSHOT.jar \
  --conf spark.sql.queryExecutionListeners=org.chojin.spark.lineage.SparkSqlLineageListener
```
