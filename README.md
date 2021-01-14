spark-lineage
=============

[![Build Status](https://travis-ci.com/thesquelched/spark-lineage.svg?branch=master)](https://travis-ci.com/thesquelched/spark-lineage)

Spark SQL listener to report lineage data to a variety of outputs, e.g. Amazon Kinesis. Heavily inspired by the
[Spark Atlas Connector](https://github.com/hortonworks-spark/spark-atlas-connector), but intended to be more generic to
help those who can't or won't use Atlas.

For a Spark SQL query that produces an output (e.g. writes data to a filesystem), the listener produces a message
containing the following:

* Output details, e.g. type, output location, and format
* For each (top-level) output field, a list of inputs that contribute to it. Each input contains:
  * Type, e.g. `hive`
  * List of input fields that affect the output field, and how they affect it (e.g. via a join, filter, aggregation,
    projection, etc.)
* Metadata, e.g. spark application name

Installation
------------

Build the jar:

```bash
$ ./gradlew shadowJar
```

Copy the output from `build/lib/jars` to your cluster. Note that you will need to have the appropriate AWS SDK jars in
 your spark classpath if you mean to use the associated reporters.


Configuration
-------------

The listener is configured via a file, `lineage.properties`, that needs to be in the class path of the spark driver,
 e.g. by placing it in the spark config directory. Here's an example using the `KinesisReporter`:

```
org.chojin.spark.lineage.reporters=org.chojin.spark.lineage.reporters.KinesisReporter
org.chojin.spark.lineage.reporter.kinesis.stream=my-kinesis-stream
org.chojin.spark.lineage.reporter.kinesis.region=us-east-1
org.chojin.spark.lineage.reporter.kinesis.shard=0
org.chojin.spark.lineage.reporter.kinesis.compression=gzip
```

Then, configure `spark.sql.queryExecutionListeners` to include the `SparkSqlLineageListener`:

```bash
$ spark-shell \
  --jars file:///path/to/spark-lineage-0.0.3-SNAPSHOT.jar \
  --conf spark.sql.queryExecutionListeners=org.chojin.spark.lineage.SparkSqlLineageListener
```

JSON Format
-----------

Consider the following spark example:

```scala
val foo = spark.table("mydb.foo")
val bar = spark.table("mydb.bar")

foo
  .join(bar, foo.col("bar_fk") === bar.col("pk"))
  .groupBy('my_flag)
  .agg(sum('amount) as 'amount_sum)
  .write
  .mode("overwrite")
  .parquet("s3:///bucket/path/to/report")
```

Once evaluated, the following JSON record is produced:

```json
{
    "output": {
        "path": "s3://bucket/path/to/report",
        "type": "fs",
        "format": "Parquet"
    },
    "metadata": {
     "appName": "Spark shell"
    },
    "outputKey": "fs-s3://bucket/path/to/report",
    "fields": {
        "my_flag": [
            {
                "type": "hive",
                "name": "mydb.foo",
                "fields": [
                    {
                        "name": "bar_fk",
                        "how": "join"
                    }
                ]
            },
            {
                "type": "hive",
                "name": "mydb.bar",
                "fields": [
                    {
                        "name": "pk",
                        "how": "join"
                    },
                    {
                        "name": "groupby",
                        "how": "my_flag"
                    }
                ]
            }
        ],
        "amount_sum": [
            {
                "type": "hive",
                "name": "mydb.foo",
                "fields": [
                    {
                        "name": "bar_fk",
                        "how": "join"
                    },
                    {
                        "name": "amount",
                        "how": "aggregate"
                    }
                ]
            },
            {
                "type": "hive",
                "name": "mydb.bar",
                "fields": [
                    {
                        "name": "pk",
                        "how": "join"
                    },
                    {
                        "name": "groupby",
                        "how": "my_flag"
                    }
                ]
            }
        ]
    }
}
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

* `stream` - stream name
* `region` - region (e.g. `us-east-2`)
* `shard` - shard/partition to which to write; multiple shards are not supported at this time
* `compression` - optional compression to apply to the JSON blob; any standard spark compression codec is supported,
  e.g. `gzip`, `deflate`

### DynamoDB

Class name: `org.chojin.spark.lineage.reporters.DynamodbReporter`

Write a structured record to an AWS DynamoDB table.

#### Options

Options start with the prefix `org.chojin.spark.lineage.reporter.dynamodb.`

* `table` - table name
* `region` - region (e.g. `us-east-2`)
* `json` - if `true`, write a single attribute, `json`, containing the lineage info as a binary blob
* `compression` - optional compression to apply to the JSON blob (only if `json=true`; any standard spark compression
  codec is supported, e.g. `gzip`, `deflate`

With `json=false`, and using the spark example above, the following DynamoDB record is produced:

```json
{
    "Item": {
        "output": {
            "M": {
                "path": {
                    "S": "s3://bucket/path/to/report"
                },
                "type": {
                    "S": "fs"
                },
                "format": {
                    "S": "Parquet"
                }
            }
        },
        "metadata": {
            "M": {
                "appName": {
                    "S": "Spark shell"
                }
            }
        },
        "outputKey": {
            "S": "fs-s3://bucket/path/to/report"
        },
        "fields": {
            "M": {
                "my_flag": {
                    "L": [
                        {
                            "M": {
                                "fields": {
                                    "L": [
                                        {
                                            "M": {
                                                "how": {
                                                    "S": "join"
                                                },
                                                "name": {
                                                    "S": "bar_fk"
                                                }
                                            }
                                        }
                                    ]
                                },
                                "type": {
                                    "S": "hive"
                                },
                                "name": {
                                    "S": "mydb.foo"
                                }
                            }
                        },
                        {
                            "M": {
                                "fields": {
                                    "L": [
                                        {
                                            "M": {
                                                "how": {
                                                    "S": "join"
                                                },
                                                "name": {
                                                    "S": "pk"
                                                }
                                            }
                                        },
                                        {
                                            "M": {
                                                "how": {
                                                    "S": "groupby"
                                                },
                                                "name": {
                                                    "S": "my_flag"
                                                }
                                            }
                                        }
                                    ]
                                },
                                "type": {
                                    "S": "hive"
                                },
                                "name": {
                                    "S": "mydb.bar"
                                }
                            }
                        }
                    ]
                },
                "amount_sum": {
                    "L": [
                        {
                            "M": {
                                "fields": {
                                    "L": [
                                        {
                                            "M": {
                                                "how": {
                                                    "S": "join"
                                                },
                                                "name": {
                                                    "S": "bar_fk"
                                                }
                                            }
                                        },
                                        {
                                            "M": {
                                                "how": {
                                                    "S": "aggregate"
                                                },
                                                "name": {
                                                    "S": "amount"
                                                }
                                            }
                                        }
                                    ]
                                },
                                "type": {
                                    "S": "hive"
                                },
                                "name": {
                                    "S": "mydb.foo"
                                }
                            }
                        },
                        {
                            "M": {
                                "fields": {
                                    "L": [
                                        {
                                            "M": {
                                                "how": {
                                                    "S": "join"
                                                },
                                                "name": {
                                                    "S": "pk"
                                                }
                                            }
                                        },
                                        {
                                            "M": {
                                                "how": {
                                                    "S": "groupby"
                                                },
                                                "name": {
                                                    "S": "my_flag"
                                                }
                                            }
                                        }
                                    ]
                                },
                                "type": {
                                    "S": "hive"
                                },
                                "name": {
                                    "S": "mydb.bar"
                                }
                            }
                        }
                    ]
                }
            }
        }
    },
    "ScannedCount": 1,
    "ConsumedCapacity": null
}
```
