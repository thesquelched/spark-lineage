package org.chojin.spark.lineage

import java.io.File
import java.nio.file.{Files, Path}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.chojin.spark.lineage.inputs.{Field, HiveInput, How}
import org.chojin.spark.lineage.outputs.FsOutput
import org.chojin.spark.lineage.report.{Metadata, Report}
import org.chojin.spark.lineage.reporter.InMemoryReporter
import org.scalatest._

class SparkSqlLineageListenerSpec extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach with Matchers with Inside {
  private var spark: SparkSession = _

  private val tempDir: Path = Files.createTempDirectory("listener-test-")
  private val reporter: InMemoryReporter = InMemoryReporter(compression = None)
  private val listener: SparkSqlLineageListener = SparkSqlLineageListener(List(reporter), async = false)
  private val outputPath = tempDir.resolve("test.parquet")

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession
      .builder()
      .master("local")
      .appName("test")
      .config("spark.ui.enabled", "false")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("create database test")

    Seq("foo", "bar", "baz").foreach { name =>
      val path = tempDir.resolve(s"test/$name/day=2019-10-01/data.csv")
      path.getParent.toFile.mkdirs()

      Files.write(path, "1,a,10\n2,b,20\n3,c,30\n".getBytes())

      spark.sql(
        s"""
           |CREATE EXTERNAL TABLE test.$name (
           | pk BIGINT,
           | name STRING,
           | value BIGINT
           |)
           |PARTITIONED BY (day STRING)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
           |STORED AS TEXTFILE
           |LOCATION '${path.getParent.getParent.toString}'
           |""".stripMargin)
      spark.sql(
        s"""
           |ALTER TABLE test.$name
           |ADD PARTITION (day='2019-10-01')
           |LOCATION '${path.getParent.toString}'
           |""".stripMargin)
    }

    spark.listenerManager.register(listener)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    reporter.clear()
  }

  override protected def afterEach(): Unit = {
    FileUtils.deleteDirectory(outputPath.toFile)

    super.afterEach()
  }

  override protected def afterAll(): Unit = {
    spark.listenerManager.unregister(listener)
    spark.sessionState.catalog.reset()
    spark.stop()

    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    FileUtils.deleteDirectory(tempDir.toFile)
    FileUtils.deleteDirectory(new File("spark-warehouse"))

    super.afterAll()
  }

  test("hive projection") {
    val ss = spark
    import ss.implicits._

    spark.table("test.foo")
      .select('pk, concat('name, 'value.cast(StringType)) as 'new_value)
      .write
      .parquet(outputPath.toString)

    val expected = Report(
      Metadata("test"),
      FsOutput(s"file:$outputPath", "Parquet"),
      Map(
        "pk" -> List(
          HiveInput(
            "test.foo",
            Set(Field("pk", How.PROJECTION)))),
        "new_value" -> List(
          HiveInput(
            "test.foo",
            Set(Field("name", How.PROJECTION), Field("value", How.PROJECTION))))))

    reporter.getReports() should contain theSameElementsInOrderAs List(expected)
  }

  test("hive filter") {
    val ss = spark
    import ss.implicits._

    spark.table("test.foo")
      .filter('name =!= "c")
      .select('pk, concat('pk, 'value.cast(StringType)) as 'new_value)
      .write
      .parquet(outputPath.toString)

    val expected = Report(
      Metadata("test"),
      FsOutput(s"file:$outputPath", "Parquet"),
      Map(
        "pk" -> List(
          HiveInput(
            "test.foo",
            Set(
              Field("pk", How.PROJECTION),
              Field("name", How.FILTER)))),
        "new_value" -> List(
          HiveInput(
            "test.foo",
            Set(
              Field("pk", How.PROJECTION),
              Field("value", How.PROJECTION),
              Field("name", How.FILTER))))))

    reporter.getReports() should contain theSameElementsInOrderAs List(expected)
  }

  test("hive aggregate") {
    val ss = spark
    import ss.implicits._

    spark.table("test.foo")
      .groupBy('name)
      .count
      .write
      .parquet(outputPath.toString)

    val expected = Report(
      Metadata("test"),
      FsOutput(s"file:$outputPath", "Parquet"),
      Map(
        "count" -> List(HiveInput("test.foo", Set(Field("name", How.GROUPBY)))),
        "name" -> List(HiveInput("test.foo", Set(Field("name", How.GROUPBY))))))

    reporter.getReports() should contain theSameElementsInOrderAs List(expected)
  }

  test("hive aggregate sum") {
    val ss = spark
    import ss.implicits._

    spark.table("test.foo")
      .groupBy('name)
      .agg(sum('value) as 'sum)
      .write
      .parquet(outputPath.toString)

    val expected = Report(
      Metadata("test"),
      FsOutput(s"file:$outputPath", "Parquet"),
      Map(
        "sum" -> List(
          HiveInput(
            "test.foo",
            Set(
              Field("name", How.GROUPBY),
              Field("value", How.AGGREGATE)))),
        "name" -> List(HiveInput("test.foo", Set(Field("name", How.GROUPBY))))))

    reporter.getReports() should contain theSameElementsInOrderAs List(expected)
  }

  test("hive join") {
    val ss = spark
    import ss.implicits._

    val foo = spark.table("test.foo").select('pk, 'name, 'value)
    val bar = spark.table("test.bar")


    foo.join(bar, Seq("pk"))
      .select(foo.col("*"), bar.col("name") as 'bar_name)
      .write
      .parquet(outputPath.toString)

    val expected = Report(
      Metadata("test"),
      FsOutput(s"file:$outputPath", "Parquet"),
      Map(
        "pk" -> List(
          HiveInput("test.foo", Set(Field("pk", How.JOIN), Field("pk", How.PROJECTION))),
          HiveInput("test.bar", Set(Field("pk", How.JOIN)))),
        "name" -> List(
          HiveInput(
            "test.foo",
            Set(Field("pk", How.JOIN), Field("name", How.PROJECTION))),
          HiveInput(
            "test.bar",
            Set(Field("pk", How.JOIN)))),
        "value" -> List(
          HiveInput(
            "test.foo",
            Set(Field("pk", How.JOIN), Field("value", How.PROJECTION))),
          HiveInput(
            "test.bar",
            Set(Field("pk", How.JOIN)))),
        "bar_name" -> List(
          HiveInput(
            "test.foo",
            Set(Field("pk", How.JOIN))),
          HiveInput(
            "test.bar",
            Set(Field("pk", How.JOIN), Field("name", How.PROJECTION))))))

    reporter.getReports() should contain theSameElementsInOrderAs List(expected)
  }

  test("hive union") {
    val ss = spark
    import ss.implicits._

    Seq("foo", "bar")
        .map(name => spark.table(s"test.$name").select('pk, concat('name, 'value.cast(StringType)) as 'new_value))
        .reduce((a, b) => a.union(b))
        .write
        .parquet(outputPath.toString)

    val expected = Report(
      Metadata("test"),
      FsOutput(s"file:$outputPath", "Parquet"),
      Map(
        "pk" -> List(
          HiveInput(
            "test.foo",
            Set(Field("pk", How.PROJECTION))),
          HiveInput(
            "test.bar",
            Set(Field("pk", How.PROJECTION)))
        ),
        "new_value" -> List(
          HiveInput(
            "test.foo",
            Set(Field("name", How.PROJECTION), Field("value", How.PROJECTION))),
          HiveInput(
            "test.bar",
            Set(Field("name", How.PROJECTION), Field("value", How.PROJECTION)))
        )))

    reporter.getReports() should contain theSameElementsInOrderAs List(expected)
  }
}
