package org.chojin.spark.lineage

import java.io.File
import java.nio.file.{Files, Path}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.chojin.spark.lineage.inputs.HiveInput
import org.chojin.spark.lineage.outputs.FsOutput
import org.chojin.spark.lineage.reporter.{LocalReporter, Report}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import org.apache.spark.sql.functions._

class SparkSqlLineageListenerSpec extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {
  private var spark: SparkSession = _

  private val tempDir: Path = Files.createTempDirectory("listener-test-")
  private val reporter: LocalReporter = new LocalReporter()
  private val listener: SparkSqlLineageListener = new SparkSqlLineageListener(reporter)

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession
        .builder()
        .master("local")
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
    FileUtils.deleteDirectory(tempDir.resolve("test.parquet").toFile)

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

    val outputPath = tempDir.resolve("test.parquet").toString

    spark.table("test.foo")
      .select('pk, concat('name, 'value.cast(StringType)) as 'new_value)
      .write
      .parquet(outputPath)

    val expected = Report(
      FsOutput(s"file:$outputPath", "Parquet"),
      Map(
        "pk" -> List(HiveInput("test.foo", List("pk"))),
        "new_value" -> List(HiveInput("test.foo", List("name", "value")))))

    assert(reporter.getReports() == List(expected))
  }

  test("hive filter") {
    val ss = spark
    import ss.implicits._

    val outputPath = tempDir.resolve("test.parquet").toString

    spark.table("test.foo")
      .filter('name =!= "c")
      .select('pk, concat('pk, 'value.cast(StringType)) as 'new_value)
      .write
      .parquet(outputPath)

    val expected = Report(
      FsOutput(s"file:$outputPath", "Parquet"),
      Map(
        "" -> List(HiveInput("test.foo", List("name"))),
        "pk" -> List(HiveInput("test.foo", List("pk"))),
        "new_value" -> List(HiveInput("test.foo", List("pk", "value")))))

    assert(reporter.getReports() == List(expected))
  }

  test("hive aggregate") {
    val ss = spark
    import ss.implicits._

    spark.table("test.foo")
      .groupBy("name")
      .count
      .write
      .parquet(tempDir.resolve("test.parquet").toString)

    assert(reporter.getReports().size == 1)
  }

  test("hive table join") {
    val ss = spark
    import ss.implicits._

    val fooFile = tempDir.resolve("test/foo/data.csv")
    val barFile = tempDir.resolve("test/bar/data.csv")
    fooFile.getParent.toFile.mkdirs()
    barFile.getParent.toFile.mkdirs()

    Files.write(fooFile, "a,1\nb,2\nc,3\n".getBytes())
    Files.write(barFile, "a,10\nb,20\nc,30\n".getBytes())

    spark.sql("create database test")
    spark.sql(
      s"""
         |create external table test.foo (
         | name string,
         | value int
         |)
         |row format delimited fields terminated by ','
         |stored as textfile
         |location '${fooFile.getParent}'
         |""".stripMargin)
    spark.sql(
      s"""
         |create external table test.bar (
         | name string,
         | value int
         |)
         |row format delimited fields terminated by ','
         |stored as textfile
         |location '${barFile.getParent}'
         |""".stripMargin)

    val foo = spark.table("test.foo")
    val bar = spark.table("test.bar")

    foo
      .join(bar, Seq("name"))
      .select(foo.col("*"), bar.col("value") as 'barvalue)
      .write
      .parquet(tempDir.resolve("test.parquet").toString)
  }

  test("csv file") {
    val ss = spark
    import ss.implicits._

    val csvFile = tempDir.resolve("test/foo/test.csv")
    csvFile.getParent.toFile.mkdirs()

    Files.write(csvFile, "a,1\nb,2\nc,3\n".getBytes())

    spark
      .read
      .schema("name string, value int")
      .csv(csvFile.toAbsolutePath.toString)
      .filter('name =!= "c")
      .groupBy("name")
      .count
      .write
      .parquet(tempDir.resolve("test.parquet").toString)
  }
}
