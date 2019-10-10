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
  private var tempDir: Path = _

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

    spark.listenerManager.register(listener)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    reporter.clear()
    tempDir = Files.createTempDirectory("listener-test-")
  }

  override protected def afterEach(): Unit = {
    FileUtils.deleteDirectory(tempDir.toFile)
    tempDir = null

    super.afterEach()
  }

  override protected def afterAll(): Unit = {
    spark.sessionState.catalog.reset()
    spark.stop()

    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    spark = null

    FileUtils.deleteDirectory(new File("spark-warehouse"))

    super.afterAll()
  }

  test("hive projection") {
    val ss = spark
    import ss.implicits._

    val csvFile = tempDir.resolve("test/foo/test.csv")
    csvFile.getParent.toFile.mkdirs()

    Files.write(csvFile, "a,1\nb,2\nc,3\n".getBytes())

    spark.sql("create database test")
    spark.sql(
      s"""
         |create external table test.foo (
         | name string,
         | value int
         |)
         |row format delimited fields terminated by ','
         |stored as textfile
         |location '${csvFile.getParent.toString}'
         |""".stripMargin)

    val outputPath = tempDir.resolve("test.parquet").toString
    spark.table("test.foo")
      .select('name, concat('name, 'value.cast(StringType)) as 'new_value)
      .write
      .parquet(outputPath)

    assert(reporter.getReports().toList == List(Report(
      FsOutput(s"file:$outputPath", "Parquet"),
      Map("name" -> List(HiveInput("test.foo", List("name"))),
          "new_value" -> List(HiveInput("test.foo", List("name", "value")))))))
  }

  test("hive aggregate") {
    val ss = spark
    import ss.implicits._

    val csvFile = tempDir.resolve("test/foo/test.csv")
    csvFile.getParent.toFile.mkdirs()

    Files.write(csvFile, "a,1\nb,2\nc,3\n".getBytes())

    spark.sql("create database test")
    spark.sql(
      s"""
        |create external table test.foo (
        | name string,
        | value int
        |)
        |row format delimited fields terminated by ','
        |stored as textfile
        |location '${csvFile.getParent.toString}'
        |""".stripMargin)

    spark.table("test.foo")
      .filter('name =!= "c")
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
