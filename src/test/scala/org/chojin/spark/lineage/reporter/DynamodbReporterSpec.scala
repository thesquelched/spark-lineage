package org.chojin.spark.lineage.reporter

import scala.collection.JavaConversions._
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, PutItemRequest}
import org.chojin.spark.lineage.inputs.{Field, HiveInput, How}
import org.chojin.spark.lineage.outputs.FsOutput
import org.chojin.spark.lineage.report.{Metadata, Report}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.mockito.captor.ArgCaptor
import org.scalatest.{WordSpec, Matchers => ScalaTestMatchers}

class DynamodbReporterSpec
  extends WordSpec
    with MockitoSugar
    with ScalaTestMatchers
    with ArgumentMatchersSugar
{
  val report = Report(
    Metadata("my-app"),
    FsOutput(
      path = "s3:///bucket/path/to/data",
      format = "parquet"),
    Map(
      "one" -> List(
        HiveInput(
          name = "db.table1",
          fields = Set(
            Field(name = "pk", how = How.JOIN),
            Field(name = "one", how = How.PROJECTION))),
        HiveInput(
          name = "db.table2",
          fields = Set(
            Field(name = "pk", how = How.JOIN)))),
      "two" -> List(
        HiveInput(
          name = "db.table1",
          fields = Set(
            Field(name = "pk", how = How.JOIN))),
        HiveInput(
          name = "db.table2",
          fields = Set(
            Field(name = "pk", how = How.JOIN),
            Field(name = "two", how = How.PROJECTION))))
    )
  )

  "report" should {
    "put a dynamo record" in {
      val dynamo = mock[AmazonDynamoDB]
      val reporter = DynamodbReporter(
        table = "mytable",
        region = None,
        compression = None,
        _client = Some(dynamo))

      reporter.report(report)

      val captor = ArgCaptor[PutItemRequest]
      verify(dynamo).putItem(captor)

      captor.value.getTableName shouldEqual "mytable"

      val item = captor.value.getItem

      item("outputKey") shouldEqual new AttributeValue().withS(
        s"fs-${report.output.asInstanceOf[FsOutput].path}")

      item("metadata") shouldEqual new AttributeValue().withM(
        Map("appName" -> new AttributeValue().withS("my-app")))

      item("output") shouldEqual new AttributeValue().withM(
        Map(
          "type" -> new AttributeValue().withS("fs"),
          "format" -> new AttributeValue().withS("parquet"),
          "path" -> new AttributeValue().withS(report.output.asInstanceOf[FsOutput].path)
        ))

      item("fields") shouldEqual new AttributeValue().withM(
        Map(
          "one" -> new AttributeValue().withL(List(
            new AttributeValue().withM(Map(
              "type" -> new AttributeValue().withS("hive"),
              "name" -> new AttributeValue().withS("db.table1"),
              "fields" -> new AttributeValue().withL(List(
                new AttributeValue().withM(Map(
                  "name" -> new AttributeValue().withS("pk"),
                  "how" -> new AttributeValue().withS("join")
                )),
                new AttributeValue().withM(Map(
                  "name" -> new AttributeValue().withS("one"),
                  "how" -> new AttributeValue().withS("projection")
                ))
              ))
            )),
            new AttributeValue().withM(Map(
              "type" -> new AttributeValue().withS("hive"),
              "name" -> new AttributeValue().withS("db.table2"),
              "fields" -> new AttributeValue().withL(List(
                new AttributeValue().withM(Map(
                  "name" -> new AttributeValue().withS("pk"),
                  "how" -> new AttributeValue().withS("join")
                ))
              ))
            ))
          )),
          "two" -> new AttributeValue().withL(List(
            new AttributeValue().withM(Map(
              "type" -> new AttributeValue().withS("hive"),
              "name" -> new AttributeValue().withS("db.table1"),
              "fields" -> new AttributeValue().withL(List(
                new AttributeValue().withM(Map(
                  "name" -> new AttributeValue().withS("pk"),
                  "how" -> new AttributeValue().withS("join")
                ))
              ))
            )),
            new AttributeValue().withM(Map(
              "type" -> new AttributeValue().withS("hive"),
              "name" -> new AttributeValue().withS("db.table2"),
              "fields" -> new AttributeValue().withL(List(
                new AttributeValue().withM(Map(
                  "name" -> new AttributeValue().withS("pk"),
                  "how" -> new AttributeValue().withS("join")
                )),
                new AttributeValue().withM(Map(
                  "name" -> new AttributeValue().withS("two"),
                  "how" -> new AttributeValue().withS("projection")
                ))
              ))
            ))
          ))
        ))
    }
  }
}
