package org.chojin.spark.lineage.reporter

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, PutItemRequest}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsync, AmazonDynamoDBAsyncClientBuilder}
import org.chojin.spark.lineage.outputs.FsOutput
import org.chojin.spark.lineage.report.Report

import scala.collection.JavaConversions._

case class DynamodbReporter(table: String,
                            region: Option[String],
                            compression: Option[String],
                            _client: Option[AmazonDynamoDBAsync] = None) extends Reporter {
  def this(props: Map[String, String]) = this(
    table=props("table"),
    region=props.get("region"),
    compression=props.get("compression")
  )

  private lazy val client = _client.getOrElse(region match {
    case Some(r) => AmazonDynamoDBAsyncClientBuilder.standard().withRegion(r).build()
    case None => AmazonDynamoDBAsyncClientBuilder.defaultClient()
  })

  override def report(report: Report): Unit = {
    val outputKey = report.output match {
      case FsOutput(path, _, typeName) => s"$typeName-$path"
    }

    val item = Map(
      "outputKey" -> new AttributeValue().withS(outputKey)
    ) ++ report.toMap().map({ case (k, v) => k -> makeAttr(v) })

    client.putItemAsync(
      new PutItemRequest()
        .withTableName(table)
        .withItem(item))
  }

  private def makeAttr(item: Any): AttributeValue = item match {
    case s: String => new AttributeValue().withS(s)
    case m: Map[String, Any] => new AttributeValue().withM(m.map({ case (k, v) => k -> makeAttr(v) }))
    case i: Iterable[Any] => new AttributeValue().withL(i.map(makeAttr).toList)
  }
}
