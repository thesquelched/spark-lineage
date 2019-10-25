package org.chojin.spark.lineage.reporter

import java.nio.ByteBuffer

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, PutItemRequest}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import org.chojin.spark.lineage.outputs.FsOutput
import org.chojin.spark.lineage.report.Report

import scala.collection.JavaConversions._

case class DynamodbReporter(table: String,
                            region: Option[String],
                            compression: Option[String],
                            json: Boolean = false,
                            _client: Option[AmazonDynamoDB] = None) extends Reporter {
  def this(props: Map[String, String]) = this(
    table=props("table"),
    region=props.get("region"),
    compression=props.get("compression"),
    json=props.get("json").exists(value => Set("true", "yes", "1").contains(value.toLowerCase))
  )

  private lazy val client = _client.getOrElse(region match {
    case Some(r) => AmazonDynamoDBClientBuilder.standard().withRegion(r).build()
    case None => AmazonDynamoDBClientBuilder.defaultClient()
  })

  override def report(report: Report): Unit = {
    val outputKey = report.output match {
      case FsOutput(path, _, typeName) => s"$typeName-$path"
    }

    val baseItem = Map(
      "outputKey" -> new AttributeValue().withS(outputKey)
    )

    val reportItem = if (json) Map("json" -> compress(report.toJson().getBytes())) else report.toMap()
    val item = baseItem ++ reportItem.map({ case (k, v) => k -> makeAttr(v) })

    client.putItem(
      new PutItemRequest()
        .withTableName(table)
        .withItem(item))
  }

  private def makeAttr(item: Any): AttributeValue = item match {
    case s: String => new AttributeValue().withS(s)
    case m: Map[String, Any] => new AttributeValue().withM(m.map({ case (k, v) => k -> makeAttr(v) }))
    case i: Iterable[Any] => new AttributeValue().withL(i.map(makeAttr).toList)
    case b: Array[Byte] => new AttributeValue().withB(ByteBuffer.wrap(b))
  }
}
