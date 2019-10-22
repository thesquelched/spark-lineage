package org.chojin.spark.lineage.reporter

import java.nio.ByteBuffer

import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.model.PutRecordRequest
import org.chojin.spark.lineage.report.Report

case class KinesisReporter(stream: String,
                           region: Option[String],
                           shard: String,
                           compression: Option[String]) extends Reporter {
  def this(props: Map[String, String]) = this(
    stream=props("stream"),
    region=props.get("region"),
    shard=props("shard"),
    compression=props.get("compression"))

  private lazy val client = region match {
    case Some(r) => AmazonKinesisClientBuilder.standard().withRegion(r).build
    case None => AmazonKinesisClientBuilder.defaultClient()
  }

  override def report(report: Report): Unit = {
    val payload = compress(report.toJson().getBytes())

    client.putRecord(
      new PutRecordRequest()
        .withStreamName(stream)
        .withPartitionKey(shard)
        .withData(ByteBuffer.wrap(payload)))
  }
}
