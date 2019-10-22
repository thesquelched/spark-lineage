package org.chojin.spark.lineage.reporter

import java.io.ByteArrayOutputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.chojin.spark.lineage.report.Report

trait Reporter {
  val compression: Option[String]

  def report(report: Report): Unit

  private lazy val codec = compression.map(name =>
    new CompressionCodecFactory(new Configuration()).getCodecByName(name))

  protected def compress(data: Array[Byte]): Array[Byte] = codec.map(codec => {
    val bos = new ByteArrayOutputStream()
    val os = codec.createOutputStream(bos)

    os.write(data)
    os.flush()
    os.close()

    bos.toByteArray
  }).getOrElse(data)
}
