package org.chojin.spark.lineage.report

case class Metadata(appName: String) {
  def toMap(): Map[String, String] = Map(
    "appName" -> appName
  )
}
