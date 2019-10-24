package org.chojin.spark.lineage

package object outputs {
  sealed trait Output {
    val typeName: String
    def toMap(): Map[String, Any]
  }

  case class FsOutput(path: String, format: String, typeName: String = "fs") extends Output {
    override def toMap: Map[String, Any] = Map(
      "type" -> typeName,
      "format" -> format,
      "path" -> path
    )
  }
}

