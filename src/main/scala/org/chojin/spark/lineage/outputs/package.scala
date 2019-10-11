package org.chojin.spark.lineage

package object outputs {
  sealed trait Output {
    val typeName: String
  }

  case class FsOutput(path: String, format: String, typeName: String = "fs") extends Output
}

