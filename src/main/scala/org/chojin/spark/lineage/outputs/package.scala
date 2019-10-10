package org.chojin.spark.lineage

import org.apache.spark.sql.execution.datasources.FileFormat

package object outputs {
  sealed trait Output

  case class FsOutput(path: String, format: String) extends Output
}
