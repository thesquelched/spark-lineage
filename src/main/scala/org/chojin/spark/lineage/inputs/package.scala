package org.chojin.spark.lineage

package object inputs {
  sealed trait Input

  case class HiveInput(name: String, columns: List[String]) extends Input
}
