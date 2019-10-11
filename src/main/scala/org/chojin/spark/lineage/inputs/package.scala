package org.chojin.spark.lineage

package object inputs {
  sealed trait Input

  case class HiveInput(name: String, columns: Set[String]) extends Input {
    override def toString: String = s"HiveInput(name: $name, columns: ${columns.mkString(", ")})"
  }
}
