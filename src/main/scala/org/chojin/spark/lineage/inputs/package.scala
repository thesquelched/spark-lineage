package org.chojin.spark.lineage

package object inputs {
  sealed trait Input {
    val typeName: String
  }

  case class HiveInput(name: String, columns: Set[String], typeName: String = "hive") extends Input {
    override def toString: String = s"HiveInput(name: $name, columns: ${columns.mkString(", ")})"
  }
}

