package org.chojin.spark.lineage

import org.chojin.spark.lineage.inputs.How.How

package object inputs {

  sealed trait Input {
    val typeName: String
    val fields: Set[Field]

    def toMap(): Map[String, Any]
  }

  object How extends Enumeration {
    type How = Value
    val PROJECTION, FILTER, JOIN, AGGREGATE, GROUPBY = Value
  }

  sealed case class Field(name: String, how: How) {
    override def toString: String = s"Column(name: $name, how: ${how}"

    def toMap(): Map[String, String] = Map(
      "name" -> name,
      "how" -> how.toString.toLowerCase
    )
  }

  case class HiveInput(name: String, fields: Set[Field], typeName: String = "hive") extends Input {
    override def toString: String = s"HiveInput(name: $name, columns: ${fields.mkString(", ")})"

    override def toMap(): Map[String, Any] = Map(
      "type" -> typeName,
      "name" -> name,
      "fields" -> fields.map(_.toMap())
    )
  }

}

