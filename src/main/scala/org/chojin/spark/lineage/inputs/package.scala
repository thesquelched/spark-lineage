package org.chojin.spark.lineage

import org.chojin.spark.lineage.inputs.How.How

package object inputs {

  sealed trait Input {
    val typeName: String
    val fields: Set[Field]
  }

  object How extends Enumeration {
    type How = Value
    val PROJECTION, FILTER, JOIN, AGGREGATE, GROUPBY = Value
  }

  sealed case class Field(name: String, how: How) {
    override def toString: String = s"Column(name: $name, how: ${how}"
  }

  case class HiveInput(name: String, fields: Set[Field], typeName: String = "hive") extends Input {
    override def toString: String = s"HiveInput(name: $name, columns: ${fields.mkString(", ")})"
  }

}

