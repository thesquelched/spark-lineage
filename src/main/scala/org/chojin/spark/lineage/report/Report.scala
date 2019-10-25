package org.chojin.spark.lineage.report

import org.chojin.spark.lineage.inputs.{HiveInput, How, Input}
import org.chojin.spark.lineage.outputs.Output
import org.json4s._
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

case class Report(metadata: Metadata, output: Output, fields: Map[String, List[Input]]) {
  implicit val formats = Serialization.formats(NoTypeHints) + new EnumNameSerializer(How)

  override def equals(other: Any): Boolean = other match {
    case Report(otherMeta, otherOutput, otherInput) => (
      metadata == otherMeta
        && output == otherOutput
        && fields.mapValues(_.toSet).toSet == otherInput.mapValues(_.toSet).toSet
      )
    case _ => false
  }

  def prettyPrint = {
    val fieldsStr = fields.map { case (k, v) =>
      val valStr = v.map({
        case HiveInput(name, columns, _) => {
          s"""      HiveInput(
             |        name: $name
             |        columns:
             |          ${columns.mkString("\n          ")}""".stripMargin
        }
        case field => s"      $field"
      }).mkString("\n")
      s"    $k:\n$valStr"
    }.mkString("\n")

    s"""|Report(
        |  metadata: $metadata,
        |  output: $output,
        |  fields:
        |$fieldsStr
        |""".stripMargin
  }

  def toJson(): String = {
    write(this)
  }

  def toMap(): Map[String, Any] = Map(
    "metadata" -> metadata.toMap(),
    "output" -> output.toMap(),
    "fields" -> fields.map({ case (k, v) => k -> v.map(_.toMap()) })
  )
}
