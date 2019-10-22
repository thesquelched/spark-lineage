package org.chojin.spark.lineage.report

import org.chojin.spark.lineage.inputs.Input
import org.chojin.spark.lineage.outputs.Output
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

case class Report(metadata: Metadata, output: Output, inputs: Map[String, List[Input]]) {
  implicit val formats = Serialization.formats(NoTypeHints)

  override def equals(other: Any): Boolean = other match {
    case Report(otherMeta, otherOutput, otherInput) => (
      metadata == otherMeta
        && output == otherOutput
        && inputs.mapValues(_.toSet).toSet == otherInput.mapValues(_.toSet).toSet
      )
    case _ => false
  }

  def prettyPrint = {
    val inputsStr = inputs.map { case (k, v) =>
      val valStr = v.map({ input => s"      $input"}).mkString("\n")
      s"    $k:\n$valStr"
    }.mkString("\n")

    s"""|Report(
        |  metadata: $metadata,
        |  output: $output,
        |  inputs:
        |$inputsStr
        |""".stripMargin
  }

  def toJson(): String = {
    write(this)
  }
}
