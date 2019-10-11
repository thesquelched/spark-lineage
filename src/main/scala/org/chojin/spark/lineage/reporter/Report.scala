package org.chojin.spark.lineage.reporter

import org.chojin.spark.lineage.inputs.Input
import org.chojin.spark.lineage.outputs.Output

case class Report(output: Output, inputs: Map[String, List[Input]]) {
  override def equals(other: Any): Boolean = other match {
    case Report(otherOutput, otherInput) => output == otherOutput && inputs.toSet == otherInput.toSet
    case _ => false
  }

  def prettyPrint = {
    val inputsStr = inputs.map { case (k, v) =>
      val valStr = v.map({ input => s"      $input"}).mkString("\n")
      s"    $k:\n$valStr"
    }.mkString("\n")

    s"""|Report(
        |  output: $output,
        |  inputs:
        |$inputsStr
        |""".stripMargin
  }
}
