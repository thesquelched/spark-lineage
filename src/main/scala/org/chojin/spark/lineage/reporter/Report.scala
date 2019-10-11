package org.chojin.spark.lineage.reporter

import org.chojin.spark.lineage.inputs.Input
import org.chojin.spark.lineage.outputs.Output

case class Report(output: Output, inputs: Map[String, List[Input]]) {
  override def equals(other: Any): Boolean = other match {
    case Report(otherOutput, otherInput) => output == otherOutput && inputs.toSet == otherInput.toSet
    case _ => false
  }
}
