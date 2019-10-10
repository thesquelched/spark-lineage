package org.chojin.spark.lineage.reporter

import org.chojin.spark.lineage.inputs.Input
import org.chojin.spark.lineage.outputs.Output

case class Report(output: Output, inputs: Map[String, List[Input]])
