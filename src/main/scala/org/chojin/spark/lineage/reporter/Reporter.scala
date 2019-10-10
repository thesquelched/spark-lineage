package org.chojin.spark.lineage.reporter

trait Reporter {
  def report(report: Report): Unit
}
