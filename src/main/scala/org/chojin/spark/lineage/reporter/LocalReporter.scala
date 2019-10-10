package org.chojin.spark.lineage.reporter

import scala.collection.mutable.ListBuffer

class LocalReporter extends Reporter {
  private val reports = new ListBuffer[Report]()

  override def report(report: Report): Unit = {
    reports += report
  }

  def getReports(): List[Report] = reports.toList

  def clear(): Unit = reports.clear()
}
