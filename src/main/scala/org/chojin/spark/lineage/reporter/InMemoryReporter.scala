package org.chojin.spark.lineage.reporter

import org.chojin.spark.lineage.report.Report

import scala.collection.mutable.ListBuffer

case class InMemoryReporter(compression: Option[String]) extends Reporter {
  private val reports = new ListBuffer[Report]()

  override def report(report: Report): Unit = {
    reports += report
  }

  def getReports(): List[Report] = reports.toList

  def clear(): Unit = reports.clear()
}
