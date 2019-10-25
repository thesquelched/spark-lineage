package org.chojin.spark.lineage

import grizzled.slf4j.Logger
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.chojin.spark.lineage.reporter.Reporter

class SparkSqlLineageListener(reporter: Reporter) extends QueryExecutionListener {
  private lazy val LOGGER = Logger[this.type]

  def this() = this(Config.createInstanceOf("reporter"))

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    qe.sparkSession.sparkContext.appName
    LOGGER.debug(s"Logical plan: ${qe.logical}")
    QueryParser.parseQuery(qe).foreach(report => {
      LOGGER.debug(s"Produced report: ${report.prettyPrint}")
      reporter.report(report)
    })

  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {

  }
}
