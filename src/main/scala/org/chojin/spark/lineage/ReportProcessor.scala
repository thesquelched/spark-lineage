package org.chojin.spark.lineage

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import grizzled.slf4j.Logger
import org.apache.spark.sql.execution.QueryExecution
import org.chojin.spark.lineage.reporter.Reporter

class ReportProcessor(private val reporters: List[Reporter]) {
  private lazy val LOGGER = Logger[this.type]

  private val queue = new LinkedBlockingQueue[QueryExecution](100)

  private lazy val thread = new Thread {
    override def run(): Unit = processReports()
  }

  def runInBackground(): Unit = {
    thread.setName(getClass.getName.concat("-process"))
    thread.setDaemon(true)

    Option(Thread.currentThread().getContextClassLoader) match {
      case Some(loader) if getClass.getClassLoader != loader => thread.setContextClassLoader(loader)
    }

    thread.start()
  }

  def offer(qe: QueryExecution, async: Boolean = true): Unit = {
    if (!queue.offer(qe, 10L, TimeUnit.SECONDS)) {
      LOGGER.warn("Unable to query execution to queue; dropping on the floor")
      LOGGER.debug(s"Skipped query plan:\n${qe.logical.treeString(verbose = true)}")
    }

    if (!async) {
      processReport()
    }
  }

  def processReport() = {
    Option(queue.poll(1L, TimeUnit.SECONDS)).foreach({qe => {
      QueryParser.parseQuery(qe).foreach(report => {
        LOGGER.debug(s"Produced report: ${report.prettyPrint}")

        reporters.foreach(reporter => reporter.report(report))
      })
    }})
  }

  def processReports(): Unit = {
    var running = true

    while(running) {
      try {
        processReport()
      } catch {
        case _: InterruptedException => {
          LOGGER.info("Caught InterruptedException; exiting...")
          running = false
        }
        case e: Throwable => {
          LOGGER.error("Caught exception while processing report", e)
        }
      }
    }
  }
}
