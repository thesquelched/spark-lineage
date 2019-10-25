package org.chojin.spark.lineage

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import grizzled.slf4j.Logger
import org.apache.spark.sql.execution.QueryExecution
import org.chojin.spark.lineage.reporter.Reporter

class ReportProcessor(private val reporters: List[Reporter]) {
  private lazy val LOGGER = Logger[this.type]

  private val queue = new LinkedBlockingQueue[QueryExecution](1000)

  private lazy val thread = new Thread {
    override def run(): Unit = processReports()
  }

  def runInBackground(): Unit = {
    thread.setName(getClass.getName.concat("-process"))
    thread.setDaemon(true)

    Option(Thread.currentThread().getContextClassLoader) match {
      case Some(loader) if getClass.getClassLoader != loader => thread.setContextClassLoader(loader)
      case _ => LOGGER.debug("Context loader not set")
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
    LOGGER.debug("Polling for report to process")
    Option(queue.poll(500L, TimeUnit.MILLISECONDS)).foreach({qe => {
      LOGGER.info("Processing report")
      LOGGER.debug(s"Query execution: $qe")

      QueryParser.parseQuery(qe).foreach(report => {
        LOGGER.debug(s"Produced report: ${report.prettyPrint}")

        reporters.foreach(reporter => reporter.report(report))

        LOGGER.info("Successfully processed report")
      })
    }})
  }

  def processReports(): Unit = {
    LOGGER.info("Starting report processor thread")

    val reportersStr = reporters.map(reporter => s"  $reporter").mkString("\n")
    LOGGER.info(s"Found ${reporters.length} reporters:\n$reportersStr")

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
