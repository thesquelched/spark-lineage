package org.chojin.spark.lineage

import grizzled.slf4j.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.{CompressionCodecFactory, GzipCodec}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.util.QueryExecutionListener
import org.chojin.spark.lineage.inputs.{HiveInput, Input}
import org.chojin.spark.lineage.outputs.FsOutput
import org.chojin.spark.lineage.reporter.{Metadata, Report, Reporter}

class SparkSqlLineageListener(reporter: Reporter) extends QueryExecutionListener {
  private lazy val LOGGER = Logger[this.type]

  def findSource(attr: AttributeReference, plan: LogicalPlan): Seq[Input] = findSource(attr.toAttribute, plan)

  def findSource(attr: Attribute, plan: LogicalPlan): Seq[Input] = {
    plan.collect {
      case r: HiveTableRelation if r.outputSet.contains(attr) => Seq(HiveInput(r.tableMeta.qualifiedName, Set(attr.name)))
      case r: LogicalRelation if r.outputSet.contains(attr) => Seq(HiveInput(r.catalogTable.get.qualifiedName, Set(attr.name)))
      case j: Join => {
        val conds = j.condition.map { cond =>
          cond.collect {
            case ar: AttributeReference => j.children.flatMap({ child => findSource(ar, child) })
            case al: Alias => al.collect {
              case ar: AttributeReference => j.children.flatMap({ child => findSource(ar, child) })
            }.flatten
          }.flatten
        }.getOrElse(Seq())

        j.children.flatMap({ child => findSource(attr, child) ++ conds })
      }
      case p: Project => {
        p.projectList.flatMap { proj =>
          proj.collect {
            case ar: AttributeReference if ar.toAttribute == attr => findSource(ar, p.child)
            case al: Alias if al.name == attr.name => al.collect {
              case ar: AttributeReference => findSource(ar, p.child)
            }.flatten
          }.flatten
        }
      }
      case f: Filter => f.condition.collect {
        case ar: AttributeReference => findSource(ar, f.child)
        case _ => Seq()
      }.flatten
      case a: Aggregate => {
        val groupings = a.groupingExpressions.collect {
          case ar: AttributeReference => findSource(ar, a.child)
          case al: Alias => al.collect {
            case ar: AttributeReference => findSource(ar, a.child)
          }.flatten
        }.flatten

        val aggregates = a.aggregateExpressions.diff(a.groupingExpressions).collect {
          case ar: AttributeReference if ar.toAttribute == attr => findSource(ar, a.child) ++ groupings
          case al: Alias if al.name == attr.name => al.collect {
            case ar: AttributeReference => findSource(ar, a.child) ++ groupings
          }.flatten
        }.flatten

        aggregates ++ groupings
      }
      case x => {
        LOGGER.info(s"Found ${x.getClass.getName}")
        Seq()
      }
    }.flatten
  }

  def run(qe: QueryExecution): Option[Report] = {
    qe.logical.collectFirst {
      case c: InsertIntoHadoopFsRelationCommand => {
        val output = FsOutput(c.outputPath.toString, c.fileFormat.toString)
        val inputs = c.query.output.map {
          attr => attr -> findSource(attr, c.query)
        }.map({ case (field, rawInputs) => {
          val inputs = rawInputs.groupBy {
            case HiveInput(name, _, _) => (HiveInput, name)
          }.map { case (key, value) => {
            key match {
              case (HiveInput, table) =>
                HiveInput(table, value.asInstanceOf[Seq[HiveInput]].map(_.columns).reduce((a, b) => a ++ b))
            }
          }
          }

          field.name -> inputs.toList
        }
        })

        LOGGER.info(s"Inputs: $inputs")

        val metadata = Metadata(qe.sparkSession.sparkContext.appName)

        Report(metadata, output, Map(inputs: _*))
      }
    }
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    qe.sparkSession.sparkContext.appName
    LOGGER.debug(s"Logical plan: ${qe.logical}")
    run(qe).foreach(report => {
      LOGGER.debug(s"Produced report: ${report.prettyPrint}")
      reporter.report(report)
    })

  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {

  }
}
