package org.chojin.spark.lineage

import grizzled.slf4j.Logger
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project, SubqueryAlias, Union}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.util.QueryExecutionListener
import org.chojin.spark.lineage.inputs.HiveInput
import org.chojin.spark.lineage.outputs.FsOutput
import org.chojin.spark.lineage.reporter.{Report, Reporter}

class SparkSqlLineageListener(reporter: Reporter) extends QueryExecutionListener {
  private lazy val LOGGER = Logger[this.type]

  def getExprSource(expr: NamedExpression, plan: LogicalPlan): Iterable[HiveInput] = {
    expr.collect{
      case attr: AttributeReference => {
        plan.collect {
          case r: HiveTableRelation if r.outputSet.contains(attr) => Seq((r.tableMeta.qualifiedName, attr.name))
        }.flatten
      }
      case _ => Seq()
    }.flatten.groupBy({ case (table, _) => table}).values.map({x => {
      val (tables, columns) = x.unzip
      HiveInput(tables.head, columns.toList)
    }})
  }
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    LOGGER.info(s"Logical plan: ${qe.logical}")
    qe.logical.collect {
//      case Union(children) => LOGGER.info("Whee!")
      case c: InsertIntoHadoopFsRelationCommand => {
        val output = FsOutput(c.outputPath.toString, c.fileFormat.toString)

        c.query.collect {
          case q: Project => {
            val inputs = Map(q.projectList.map({expr => expr.name -> getExprSource(expr, q.child).toList}): _*)

            Report(output, inputs)
          }
          //        case a: Aggregate => {
          //        }
          //        case r: LogicalRelation
        }
      }
    }.flatten.foreach(reporter.report)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {

  }
}
