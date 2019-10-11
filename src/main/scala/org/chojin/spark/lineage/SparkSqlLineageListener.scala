package org.chojin.spark.lineage

import grizzled.slf4j.Logger
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Project, SubqueryAlias, Union}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.util.QueryExecutionListener
import org.chojin.spark.lineage.inputs.{HiveInput, Input}
import org.chojin.spark.lineage.outputs.FsOutput
import org.chojin.spark.lineage.reporter.{Report, Reporter}

class SparkSqlLineageListener(reporter: Reporter) extends QueryExecutionListener {
  private lazy val LOGGER = Logger[this.type]

//  def getSources(plan: LogicalPlan) = {
//    plan.collect {
//      case r: HiveTableRelation => (r.tableMeta.qualifiedName, "*")
//    }
//  }
//
//  def getExprSource(expr: NamedExpression, plan: LogicalPlan): Seq[(String, String)] = {
//    expr.collect{
//      case attr: AttributeReference => {
//        plan.collect {
//          case r: HiveTableRelation if r.outputSet.contains(attr) => Seq((r.tableMeta.qualifiedName, attr.name))
//          case f: Filter => {
//            f.condition.collect({ case e: NamedExpression => getExprSource(e, f.child) }).flatten
//          }
//        }.flatten
//      }
//      case _ => Seq()
//    }.flatten
//  }
//
//  def getInputMapping(plan: LogicalPlan) = {
//    plan.collect {
//      case q: Project => {
//        Map(q.projectList.map({expr => expr.name -> getExprSource(expr, q.child).toList}): _*)
//      }
////      case f: Filter => {
////        Map("" -> f.condition.collect({ case e: NamedExpression => getExprSource(e, f.child) }).flatten)
////      }
//      case a: Aggregate => {
//        val sources = getSources(a.child)
//        val groupings = Map(a.groupingExpressions.collect({ case expr:NamedExpression => expr.name -> getExprSource(expr, a.child) }): _*)
//        val aggregates = Map(a.aggregateExpressions.diff(a.groupingExpressions).collect({ case expr:NamedExpression => expr.name -> sources }): _*)
//
//        groupings ++ aggregates.map { case (k, v) => k -> (v ++ groupings.getOrElse(k, Seq()))}
//      }
//    }.flatten.groupBy({ case(column, _) => column }).values.map({ result => {
//      val (columns, allInputs) = result.unzip
//
//      val mergedInputs = allInputs.flatten.groupBy({ case (table, _) => table}).values.map({x =>
//        val (tables, columns) = x.unzip
//        HiveInput(tables.head, columns.toSet)
//      })
//
//      columns.head -> mergedInputs.toList
//    }})
//  }
//
//  def process(qe: QueryExecution) = {
//    qe.logical.collect {
//      case c: InsertIntoHadoopFsRelationCommand => {
//        LOGGER.info(s"Outputs = ${c.query.output}")
//        val output = FsOutput(c.outputPath.toString, c.fileFormat.toString)
//        val inputs = getInputMapping(c.query)
//
//        val report = Report(output, Map(inputs.toSeq: _*))
//        LOGGER.info(s"Generated report: ${report.prettyPrint}")
//
//        report
//      }
//    }
//  }

  def findSource(attr: AttributeReference, plan: LogicalPlan): Seq[Input] = findSource(attr.toAttribute, plan)

  def findSource(attr: Attribute, plan: LogicalPlan): Seq[Input] = {
    plan.collect {
      case r: HiveTableRelation if r.outputSet.contains(attr) => Seq(HiveInput(r.tableMeta.qualifiedName, Set(attr.name)))
      case p: Project => {
        p.projectList.flatMap { proj =>
          proj.collect {
            case ar: AttributeReference if ar.toAttribute == attr => findSource(ar, p.child)
            case al: Alias if al.name == attr.name => al.collect {
              case ar: AttributeReference => findSource(ar, p.child)
            }.flatten
            case _ => Seq()
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
    }.flatten
  }

  def run(plan: LogicalPlan): Option[Report] = {
    plan.collectFirst {
      case c: InsertIntoHadoopFsRelationCommand => {
        val output = FsOutput(c.outputPath.toString, c.fileFormat.toString)
        val inputs = c.query.output.map {
          attr => attr -> findSource(attr, c.query)
        }.map({ case (field, rawInputs) => {
          val inputs = rawInputs.groupBy {
            case HiveInput(name, _) => (HiveInput, name)
          }.map { case (key, value) => {
            key match {
              case (HiveInput, table) =>
                HiveInput(table, value.asInstanceOf[Seq[HiveInput]].map(_.columns).reduce((a, b) => a ++ b))
            }
          }}

          field.name -> inputs.toList
        }})

        LOGGER.info(s"Inputs: $inputs")

        Report(output, Map(inputs: _*))
      }
    }
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    LOGGER.info(s"Logical plan: ${qe.logical}")
    //process(qe).foreach(reporter.report)
    run(qe.logical).foreach(report => {
      LOGGER.info(s"Produced report: ${report.prettyPrint}")
      reporter.report(report)
    })

  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {

  }
}
