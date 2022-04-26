package io.github.spark_redshift_community.spark.redshift.pushdown

import io.github.spark_redshift_community.spark.redshift.pushdown.querygeneration.QueryBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute

/** Clean up the plan, then try to generate a query from it for Redshift.
 * The Try-Catch is unnecessary and may obfuscate underlying problems,
 * but in beta mode we'll do it for safety and let Spark use other strategies
 * in case of unexpected failure.
 */
class RedshiftStrategy extends Strategy {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    try {
      buildQueryRDD(plan.transform({
        case Project(Nil, child) => child
        case SubqueryAlias(_, child) => child
      })).getOrElse(Nil)
    } catch {

      case t: UnsupportedOperationException =>
        log.warn(s"Unsupported Operation:${t.getMessage}")
        Nil

      case e: Exception =>
        log.warn(s"Pushdown failed :${e.getMessage}", e)
        Nil
    }
  }

  /** Attempts to get a SparkPlan from the provided LogicalPlan.
   *
   * @param plan The LogicalPlan provided by Spark.
   * @return An Option of Seq[RedshiftPlan] that contains the PhysicalPlan if
   *         query generation was successful, None if not.
   */
  private def buildQueryRDD(plan: LogicalPlan): Option[Seq[RedshiftPlan]] =
    QueryBuilder.getRDDFromPlan(plan).map {
      case (output: Seq[Attribute], rdd: RDD[InternalRow]) =>
        Seq(RedshiftPlan(output, rdd))
    }
}