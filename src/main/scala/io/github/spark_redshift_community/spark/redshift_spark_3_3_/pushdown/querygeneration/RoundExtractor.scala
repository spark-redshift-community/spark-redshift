package io.github.spark_redshift_community.spark.redshift.pushdown.querygeneration

import org.apache.spark.sql.catalyst.expressions.{Expression, Round}

private[querygeneration] object RoundExtractor {
  def unapply(expr: Expression): Option[(Expression, Expression, Boolean)] = expr match {
    // always return false for ansiEnabled since spark 3.3 connector
    // acted as though it was always false
    case Round(child, scale) => Some(child, scale, false)
    case _ => None
  }
}
