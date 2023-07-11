package io.github.spark_redshift_community.spark.redshift.pushdown.querygeneration

import org.apache.spark.sql.catalyst.expressions.{Expression, Round}

private[querygeneration] object RoundExtractor {
  def unapply(expr: Expression): Option[(Expression, Expression, Boolean)] = expr match {
    case Round(child, scale, ansiEnabled) => Some(child, scale, ansiEnabled)
    case _ => None
  }
}
