package io.github.spark_redshift_community.spark.redshift.pushdown.querygeneration

import org.apache.spark.sql.catalyst.expressions.{Expression, PromotePrecision}

private[querygeneration] object PromotePrecisionExtractor {
  def unapply(expr: Expression): Option[Expression] = expr match {
    case PromotePrecision(expression) => Some(expression)
    case _ => None
  }
}
