package io.github.spark_redshift_community.spark.redshift.pushdown.querygeneration

import org.apache.spark.sql.catalyst.expressions.Expression

private[querygeneration] object PromotePrecisionExtractor {
  // Always return none, this operator was removed from 3.4
  def unapply(expr: Expression): Option[Expression] = None
}
