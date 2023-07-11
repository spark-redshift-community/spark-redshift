package io.github.spark_redshift_community.spark.redshift.pushdown.querygeneration

import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.types.DataType

private[querygeneration] object CastExtractor {
  def unapply(expr: Expression): Option[(Expression, DataType, Boolean)] = expr match {
    case Cast(child, dataType, _, ansiEnabled) => Some(child, dataType, ansiEnabled)
    case _ => None
  }
}
