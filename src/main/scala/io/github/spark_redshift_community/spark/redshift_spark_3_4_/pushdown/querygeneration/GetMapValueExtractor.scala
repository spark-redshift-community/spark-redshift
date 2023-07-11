package io.github.spark_redshift_community.spark.redshift.pushdown.querygeneration

import org.apache.spark.sql.catalyst.expressions.{Expression, GetMapValue}

private[querygeneration] object GetMapValueExtractor {
  def unapply(expr: Expression): Option[(Expression, Expression, Boolean)] = expr match {
    // set third tuple value representing failOnError to false
    // this is how GetMapValue in spark 3.4 works. Since the
    // parameter has been removed
    case GetMapValue(child, key) => Some(child, key, false)
    case _ => None
  }
}
