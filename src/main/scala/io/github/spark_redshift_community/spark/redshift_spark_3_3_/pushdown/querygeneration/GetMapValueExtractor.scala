package io.github.spark_redshift_community.spark.redshift.pushdown.querygeneration

import org.apache.spark.sql.catalyst.expressions.{Expression, GetMapValue}

object GetMapValueExtractor {
  def unapply(expr: Expression): Option[(Expression, Expression, Boolean)] = expr match {
    case GetMapValue(child, key, failOnError) => Some(child, key, failOnError)
    case _ => None
  }
}
