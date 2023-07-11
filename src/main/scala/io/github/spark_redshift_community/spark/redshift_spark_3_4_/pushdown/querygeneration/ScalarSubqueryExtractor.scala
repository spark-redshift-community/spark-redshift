package io.github.spark_redshift_community.spark.redshift.pushdown.querygeneration

import org.apache.spark.sql.catalyst.expressions.{ExprId, Expression, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object ScalarSubqueryExtractor {
  def unapply(expr: Expression): Option[(LogicalPlan, Seq[Expression], ExprId, Seq[Expression])] =
    expr match {
      // ignoring hintinfo
      case ScalarSubquery(plan, outerAttrs, exprId, joinCond, hintInfo) =>
        Some(plan, outerAttrs, exprId, joinCond)
      case _ => None
  }
}
