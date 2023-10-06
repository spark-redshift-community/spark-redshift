/*
 * Copyright 2015-2018 Snowflake Computing
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.spark_redshift_community.spark.redshift.pushdown.querygeneration

import org.apache.spark.sql.catalyst.expressions.{ExprId, Expression, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object ScalarSubqueryExtractor {
  def unapply(expr: Expression): Option[(LogicalPlan, Seq[Expression], ExprId, Seq[Expression])] =
    expr match {
      // ignoring hintinfo and mayHaveCountBug
      case sq : ScalarSubquery =>
        Some(sq.plan, sq.outerAttrs, sq.exprId, sq.joinCond)
      case _ => None
  }
}
