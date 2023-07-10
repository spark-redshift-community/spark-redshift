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

import io.github.spark_redshift_community.spark.redshift.pushdown.{ConstantString, EmptyRedshiftSQLStatement, RedshiftSQLStatement}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Attribute, DenseRank, Expression, PercentRank, Rank, RowNumber, WindowExpression, WindowSpecDefinition}

/**
  * Windowing functions
  */
private[querygeneration] object WindowStatement {

  /** Used mainly by QueryGeneration.convertExpression. This matches
    * a tuple of (Expression, Seq[Attribute]) representing the expression to
    * be matched and the fields that define the valid fields in the current expression
    * scope, respectively.
    *
    * @param expAttr A pair-tuple representing the expression to be matched and the
    *                attribute fields.
    * @return An option containing the translated SQL, if there is a match, or None if there
    *         is no match.
    */
  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[RedshiftSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      // Handle Window Expression.
      case WindowExpression(func, spec) =>
        func match {
          // These functions in Redshift support a window frame.
          // Note that pushdown for these may or may not yet be supported in the connector.
          case _: Rank | _: DenseRank | _: PercentRank =>
            convertStatement(func, fields) + " OVER " + windowBlock(
              spec,
              fields,
              useWindowFrame = true
            )

          // Disable window function pushdown if
          // 1. The function are both window function and aggregate function
          // 2. User specifies Window Frame. But there is no way to detect
          //    whether the window function has Window Frame or not.
          //    So we check whether ORDER BY is specified instead.
          //    This may disable the window function which has ORDER BY but
          //    without Window Frame. This is not an issue because it still works.
          case _: AggregateExpression if spec.orderSpec.nonEmpty =>
            null

          // These do not.
          case _ =>
            convertStatement(func, fields) + " OVER " + windowBlock(
              spec,
              fields,
              useWindowFrame = false
            )
        }

      // Handle supported window function
      case _: RowNumber | _: Rank | _: DenseRank =>
        ConstantString(expr.prettyName.toUpperCase) + "()"

      // PercentRank not be pushdown to redshift
      case _: PercentRank => null

      case _ => null
    })
  }

  // Handle window block.
  private final def windowBlock(
                                 spec: WindowSpecDefinition,
                                 fields: Seq[Attribute],
                                 useWindowFrame: Boolean
                               ): RedshiftSQLStatement = {
    val partitionBy =
      if (spec.partitionSpec.isEmpty) {
        EmptyRedshiftSQLStatement()
      } else {
        ConstantString("PARTITION BY") +
          mkStatement(spec.partitionSpec.map(convertStatement(_, fields)), ",")
      }

    val orderBy =
      if (spec.orderSpec.isEmpty) {
        EmptyRedshiftSQLStatement()
      } else {
        ConstantString("ORDER BY") +
          mkStatement(spec.orderSpec.map(convertStatement(_, fields)), ",")
      }

    val fromTo =
      if (!useWindowFrame || spec.orderSpec.isEmpty) ""
      else " " + spec.frameSpecification.sql

    blockStatement(partitionBy + orderBy + fromTo)
  }

}
