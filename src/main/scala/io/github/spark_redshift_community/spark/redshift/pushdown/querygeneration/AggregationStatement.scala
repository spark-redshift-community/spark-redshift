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

import io.github.spark_redshift_community.spark.redshift.{RedshiftFailMessage, RedshiftPushdownUnsupportedException, Utils}
import io.github.spark_redshift_community.spark.redshift.pushdown.{ConstantString, EmptyRedshiftSQLStatement, RedshiftSQLStatement}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.{BooleanType, DecimalType, DoubleType, FloatType}

import scala.language.postfixOps

/**
  * Extractor for aggregate-style expressions.
  */
private[querygeneration] object AggregationStatement {
  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[RedshiftSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    expr match {
      case _: AggregateExpression =>
        // Take only the first child, as all of the functions below have only one.
        expr.children.headOption.flatMap(agg_fun => {
          val distinct: RedshiftSQLStatement =
            if (expr.sql contains "(DISTINCT ") ConstantString("DISTINCT") !
            else EmptyRedshiftSQLStatement()
          Option(agg_fun match {
            case Max(child) if child.dataType == BooleanType =>
              throw new RedshiftPushdownUnsupportedException(
                RedshiftFailMessage.FAIL_PUSHDOWN_AGGREGATE_EXPRESSION,
                s"${agg_fun.prettyName} @ AggregationStatement",
                "MAX(Boolean) is not defined in redshift",
                true
              )
            case Min(child) if child.dataType == BooleanType =>
              throw new RedshiftPushdownUnsupportedException(
                RedshiftFailMessage.FAIL_PUSHDOWN_AGGREGATE_EXPRESSION,
                s"${agg_fun.prettyName} @ AggregationStatement",
                "MIN(Boolean) is not defined in redshift",
                true
              )
            case _: Count | _: Max | _: Min | _: Sum | _: StddevSamp | _: StddevPop |
                 _: VariancePop | _: VarianceSamp =>
              ConstantString(agg_fun.prettyName.toUpperCase) +
                blockStatement(
                  distinct + convertStatements(fields, agg_fun.children: _*)
                )
            case avg: Average =>
              // Type casting is needed if column type is short, int, long or decimal with scale 0.
              // Because Redshift and Spark have different behavior on AVG on these types, type
              // should be casted to float to keep result numbers after decimal point.
              val doCast: Boolean = avg.child.dataType match {
                case _: FloatType | DoubleType => false
                case d: DecimalType if d.scale != 0 => false
                case _ => true
              }
              ConstantString(agg_fun.prettyName.toUpperCase) +
                blockStatement(
                  distinct + convertStatements(fields, agg_fun.children: _*) +
                    (if (doCast) ConstantString("::FLOAT") ! else EmptyRedshiftSQLStatement())
                )
            case _ =>
              // This exception is not a real issue. It will be caught in
              // QueryBuilder.treeRoot.
              throw new RedshiftPushdownUnsupportedException(
                RedshiftFailMessage.FAIL_PUSHDOWN_AGGREGATE_EXPRESSION,
                s"${agg_fun.prettyName} @ AggregationStatement",
                agg_fun.sql,
                false
              )
          })
        })
      case _ => None
    }
  }
}
