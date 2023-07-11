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

import io.github.spark_redshift_community.spark.redshift.pushdown.querygeneration.StringStatement.DEFAULT_LIKE_ESCAPE_CHAR
import io.github.spark_redshift_community.spark.redshift.pushdown.{ConstantString, RedshiftSQLStatement}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Concat, Contains, EndsWith, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Like, Literal, Not, StartsWith}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

/** Extractor for boolean expressions (return true or false). */
private[querygeneration] object BooleanStatement {
  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[RedshiftSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      case In(child, list) if list.forall(_.isInstanceOf[Literal]) =>
        convertStatement(child, fields) + "IN" +
          blockStatement(convertStatements(fields, list: _*))
      case IsNull(child) =>
        blockStatement(convertStatement(child, fields) + "IS NULL")
      case IsNotNull(child) =>
        blockStatement(convertStatement(child, fields) + "IS NOT NULL")
      case Not(child) => {
        child match {
          case EqualTo(left, right) =>
            blockStatement(
              convertStatement(left, fields) + "!=" +
                convertStatement(right, fields)
            )
          // NOT ( GreaterThanOrEqual, LessThanOrEqual,
          // GreaterThan and LessThan ) have been optimized by spark
          // and are handled by BinaryOperator in BasicStatement.
          case GreaterThanOrEqual(left, right) =>
            convertStatement(LessThan(left, right), fields)
          case LessThanOrEqual(left, right) =>
            convertStatement(GreaterThan(left, right), fields)
          case GreaterThan(left, right) =>
            convertStatement(LessThanOrEqual(left, right), fields)
          case LessThan(left, right) =>
            convertStatement(GreaterThanOrEqual(left, right), fields)
          case _ =>
            ConstantString("NOT") +
              blockStatement(convertStatement(child, fields))
        }
      }
      // Cast the left string into a varchar to ensure fixed-length strings are right-trimmed
      // since Redshift doesn't do this automatically for LIKE expressions. We want the push-down
      // behavior to always match the non-push-down behavior which trims fixed-length strings.
      case Contains(left, right) =>
        blockStatement(convertStatement(Like(left, Concat(Seq(Literal("%"), right, Literal("%"))),
          DEFAULT_LIKE_ESCAPE_CHAR), fields))
      case EndsWith(left, right) =>
        blockStatement(convertStatement(Like(left, Concat(Seq(Literal("%"), right)),
          DEFAULT_LIKE_ESCAPE_CHAR), fields))
      case StartsWith(left, right) =>
        blockStatement(convertStatement(Like(left, Concat(Seq(right, Literal("%"))),
          DEFAULT_LIKE_ESCAPE_CHAR), fields))
      case _ => null
    })
  }
}
