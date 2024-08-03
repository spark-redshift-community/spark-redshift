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

import io.github.spark_redshift_community.spark.redshift.pushdown.{ConstantString, RedshiftSQLStatement}
import org.apache.spark.sql.catalyst.expressions.{Ascii, Attribute, Concat, Expression, Length, Like, Literal, Lower, StringLPad, StringRPad, StringTranslate, StringTrim, StringTrimLeft, StringTrimRight, Substring, Upper}

/** Extractor for boolean expressions (return true or false). */
private[querygeneration] object StringStatement {
  // ESCAPE CHARACTER for LIKE is supported from Spark 3.0
  // The default escape character comes from the constructor of Like class.
  val DEFAULT_LIKE_ESCAPE_CHAR: Char = '\\'

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

    val result = Option(expr match {
      case _: Ascii | _: Lower | _: StringLPad | _: StringRPad |
          _: StringTranslate | _: Upper | _: Length =>
        ConstantString(expr.prettyName.toUpperCase) +
          blockStatement(convertStatements(fields, expr.children: _*))

      case _: StringTrim | _: StringTrimLeft | _: StringTrimRight =>
        if (expr.children.length == 1) {
          // If a trim string wasn't provided, explicitly make it a space character
          // so that Redshift does not also trim tabs or other whitespace characters.
          // See SIM [Redshift-7056] for further details.
          ConstantString(expr.prettyName.toUpperCase) +
            blockStatement(convertStatements(fields, expr.children: _*) + ", ' '")
        } else {
          ConstantString(expr.prettyName.toUpperCase) +
            blockStatement(convertStatements(fields, expr.children: _*))
        }

      case Substring(_, pos, _) =>
        // Only support pushdown when the starting position is a positive number as Spark
        // and Redshift have differences in interpreting non-positive starting indices.
        pos match {
          case (l: Literal) => l.value match {
            case (i: Integer) if (i.intValue() > 0) =>
              ConstantString("SUBSTRING") +
              blockStatement(convertStatements(fields, expr.children: _*))
            case _ => null
          }
          case _ => null
        }

      case Concat(children) =>
        val rightSide =
          if (children.length > 2) Concat(children.drop(1)) else children(1)
        ConstantString("CONCAT") + blockStatement(
          convertStatement(children.head, fields) + "," +
            convertStatement(rightSide, fields)
        )

      // ESCAPE Char is supported from Spark 3.0
      case Like(left, right, escapeChar) =>
        val escapeClause =
          if (escapeChar == DEFAULT_LIKE_ESCAPE_CHAR) {
            ""
          } else {
            s"ESCAPE '${escapeChar}'"
          }
        // Cast the left string into a varchar to ensure fixed-length strings are right-trimmed
        // since Redshift doesn't do this automatically for LIKE expressions. We want the push-down
        // behavior to always match the non-push-down behavior which trims fixed-length strings.
        ConstantString("CAST") + blockStatement(convertStatement(left, fields) + "AS VARCHAR") +
          ConstantString("LIKE") + convertStatement(right, fields) + escapeClause

      case _ => null
    })

    // If there are no matches, check if there are any version-specific implementations
    // that can push the query down.
    result match {
      case None =>
        (expr, fields) match {
          case StringStatementExtensions(stmt) => Option(stmt)
          case _ => None
        }
      case _ => result
    }
  }
}
