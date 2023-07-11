/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import io.github.spark_redshift_community.spark.redshift.pushdown.querygeneration.ComplexTypeStatementCommon.{cannotPushdownStrictIndexOperatorExpression, handleComplexTypeSubfieldConversion}
import io.github.spark_redshift_community.spark.redshift.pushdown.{Identifier, RedshiftSQLStatement}
import io.github.spark_redshift_community.spark.redshift.{RedshiftFailMessage, RedshiftPushdownUnsupportedException}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GetArrayItem, GetMapValue, GetStructField, Literal}
import org.apache.spark.sql.types.{LongType, StringType}

/* Extractors for projections of complex types (Structs, Arrays, Maps) */
private[querygeneration] object ComplexTypeStatement {
  def unapply(
               expAttr: (Expression, Seq[Attribute])
             ): Option[RedshiftSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      case GetStructField(child, _, _) => {
        handleComplexTypeSubfieldConversion(expr,
          convertStatement(child, fields) + "." +
            Identifier(expr.asInstanceOf[GetStructField].extractFieldName))
      }
      // redshift has lax super semantics by default, let spark handle strict case
      case GetArrayItem(_, _, true) => throw cannotPushdownStrictIndexOperatorExpression(expr)
      // spark will ensure ordinal datatype is one of byte, short, int or long
      // redshift does not support bigint (LongType) as an array index
      case GetArrayItem(_, ordinal, _) if ordinal.dataType == LongType =>
        throw new RedshiftPushdownUnsupportedException(
          RedshiftFailMessage.FAIL_PUSHDOWN_STATEMENT,
          s"${expr.prettyName} @ ComplexTypeStatement",
          s"${ordinal.dataType} is not supported as an array index",
          true)
      case GetArrayItem(child, ordinal, _) => {
        handleComplexTypeSubfieldConversion(expr,
          convertStatement(child, fields) + "[" + convertStatement(ordinal, fields) + "]"
        )
      }
      // redshift only supports string literals as map keys
      case GetMapValue(child, key: Literal) if key.dataType == StringType => {
        val convertedLiteral = Identifier(key.value.toString)
        handleComplexTypeSubfieldConversion(expr,
          convertStatement(child, fields) + "." + convertedLiteral)
      }
      // This case applies to GetMapValue expressions that don't have literal keys of StringType
      case GetMapValue(_, key) =>
        throw new RedshiftPushdownUnsupportedException(
          RedshiftFailMessage.FAIL_PUSHDOWN_STATEMENT,
          s"${expr.prettyName} @ ComplexTypeStatement",
          s"${key.prettyName} of type ${key.dataType} is not supported as a map key",
          true)
      case _ => null
    })
  }
}
