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

import io.github.spark_redshift_community.spark.redshift.pushdown.{ConstantString, Identifier, RedshiftSQLStatement}
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
      case GetMapValueExtractor(_, _, true) =>
        throw cannotPushdownStrictIndexOperatorExpression(expr)
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
      case GetMapValueExtractor(child, key: Literal, _) if key.dataType == StringType => {
        val convertedLiteral = Identifier(key.value.toString)
        handleComplexTypeSubfieldConversion(expr,
          convertStatement(child, fields) + "." + convertedLiteral)
      }
      // This case applies to GetMapValue expressions that don't have literal keys of StringType
      case GetMapValueExtractor(_, key, _) =>
        throw new RedshiftPushdownUnsupportedException(
          RedshiftFailMessage.FAIL_PUSHDOWN_STATEMENT,
          s"${expr.prettyName} @ ComplexTypeStatement",
          s"${key.prettyName} of type ${key.dataType} is not supported as a map key",
          true)
      case _ => null
    })
  }

  /**
   * Creates an exception explaining that this expression cannot be pushed down due to
   * redshift not having a mechanism to support fail on error. Redshift does have a similar
   * setting described here https://docs.aws.amazon.com/redshift/latest/dg/super-configurations.html
   * called navigate_super_null_on_error but it cannot be enabled mid query.
   * @param expr expression to create the exception for
   */
  def cannotPushdownStrictIndexOperatorExpression(expr: Expression)
  : RedshiftPushdownUnsupportedException = {
    new RedshiftPushdownUnsupportedException(
      RedshiftFailMessage.FAIL_PUSHDOWN_STATEMENT,
      s"${expr.prettyName} @ ComplexTypeStatement",
      s"failOnError in ${expr.prettyName} expression is not supported",
      true
    )
  }

  /**
   * Handle converting a complex type's subfield to the correct redshift type.
   * Generally this converts expressions like `a[0]` into `a[0]::float4` if `a` was an array of
   * floats or converts expressions like `a.hello` into `a.hello::short` if `a` was a struct with
   * hello as a short. However this must also handle a special case when converting subfields of
   * type String so that it matches the behavior of spark when pushdown is not in use. If pushdown
   * is not in use and the column `a` contains two entries `{"hello": "world"}` and
   * `{"hello":{"hi":1}}` then when a query like `select a.hello from table_containing_a` is issued
   * the resulting dataframe will contain a row for the string `world` and a row for the string
   * `{"hi":1}`. To match this behavior the pushdown query must convert `a.hello` to varchar when
   * `is_varchar` is true and must otherwise use `json_serialize(a.hello)` as the result. This can
   * be illustrated with three queries using the data described above followed by their results:
   * `select a.hello from table_containing_a` -> `"hello"`, `{"hi":1}`
   * `select a.hello::varchar from table_containing_a` -> `hello`, `NULL`
   * `SELECT case when is_varchar(a.hello) then a.hello::varchar else json_serialize(a.hello) end
   * from table_containing_a` -> `hello`, `{"hi":1}`
   * @param expression expression that generated the passed redshiftSQLStatement
   * @param redshiftSQLStatement statement to adjust for special case
   * @return RedshiftSQLStatement
   */
  def handleComplexTypeSubfieldConversion(expression: Expression,
                                          redshiftSQLStatement: RedshiftSQLStatement)
  : RedshiftSQLStatement = {
    blockStatement(expression.dataType match {
      case StringType => ConstantString("case when is_varchar(") + redshiftSQLStatement + ")" +
        "then " + redshiftSQLStatement + "::varchar " +
        "else json_serialize(" + redshiftSQLStatement + ") end"
      case _ => getCastType(expression.dataType).
        map(typeName => redshiftSQLStatement + s"::$typeName").getOrElse(redshiftSQLStatement)
    })
  }
}
