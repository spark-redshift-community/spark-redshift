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

import io.github.spark_redshift_community.spark.redshift.pushdown.{ConstantString, EmptyRedshiftSQLStatement, IntVariable, RedshiftSQLStatement}
import io.github.spark_redshift_community.spark.redshift.{RedshiftFailMessage, RedshiftPushdownUnsupportedException}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, CaseWhen, Coalesce, Descending, Exists, Expression, If, In, InSet, InSubquery, Literal, MakeDecimal, NullsFirst, NullsLast, SortOrder, UnscaledValue}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** Extractors for everything else. */
private[querygeneration] object MiscStatement {

  def unapply(
               expAttr: (Expression, Seq[Attribute])
             ): Option[RedshiftSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      case Alias(child: Expression, name: String) =>
        blockStatement(convertStatement(child, fields), name)

      // Context for removing the ansi condition check:
      // Snowflake added this ansi check during the Spark 3.2 upgrade
      // as a precaution for Cast push down, even though the ansi property existed
      // before 3.2. Cast push down worked regardless of ansi settings, and tests
      // pass without the check. We’re removing it since it’s redundant. See where
      // Snowflake introduced this: https://github.com/snowflakedb/spark-snowflake/pull/408
      case CastExtractor(child, t, _) =>
        getCastType(t) match {
          case Some(cast) =>
            // For known unsupported data conversion, raise exception to break the
            // pushdown process.
            (child.dataType, t) match {
              case (_: DateType | _: TimestampType,
              _: IntegerType | _: LongType | _: FloatType | _: DoubleType | _: DecimalType) =>
                // This exception will not break the connector. It will be caught in
                // QueryBuilder.treeRoot.
                throw new RedshiftPushdownUnsupportedException(
                  RedshiftFailMessage.FAIL_PUSHDOWN_UNSUPPORTED_CONVERSION,
                  s"Don't support to convert ${child.dataType} column to $t type",
                  "",
                  false)
              case _ =>
            }

            (child.dataType, t) match {
              // Manually cast booleans to strings as Redshift does not support it [Redshift-7625]
              case (_: BooleanType, _: StringType) =>
                ConstantString("CASE") +
                  convertStatement(child, fields) +
                  ConstantString("WHEN TRUE THEN 'true' WHEN FALSE THEN 'false' ELSE null END")
              // casting complex types does not work in redshift
              case (_: StructType | _: MapType | _: ArrayType, _) =>
                throw new RedshiftPushdownUnsupportedException(
                  RedshiftFailMessage.FAIL_PUSHDOWN_UNSUPPORTED_CONVERSION,
                  s"Converting complex datatype ${child.dataType} to ${t} is not supported",
                  "",
                  false
                )
              case _ =>
                ConstantString("CAST") +
                  blockStatement(convertStatement(child, fields) + "AS" + cast)
            }
          case _ => convertStatement(child, fields)
        }

      case If(child, trueValue, falseValue) =>
        ConstantString("CASE WHEN") +  convertStatement(child, fields) +
          ConstantString("THEN") + convertStatement(trueValue, fields) +
          ConstantString("ELSE") + convertStatement(falseValue, fields) +
          ConstantString("END")

      case In(child, list) =>
        blockStatement(
          convertStatement(child, fields) + "IN" +
            blockStatement(convertStatements(fields, list: _*))
        )

      case InSet(child, hset) =>
        convertStatement(In(child, setToExpr(hset)), fields)

      case MakeDecimal(child, precision, scale, _) =>
        ConstantString("CAST") + blockStatement(
          blockStatement(convertStatement(child, fields) + "/ POW(10," +
            IntVariable(Some(scale)) + ")"
          ) + " AS DECIMAL(" + IntVariable(Some(precision)) + "," +
            IntVariable(Some(scale)) + ")"
        )

      case SortOrder(child, Ascending, NullsFirst, _) =>
        blockStatement(convertStatement(child, fields)) + "ASC NULLS FIRST"
      case SortOrder(child, Ascending, NullsLast, _) =>
        blockStatement(convertStatement(child, fields)) + "ASC NULLS LAST"
      case SortOrder(child, Descending, NullsFirst, _) =>
        blockStatement(convertStatement(child, fields)) + "DESC NULLS FIRST"
      case SortOrder(child, Descending, NullsLast, _) =>
        blockStatement(convertStatement(child, fields)) + "DESC NULLS LAST"

      // Spark 3.2 introduces below new field
      //   joinCond: Seq[Expression] = Seq.empty
      // So support to pushdown, if joinCond is empty.
      // https://github.com/apache/spark/commit/806da9d6fae403f88aac42213a58923cf6c2cb05
      // To support spark 3.1
      //      case ScalarSubquery(subquery, _, _) =>
      case ScalarSubqueryExtractor(subquery, _, _, joinCond) if joinCond.isEmpty =>
        blockStatement(new QueryBuilder(subquery).statement)

      case InSubquery(values, subQuery) if values.size == 1 =>
        convertStatement(values.head, fields) + ConstantString("IN") +
          convertSubQuery(fields, subQuery.plan, subQuery.joinCond)

      case Exists(subQuery, _, _, joinCond, _) =>
        ConstantString("EXISTS").toStatement + convertSubQuery(fields, subQuery, joinCond)

      case UnscaledValue(child) =>
        child.dataType match {
          case d: DecimalType =>
            blockStatement(
              convertStatement(child, fields) + "* POW(10," + IntVariable(
                Some(d.scale)
              ) + ")"
            )
          case _ => null
        }

      case CaseWhen(branches, elseValue) =>
        ConstantString("CASE") +
          mkStatement(branches.map(conditionValue => {
            ConstantString("WHEN") + convertStatement(conditionValue._1, fields) +
              ConstantString("THEN") + convertStatement(conditionValue._2, fields)
          }
          ), " ") + { elseValue match {
          case Some(value) => ConstantString("ELSE") + convertStatement(value, fields)
          case None => EmptyRedshiftSQLStatement()
        }} + ConstantString("END")

      case Coalesce(columns) =>
        ConstantString(expr.prettyName.toUpperCase) +
          blockStatement(
            mkStatement(
              columns.map(convertStatement(_, fields)),
              ", "
            )
          )

      case _ => null
    })
  }

  def convertSubQuery(fields: Seq[Attribute], subQuery: LogicalPlan, joinCond: Seq[Expression]):
  RedshiftSQLStatement = {
    val subQueryBuilder = new QueryBuilder(subQuery)
    val suffix =
      if (joinCond.nonEmpty) {
        // When there is a join condition, we need a separate query builder and helper
        // for retrieving the aliases for the join conditions.
        val subQueryHelper =
            QueryHelper(
              children = Seq(subQueryBuilder.treeRoot),
              projections = None,
              outputAttributes = None,
              alias = "")
        ConstantString("WHERE") + mkStatement(
          joinCond.map(convertStatement(_, fields ++ subQueryHelper.pureColSet)), "AND")
      } else {
        EmptyRedshiftSQLStatement()
      }

    blockStatement(subQueryBuilder.statement + suffix)
  }

  final def setToExpr(set: Set[Any]): Seq[Expression] = {
    set.map {
      case d: Decimal => Literal(d, DecimalType(d.precision, d.scale))
      case s @ (_: String | _: UTF8String) => Literal(s, StringType)
      case i: Integer => Literal(i, IntegerType)
      case d: Double => Literal(d, DoubleType)
      case e: Expression => e
      case default =>
        // This exception will not break the connector. It will be caught in
        // QueryBuilder.treeRoot.
        throw new RedshiftPushdownUnsupportedException(
          RedshiftFailMessage.FAIL_PUSHDOWN_SET_TO_EXPR,
          s"${default.getClass.getSimpleName} @ MiscStatement.setToExpr",
          "Class " + default.getClass.getName + " is not supported in the 'in()' expression",
          false
        )
    }.toSeq
  }
}