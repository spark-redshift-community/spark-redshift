/*
 * Copyright 2015 Databricks
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

package io.github.spark_redshift_community.spark.redshift

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
 * Helper methods for pushing filters into Redshift queries.
 */
private[redshift] object FilterPushdown {
  /**
   * Build a SQL WHERE clause for the given filters. If a filter cannot be pushed down then no
   * condition will be added to the WHERE clause. If none of the filters can be pushed down then
   * an empty string will be returned.
   *
   * @param schema the schema of the table being queried
   * @param filters an array of filters, the conjunction of which is the filter condition for the
   *                scan.
   * @param escapeQuote escape single quote if it is true. Generally, it the query is a subquery
   *                    (like in UNLOAD command), we need escape the quote.
   */
  def buildWhereClause(schema: StructType, filters: Seq[Filter],
                       escapeQuote: Boolean = false): String = {
    val filterExpressions = filters.flatMap(f => buildFilterExpression(schema, f, escapeQuote))
      .mkString(" AND ")
    if (filterExpressions.isEmpty) "" else "WHERE " + filterExpressions
  }

  /**
   * Attempt to convert the given filter into a SQL expression. Returns None if the expression
   * could not be converted.
   *
   * If escapeQuote is true, it will escape the single quote (that encloses the attribute) with
   * two single quotes. Normally, this is used when the query is a sub-query in UNLOAD. Reference:
   * "If your query contains quotation marks (for example to enclose literal values), put the
   * literal between two sets of single quotation marks—you must also enclose the query between
   * single quotation marks" per doc https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html
   */
  def buildFilterExpression(schema: StructType, filter: Filter, escapeQuote: Boolean = true):
  Option[String] = {
    def buildComparison(attr: String, value: Any, comparisonOp: String, escapeQuote: Boolean):
    Option[String] = {
      getTypeForAttribute(schema, attr).map { dataType =>
        val sqlEscapedValue: String = dataType match {
          case StringType =>
            if (escapeQuote) {
              s"''${value.toString.replace("'", "\\'\\'")}''"
            } else {
              s"'${value.toString.replace ("'", "\\'\\'")}'"
            }
          case DateType =>
            if (escapeQuote) {
              s"''${value.asInstanceOf[Date]}''"
            } else {
              s"'${value.asInstanceOf[Date]}'"
            }
          case TimestampType =>
            if (escapeQuote) {
              s"''${value.asInstanceOf[Timestamp]}''"
            } else {
              s"'${value.asInstanceOf[Timestamp]}'"
            }
          case _ if value.isInstanceOf[Float] => s"$value::float4"
          case _ => value.toString
        }
        s""""$attr" $comparisonOp $sqlEscapedValue"""
      }
    }

    filter match {
      case EqualTo(attr, value) => buildComparison(attr, value, "=", escapeQuote)
      case LessThan(attr, value) => buildComparison(attr, value, "<", escapeQuote)
      case GreaterThan(attr, value) => buildComparison(attr, value, ">", escapeQuote)
      case LessThanOrEqual(attr, value) => buildComparison(attr, value, "<=", escapeQuote)
      case GreaterThanOrEqual(attr, value) => buildComparison(attr, value, ">=", escapeQuote)
      case IsNotNull(attr) =>
        getTypeForAttribute(schema, attr).map(dataType => s""""$attr" IS NOT NULL""")
      case IsNull(attr) =>
        getTypeForAttribute(schema, attr).map(dataType => s""""$attr" IS NULL""")
      case _ => None
    }
  }

  /**
   * Use the given schema to look up the attribute's data type. Returns None if the attribute could
   * not be resolved.
   */
  private def getTypeForAttribute(schema: StructType, attribute: String): Option[DataType] = {
    if (schema.fieldNames.contains(attribute)) {
      Some(schema(attribute).dataType)
    } else {
      None
    }
  }
}
