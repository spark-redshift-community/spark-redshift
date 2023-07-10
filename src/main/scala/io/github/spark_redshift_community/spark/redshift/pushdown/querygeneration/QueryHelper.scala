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

import io.github.spark_redshift_community.spark.redshift.RedshiftPushdownException
import io.github.spark_redshift_community.spark.redshift.pushdown.{EmptyRedshiftSQLStatement, RedshiftSQLStatement}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, NamedExpression}

/**
  * Helper class to maintain the fields, output, and projection expressions of
  * a RedshiftQuery. This may be refactored into the RedshiftQuery abstract class.
  *
  * @constructor Creates an instance of a QueryHelper. Created by every RedshiftQuery.
  * @param children A sequence containing the child queries. May be empty in the case
  *                 of a source (bottom-level) query, contain one element (for most
  *                 unary operations), or contain two elements (for joins, etc.).
  * @param projections Contains optional projection columns for this query.
  * @param outputAttributes Optional manual override for output.
  * @param alias The alias for this subquery.
  * @param conjunctionStatement Conjunction phrase to be used in between subquery children,
  *                             or simple phrase when there are no subqueries.
  */
private[querygeneration] case class QueryHelper(
     children: Seq[RedshiftQuery],
     projections: Option[Seq[NamedExpression]] = None,
     outputAttributes: Option[Seq[Attribute]],
     alias: String,
     conjunctionStatement: RedshiftSQLStatement = EmptyRedshiftSQLStatement(),
     fields: Option[Seq[Attribute]] = None,
     // For some query clauses we may override the outputAttributes, but will
     // need a different set of resolvable attributes to be visible to the parent
     // query clause, e.g., in UnionQuery
     visibleAttributeOverride: Option[Seq[Attribute]] = None
) {

  val colSet: Seq[Attribute] =
    if (fields.isEmpty) {
      children.foldLeft(Seq.empty[Attribute])(
        (x, y) => {
          val attrs =
            if (y.helper.visibleAttributeOverride.isEmpty) {
              y.helper.outputWithQualifier
            } else {
              y.helper.visibleAttributeOverride.get
            }

          x ++ attrs
        }
      )
    } else {
      fields.get
    }

  val pureColSet: Seq[Attribute] =
    children.foldLeft(Seq.empty[Attribute])((x, y) => x ++ y.helper.output)

  val processedProjections: Option[Seq[NamedExpression]] = projections
    .map(
      p =>
        p.map(
          e =>
            colSet.find(c => c.exprId == e.exprId) match {
              case Some(a) if e.isInstanceOf[AttributeReference] =>
                AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
                  a.exprId
                )
              case _ => e
          }
      )
    )
    .map(p => renameColumns(p, alias))

  val columns: Option[RedshiftSQLStatement] =
    processedProjections.map(
      p => mkStatement(p.map(convertStatement(_, colSet)), ",")
    )

  lazy val output: Seq[Attribute] = {
    outputAttributes.getOrElse(
      processedProjections.map(p => p.map(_.toAttribute)).getOrElse {
        if (children.isEmpty) {
          throw new RedshiftPushdownException(
            "Query output attributes must not be empty when it has no children."
          )
        } else {
          children
            .foldLeft(Seq.empty[Attribute])((x, y) => x ++ y.helper.output)
        }
      }
    )
  }

  var outputWithQualifier: Seq[AttributeReference] = output.map(
    a =>
      AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
        a.exprId,
        Seq[String](alias)
    )
  )

  // For OUTER JOIN, the column's nullability may need to be modified as true
  def nullableOutputWithQualifier: Seq[AttributeReference] = output.map(
    a =>
      AttributeReference(a.name, a.dataType, nullable = true, a.metadata)(
        a.exprId,
        Seq[String](alias)
      )
  )

  val sourceStatement: RedshiftSQLStatement =
    if (children.nonEmpty) {
      mkStatement(children.map(_.getStatement(true)), conjunctionStatement)
    } else {
      conjunctionStatement
    }
}
