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

package io.github.spark_redshift_community.spark.redshift.pushdown

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, NamedExpression}
import org.apache.spark.sql.types.MetadataBuilder
import org.slf4j.LoggerFactory

import scala.language.postfixOps

/** Package-level static methods and variable constants. These includes helper functions for
  * adding and converting expressions, formatting blocks and identifiers, logging, and
  * formatting SQL.
  */
package object querygeneration {

  private[querygeneration] final val ORIG_NAME = "ORIG_NAME"

  /** This wraps all identifiers with the following symbol. */
  private final val identifier = "\""

  private[querygeneration] final val log = LoggerFactory.getLogger(getClass)

  private[querygeneration] final def blockStatement(
    stmt: RedshiftSQLStatement
  ): RedshiftSQLStatement =
    ConstantString("(") + stmt + ")"

  private[querygeneration] final def blockStatement(
    stmt: RedshiftSQLStatement,
    alias: String
  ): RedshiftSQLStatement =
    blockStatement(stmt) + "AS" + wrapStatement(alias)

  private[querygeneration] final def buildAliasStatement(
                                                     stmt: RedshiftSQLStatement,
                                                     alias: String
                                                   ): RedshiftSQLStatement =
    stmt + "AS" + wrapStatement(alias)

  /** This adds an attribute as part of a SQL expression, searching in the provided
    * fields for a match, so the subquery qualifiers are correct.
    *
    * @param attr The Spark Attribute object to be added.
    * @param fields The Seq[Attribute] containing the valid fields to be used in the expression,
    *               usually derived from the output of a subquery.
    * @return A RedshiftSQLStatement representing the attribute expression.
    */
  private[querygeneration] final def addAttributeStatement(
    attr: Attribute,
    fields: Seq[Attribute]
  ): RedshiftSQLStatement =
    fields.find(e => e.exprId == attr.exprId) match {
      case Some(resolved) =>
        qualifiedAttributeStatement(resolved.qualifier, resolved.name)
      case None => qualifiedAttributeStatement(attr.qualifier, attr.name)
    }

  /** Qualifies identifiers with that of the subquery to which it belongs */
  private[querygeneration] final def qualifiedAttribute(
    alias: Seq[String],
    name: String
  ): String = {
    val str =
      if (alias.isEmpty) ""
      else alias.map(wrap).mkString(".") + "."

    if (name.startsWith("\"") && name.endsWith("\"")) str + name
    else str + wrapObjectName(name)
  }

  private[querygeneration] final def qualifiedAttributeStatement(
    alias: Seq[String],
    name: String
  ): RedshiftSQLStatement =
    ConstantString(qualifiedAttribute(alias, name)) !

  private[querygeneration] final def wrapObjectName(name: String): String = wrap(name)


  private[querygeneration] final def wrap(name: String): String = {
    identifier + name.toUpperCase + identifier
  }

  private[querygeneration] final def wrapStatement(
    name: String
  ): RedshiftSQLStatement =
    ConstantString(identifier + name.toUpperCase + identifier) !

  /** This performs the conversion from Spark expressions to SQL runnable by Redshift.
    * We should have as many entries here as possible, or the translation will not be
    * able ot happen.
    *
    * @note (A MatchError may be raised for unsupported Spark expressions).
    */
  private[querygeneration] final def convertStatement(
    expression: Expression,
    fields: Seq[Attribute]
  ): RedshiftSQLStatement = {
    (expression, fields) match {
      case AggregationStatement(stmt) => stmt
      case BasicStatement(stmt) => stmt
      case BooleanStatement(stmt) => stmt
      case DateStatement(stmt) => stmt
      case MiscStatement(stmt) => stmt
      case NumericStatement(stmt) => stmt
      case StringStatement(stmt) => stmt
      case ComplexTypeStatement(stmt) => stmt
//      case WindowStatement(stmt) => stmt  // Window functions are not supported
      case UnsupportedStatement(stmt) => stmt // UnsupportedStatement must be the last CASE
    }
  }

  private[querygeneration] final def convertStatements(
    fields: Seq[Attribute],
    expressions: Expression*
  ): RedshiftSQLStatement =
    mkStatement(expressions.map(convertStatement(_, fields)), ",")

  private[querygeneration] def renameColumns(
    origOutput: Seq[NamedExpression],
    alias: String
  ): Seq[NamedExpression] = {

    val col_names = Iterator.from(0).map(n => s"COL_$n")

    origOutput.map { expr =>
      val metadata =
        if (!expr.metadata.contains(ORIG_NAME)) {
          new MetadataBuilder()
            .withMetadata(expr.metadata)
            .putString(ORIG_NAME, expr.name)
            .build
        } else expr.metadata

      val altName = s"""${alias}_${col_names.next()}"""

      expr match {
        case a @ Alias(child: Expression, name: String) =>
          val meta = child.references.size match {
            case 1 =>
              val r = child.references.head
              if (child.dataType == r.dataType && r.metadata.contains("redshift_type")) {
                new MetadataBuilder()
                  .withMetadata(metadata)
                  .putString("redshift_type", r.metadata.getString("redshift_type"))
                  .build
              } else metadata
            case _ => metadata
          }
          Alias(child, altName)(a.exprId, Seq.empty[String], Some(meta))
        case _ =>
          Alias(expr, altName)(expr.exprId, Seq.empty[String], Some(metadata))
      }
    }
  }

  final def mkStatement(
    seq: Seq[RedshiftSQLStatement],
    delimiter: RedshiftSQLStatement
  ): RedshiftSQLStatement =
    seq.foldLeft(EmptyRedshiftSQLStatement()) {
      case (left, stmt) =>
        if (left.isEmpty) stmt else left + delimiter + stmt
    }

  final def mkStatement(seq: Seq[RedshiftSQLStatement],
                        delimiter: String): RedshiftSQLStatement =
    mkStatement(seq, ConstantString(delimiter) !)
}
