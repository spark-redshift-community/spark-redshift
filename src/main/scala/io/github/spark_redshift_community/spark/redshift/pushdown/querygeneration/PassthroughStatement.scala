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

import io.github.spark_redshift_community.spark.redshift.pushdown._
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.expressions.{Attribute, CheckOverflowInTableInsert, Expression}

import scala.language.postfixOps

/**
 * Extractor for expressions that are ignored.
 */
private[querygeneration] object PassthroughStatement {

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
      case CheckOverflowInTableInsert(child, _) => convertStatement(child, fields)
      case _ => null
    })
  }
}
