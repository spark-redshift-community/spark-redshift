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

import io.github.spark_redshift_community.spark.redshift.pushdown.{ConstantString, RedshiftSQLStatement}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Expression, ToPrettyString}
import org.apache.spark.sql.types._

private[querygeneration] object StringStatementExtensions {
  def unapply(expAttr: (Expression, Seq[Attribute])): Option[RedshiftSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {

      case ToPrettyString(child: Expression, timeZoneId: Option[String]) =>
        ConstantString("CASE WHEN") +
          convertStatement(child, fields) +
          ConstantString("IS NULL THEN 'NULL' ELSE") +
          convertStatement(Cast(child, StringType, timeZoneId), fields) +
          ConstantString("END")

      case _ => null
    })
  }
}
