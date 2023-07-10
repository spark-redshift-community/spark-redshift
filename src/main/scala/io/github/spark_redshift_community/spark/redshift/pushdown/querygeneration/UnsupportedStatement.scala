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

import io.github.spark_redshift_community.spark.redshift.{RedshiftFailMessage, RedshiftPushdownUnsupportedException}
import io.github.spark_redshift_community.spark.redshift.pushdown.RedshiftSQLStatement
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, PythonUDF, ScalaUDF}
import org.apache.spark.sql.execution.aggregate.ScalaUDAF

/**
  * This class is used to catch unsupported statement and raise an exception
  * to stop the push-down to Redshift.
  */
private[querygeneration] object UnsupportedStatement {
  /** Used mainly by QueryGeneration.convertStatement. This matches
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

    // This exception is not a real issue. It will be caught in
    // QueryBuilder.treeRoot.
    throw new RedshiftPushdownUnsupportedException(
      RedshiftFailMessage.FAIL_PUSHDOWN_STATEMENT,
      expr.prettyName,
      expr.sql,
      isKnownUnsupportedOperation(expr))
  }

  // Determine whether the unsupported operation is known or not.
  private def isKnownUnsupportedOperation(expr: Expression): Boolean = {
    // The pushdown for UDF is known unsupported
    (expr.isInstanceOf[PythonUDF]
      || expr.isInstanceOf[ScalaUDF]
      || expr.isInstanceOf[ScalaUDAF])
  }
}
