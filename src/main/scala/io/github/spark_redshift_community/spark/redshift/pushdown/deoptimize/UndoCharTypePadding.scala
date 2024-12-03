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
package io.github.spark_redshift_community.spark.redshift.pushdown.deoptimize

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, IsNotNull, Literal}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{CharVarcharCodegenUtils, CharVarcharUtils}
import org.apache.spark.sql.types.{CharType, IntegerType, StringType}

object UndoCharTypePadding extends Rule[LogicalPlan] {

  // Remove padding as the SQL that will be generated shouldn't include padding which is an internal
  // detail of the engine. Redshift should handle this internally.
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithSubqueries {
    // Scope down to project alias cases as observed in TPC-DS queries
    case project @ Project(projectList, child) =>
      var modified = false
      val newProjectList = projectList.map {
        case alias @ Alias(ReadSidePadding(ref), _) =>
          modified = true
          alias.withNewChildren(ref :: Nil).asInstanceOf[Alias]
        case other =>
          other
      }
      if (modified) {
        Project(newProjectList, child)
      } else {
        project
      }

    // Scope down to IsNotNull Filter cases as observed in TPC-DS queries
    case filter @ Filter(condition, child) =>
      val newCondition = condition.transform {
        case IsNotNull(ReadSidePadding(ref)) =>
          IsNotNull(ref)
      }
      if (condition eq newCondition) {
        filter
      } else {
        filter.copy(newCondition, child)
      }
  }
}

object ReadSidePadding {
  def unapply(s: StaticInvoke): Option[Expression] = s match {
    case StaticInvoke(clazz, StringType, "readSidePadding", ref  +: Literal(length, IntegerType)
      +: Nil, _, _, _, _)
      if ref.isInstanceOf[AttributeReference] &&
        clazz == classOf[CharVarcharCodegenUtils] &&
        length.isInstanceOf[Int] =>
      val metadata = ref.asInstanceOf[AttributeReference].metadata
      val optionalDataType = CharVarcharUtils.getRawType(metadata)
      optionalDataType.filter { dataType =>
        dataType == CharType(length.asInstanceOf[Int])
      }.map(_ => ref)
    case _ => None
  }
}
