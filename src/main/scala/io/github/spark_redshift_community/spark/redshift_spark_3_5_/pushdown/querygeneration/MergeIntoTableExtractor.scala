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

import io.github.spark_redshift_community.spark.redshift.RedshiftRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, MergeAction, MergeIntoTable}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.LogicalRelation
object MergeIntoTableExtractor {
  def unapply(plan: LogicalPlan): Option[(LogicalPlan,
                                          RedshiftRelation,
                                          LogicalPlan,
                                          Expression,
                                          Seq[MergeAction],
                                          Seq[MergeAction],
                                          Seq[MergeAction])] =
    plan match {
      case MergeIntoTable(targetTable@LogicalRelation(targetRelation: RedshiftRelation, _, _, _),
                          sourcePlan,
                          mergeCondition,
                          matchedActions,
                          notMatchedActions,
                          notMatchedBySourceActions) =>
        Some(targetTable,
             targetRelation,
             sourcePlan,
             mergeCondition,
             matchedActions,
             notMatchedActions,
             notMatchedBySourceActions)
      case _ => None
  }
}
