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

import io.github.spark_redshift_community.spark.redshift.RedshiftRelation
import io.github.spark_redshift_community.spark.redshift.pushdown.querygeneration.QueryBuilder
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{InsertIntoDataSourceCommand, LogicalRelation}

import scala.collection.mutable.ArrayBuffer

/**
 * Clean up the plan, then try to generate a query from it for Redshift.
 */
case class RedshiftStrategy(session: SparkSession) extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    try {
      buildQueryRDD(plan.transform({
        case Project(Nil, child) => child
        case SubqueryAlias(_, child) => child
      })).getOrElse(Nil)
    } catch {

      case t: UnsupportedOperationException =>
        log.warn("Unsupported Operation: {}", t.getMessage)
        Nil

      case e: Exception =>
        log.warn("Pushdown failed: {}", e.getMessage)
        Nil
    }
  }

  /** Enumerates over the full tree structure of a logical query plan while looking for
   * RedshiftRelations.
   *
   * @param node A node in the logical Spark plan
   * @param allRedshiftInstances A collection of the found RedshiftRelation objects.
   * @return Nothing
   */
  private def findRedshiftRelations(node: TreeNode[_],
                                    allRedshiftInstances: ArrayBuffer[String]): Unit = {
    // Check whether this node is a RedshiftRelation. We must special-case inserts because it
    // embeds the target RedshiftRelation as a constructor parameter rather than a child node.
    node match {
      case LogicalRelation(relation: RedshiftRelation, _, _, _) =>
        allRedshiftInstances += relation.params.uniqueClusterName
      case InsertIntoDataSourceCommand(
        LogicalRelation(relation: RedshiftRelation, _, _, _), _, _) =>
          allRedshiftInstances += relation.params.uniqueClusterName
      case _ =>
    }

    // Enumerate over both the inner and outer children.
    node.innerChildren.foreach(findRedshiftRelations(_, allRedshiftInstances))
    node.children.map(_.asInstanceOf[TreeNode[_]])
      .foreach(findRedshiftRelations(_, allRedshiftInstances))
  }

  /** Attempts to get a SparkPlan from the provided LogicalPlan.
   *
   * @param plan The LogicalPlan provided by Spark.
   * @return An Option of Seq[RedshiftPlan] that contains the PhysicalPlan if
   *         query generation was successful, None if not.
   */
  private def buildQueryRDD(plan: LogicalPlan): Option[Seq[SparkPlan]] = {
    val useLazyMode = session.conf.get(RedshiftStrategy.LAZY_CONF_KEY, "true")
      .toBoolean

    // Gather the list of Redshift clusters for all referenced tables in the query plan.
    // We must special-case inserts because it embeds the RedshiftRelation as a constructor
    // parameter rather than as a child node.
    val allRedshiftInstances = new ArrayBuffer[String]
    findRedshiftRelations(plan, allRedshiftInstances)

    // Make sure the query plan spans only a single cluster since cross-cluster queries
    // may fail or produce incorrect results when run from a single cluster.
    if (allRedshiftInstances.isEmpty) {
      None
    } else if (!allRedshiftInstances.forall(_ == allRedshiftInstances.head)) {
      logWarning("Unable to pushdown query across multiple clusters")
      None
    } else {
      QueryBuilder.getSparkPlanFromLogicalPlan(plan, useLazyMode)
    }
  }
}

object RedshiftStrategy {
  val LAZY_CONF_KEY = "spark.datasource.redshift.community.autopushdown.lazyMode"
}