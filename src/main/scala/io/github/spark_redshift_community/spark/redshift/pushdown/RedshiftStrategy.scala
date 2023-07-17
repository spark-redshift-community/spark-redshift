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

import io.github.spark_redshift_community.spark.redshift.pushdown.querygeneration.QueryBuilder
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.execution.SparkPlan

/**
 * Clean up the plan, then try to generate a query from it for Redshift.
 */
class RedshiftStrategy(session: SparkSession) extends Strategy {
  private val LAZY_CONF_KEY = "spark.datasource.redshift.community.autopushdown.lazyMode"

  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    try {
      buildQueryRDD(plan.transform({
        case Project(Nil, child) => child
        case SubqueryAlias(_, child) => child
      })).getOrElse(Nil)
    } catch {

      case t: UnsupportedOperationException =>
        log.warn(s"Unsupported Operation:${t.getMessage}")
        Nil

      case e: Exception =>
        log.warn(s"Pushdown failed:${e.getMessage}", e)
        Nil
    }
  }

  /** Attempts to get a SparkPlan from the provided LogicalPlan.
   *
   * @param plan The LogicalPlan provided by Spark.
   * @return An Option of Seq[RedshiftPlan] that contains the PhysicalPlan if
   *         query generation was successful, None if not.
   */
  private def buildQueryRDD(plan: LogicalPlan): Option[Seq[SparkPlan]] = {
    val useLazyMode = session.conf.get(LAZY_CONF_KEY, "true")
      .toBoolean

    if (useLazyMode) {
      logInfo("Using lazy mode for redshift query push down")
      QueryBuilder.getQueryFromPlan(plan).map {
        case (output, query, relation) =>
          Seq(RedshiftScanExec(output, query, relation))
      }
    } else {
      logWarning("Using eager mode for redshift query push down. " +
        "To improve performance please run " +
        s"`SET $LAZY_CONF_KEY=true`")
      QueryBuilder.getRDDFromPlan(plan).map {
        case (output, rdd) =>
          Seq(RedshiftPlan(output, rdd))
      }
    }
  }
}