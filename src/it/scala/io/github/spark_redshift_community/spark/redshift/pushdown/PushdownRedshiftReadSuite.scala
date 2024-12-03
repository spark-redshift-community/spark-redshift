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
package io.github.spark_redshift_community.spark.redshift.pushdown

import io.github.spark_redshift_community.spark.redshift.data.JDBCWrapper
import io.github.spark_redshift_community.spark.redshift.{IntegrationSuiteBase, OverrideNullableSuite}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

class PushdownRedshiftReadSuite extends IntegrationSuiteBase with OverrideNullableSuite {
  override val auto_pushdown: String = "true"

  test("pushdowns across multiple clusters are executed separately") {
    // This method only works for JDBC.
    if (redshiftWrapper.isInstanceOf[JDBCWrapper]) {
      // A single pushdown operation cannot query data from multiple clusters
      // this verifies that separate scans are generated for each cluster
      val expectedUrl1 = jdbcUrl + "&ApplicationName=1"
      val expectedUrl2 = jdbcUrl + "&ApplicationName=2"

      withTempRedshiftTable("testTable") { name =>
        redshiftWrapper.executeUpdate(conn, s"create table $name (id integer)")
        read
          .option("url", expectedUrl1)
          .option("dbtable", name)
          .load().createOrReplaceTempView("view1")
        read
          .option("url", expectedUrl2)
          .option("dbtable", name)
          .load().createOrReplaceTempView("view2")

        val plan = sqlContext.sql("select count(*) from view1 union select count(*) from view2").
          queryExecution.executedPlan

        val traversablePlan = plan match {
          case p: AdaptiveSparkPlanExec => p.executedPlan
          case _ => plan
        }

        assert(traversablePlan.exists {
          case RedshiftScanExec(_, _, relation) => relation.params.jdbcUrl.get == expectedUrl1
          case _ => false
        })
        assert(traversablePlan.exists {
          case RedshiftScanExec(_, _, relation) => relation.params.jdbcUrl.get == expectedUrl2
          case _ => false
        })
      }
    }
  }
}
