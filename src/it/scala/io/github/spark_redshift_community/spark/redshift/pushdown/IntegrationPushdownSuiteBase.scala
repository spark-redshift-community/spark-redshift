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

import io.github.spark_redshift_community.spark.redshift.{IntegrationSuiteBase, Utils}
import io.github.spark_redshift_community.spark.redshift.Parameters.{PARAM_AUTO_PUSHDOWN, PARAM_PUSHDOWN_UNLOAD_S3_FORMAT}
import org.apache.spark.sql.{DataFrameReader, SQLContext}

class IntegrationPushdownSuiteBase extends IntegrationSuiteBase {
  protected var test_table: String = setTestTableName()
  protected val test_table_safe_null = s""""PUBLIC"."pushdown_suite_test_safe_null_$randomSuffix""""
  // This flag controls whether auto pushdown operations into Redshift is enabled.
  protected val auto_pushdown: String = "true"
  // This flag is useful only if auto pushdown is enabled.
  // Otherwise, the s3 format will always be default.
  protected val s3format: String = "DEFAULT"
  // This flag controls whether to use preloaded Redshift correctness test dataset on cluster.
  protected val preloaded_data: String = "false"

  def setTestTableName(): String = s""""PUBLIC"."pushdown_suite_test_table_$randomSuffix""""

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (!preloaded_data.toBoolean) {
      conn.prepareStatement(s"drop table if exists $test_table").executeUpdate()
      conn.prepareStatement(s"drop table if exists $test_table_safe_null").executeUpdate()
      createTestDataInRedshift(test_table)
    }
  }

  override def afterAll(): Unit = {
    try {
      if (!preloaded_data.toBoolean) {
        conn.prepareStatement(s"drop table if exists $test_table").executeUpdate()
        conn.prepareStatement(s"drop table if exists $test_table_safe_null").executeUpdate()
      }
    } finally {
      super.afterAll()
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    SqlToS3TempCache.clearCache()
    read
      .option("dbtable", test_table)
      .load()
      .createOrReplaceTempView("test_table")
  }

  override def read: DataFrameReader = {
    sqlContext.read
      .format("io.github.spark_redshift_community.spark.redshift")
      .option("url", jdbcUrl)
      .option("tempdir", tempDir)
      .option("forward_spark_s3_credentials", "true")
      .option(PARAM_AUTO_PUSHDOWN, auto_pushdown)
      .option(PARAM_PUSHDOWN_UNLOAD_S3_FORMAT, s3format)
  }

  def checkSqlStatement(expectedAnswer: String): Unit = {
    // If there is no operation pushed down into Redshift, there is no need to
    // validate executed statement in Redshift as it will be a simple select * statement.
    if (auto_pushdown.toBoolean) {
      assert(
        Utils.lastBuildStmt.replaceAll("\\s", "")
          ==
          expectedAnswer.replaceAll("\\s", "")
      )
    }
  }

  // Method that performs validation of statement result and statement executed
  // on Redshift.
  def doTest(sqlContext: SQLContext, tc: TestCase): Unit = {
    checkAnswer( // ensure statement result is as expected
      sqlContext.sql(tc.sparkStatement),
      tc.expectedResult
    )
    checkSqlStatement(tc.expectedPushdownStatement)
  }
}
