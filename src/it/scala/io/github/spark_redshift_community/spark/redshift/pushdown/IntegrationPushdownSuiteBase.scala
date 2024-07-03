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
import io.github.spark_redshift_community.spark.redshift.Parameters.{PARAM_AUTO_PUSHDOWN, PARAM_PUSHDOWN_S3_RESULT_CACHE, PARAM_TEMPDIR_REGION, PARAM_UNLOAD_S3_FORMAT}
import org.apache.spark.sql.{DataFrameReader, SQLContext}

import java.time.format.DateTimeFormatter
import org.scalatest.Tag

// Test tag to specify the functional tests for Redshift timestamptz type.
object TimestamptzTest extends Tag("TimestamptzTest")
// Test tag to specify the functional tests that needs preloaded Redshift correctness dataset
// on cluster.
object PreloadTest extends Tag("PreloadTest")
// P0 functional test set to run within half an hour, which can be used in every development build.
object P0Test extends Tag("P0Test")
// P1 functional test set to run in about one hour and a half, which can be used in nightly build.
object P1Test extends Tag("P1Test")

class IntegrationPushdownSuiteBase extends IntegrationSuiteBase {
  protected var test_table: String = setTestTableName()
  protected val test_table_safe_null = s""""PUBLIC"."pushdown_suite_test_safe_null_$randomSuffix""""
  // This flag controls whether auto pushdown operations into Redshift is enabled.
  override protected val auto_pushdown: String = "true"
  // This flag controls whether to unload data as parquet or pipe-delimited text.
  protected val s3format: String = "TEXT"
  // This flag controls whether to cache previously unloaded data in S3 with auto_pushdown
  protected val s3_result_cache = "true"
  // This flag controls whether to use preloaded Redshift correctness test dataset on cluster.
  protected val preloaded_data: String = "false"
  // This is used for timestamptz related tests
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z")

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
      .option(PARAM_UNLOAD_S3_FORMAT, s3format)
      .option(PARAM_TEMPDIR_REGION, AWS_S3_SCRATCH_SPACE_REGION)
      .option(PARAM_PUSHDOWN_S3_RESULT_CACHE, s3_result_cache)
  }

  def checkSqlStatement(expectedAnswers: String *): Unit = {
    // If there is no operation pushed down into Redshift, there is no need to
    // validate executed statement in Redshift as it will be a simple select * statement.
    if (auto_pushdown.toBoolean) {
      // Make sure there is at least one match.
      val threadName = Thread.currentThread.getName
      val lastBuildStmt = Utils.lastBuildStmt(threadName).replaceAll("\\s", "")
      assert(expectedAnswers.exists(_.replaceAll("\\s", "") == lastBuildStmt),
        s"Actual sql: ${Utils.lastBuildStmt}")
    }
  }

  def checkResult(expectedResult: String, actualResult: String): Unit = {
    if (!expectedResult.trim().equals(actualResult.trim())) {
      val errorMessage =
        s"""
           |Results do not match:
           |== Found ==
           |${actualResult}
           |== Expected ==
           |${expectedResult}
            """.stripMargin
      fail(errorMessage)
    }
  }

  // Method that performs validation of statement result and statement executed
  // on Redshift.
  def doTest(sqlContext: SQLContext, tc: TestCase): Unit = {
    checkAnswer( // ensure statement result is as expected
      sqlContext.sql(tc.sparkStatement),
      tc.expectedResult
    )
    checkSqlStatement(tc.expectedAnswers: _*)
  }
}
