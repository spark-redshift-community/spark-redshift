package io.github.spark_redshift_community.spark.redshift.pushdown

import io.github.spark_redshift_community.spark.redshift.{IntegrationSuiteBase, Utils}
import io.github.spark_redshift_community.spark.redshift.Parameters.{PARAM_AUTO_PUSHDOWN, PARAM_PUSHDOWN_UNLOAD_S3_FORMAT}
import org.apache.spark.sql.DataFrameReader

class IntegrationPushdownSuiteBase extends IntegrationSuiteBase {
  protected val test_table: String = s""""PUBLIC"."pushdown_suite_test_table_$randomSuffix""""
  protected val s3format: String = "DEFAULT"
  override def beforeAll(): Unit = {
    super.beforeAll()
    conn.prepareStatement(s"drop table if exists $test_table").executeUpdate()
    createTestDataInRedshift(test_table)
  }

  override def afterAll(): Unit = {
    try {
      conn.prepareStatement(s"drop table if exists $test_table").executeUpdate()
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
      .option(PARAM_AUTO_PUSHDOWN, "true")
      .option(PARAM_PUSHDOWN_UNLOAD_S3_FORMAT, s3format)
  }

  def checkSqlStatement(expectedAnswer: String): Unit = {
    assert(
      Utils.lastBuildStmt.replaceAll("\\s", "")
      ==
      expectedAnswer.replaceAll("\\s", "")
    )
  }
}
