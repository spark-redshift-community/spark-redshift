/*
 * Copyright 2016 Databricks
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

package io.github.spark_redshift_community.spark.redshift.test

import io.github.spark_redshift_community.spark.redshift.Parameters.PARAM_TEMPDIR_REGION
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * Integration tests where the Redshift cluster and the S3 bucket are in different AWS regions.
 */
class CrossRegionIntegrationSuite extends IntegrationSuiteBase {

  protected val AWS_S3_CROSS_REGION_SCRATCH_SPACE: String =
    loadConfigFromEnv("AWS_S3_CROSS_REGION_SCRATCH_SPACE")
  protected val AWS_S3_CROSS_REGION_SCRATCH_SPACE_REGION: String =
    loadConfigFromEnv("AWS_S3_CROSS_REGION_SCRATCH_SPACE_REGION")
  require(AWS_S3_CROSS_REGION_SCRATCH_SPACE.contains("s3a"), "must use s3a:// URL")

  override protected val tempDir: String = AWS_S3_CROSS_REGION_SCRATCH_SPACE + randomSuffix + "/"

  test("write") {
    val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1)), 1),
      StructType(StructField("foo", IntegerType) :: Nil))
    val tableName = s"roundtrip_save_and_load_$randomSuffix"
    try {
      write(df)
        .option("dbtable", tableName)
        .option(PARAM_TEMPDIR_REGION, AWS_S3_CROSS_REGION_SCRATCH_SPACE_REGION)
        .save()
      // Check that the table exists. It appears that creating a table in one connection then
      // immediately querying for existence from another connection may result in spurious "table
      // doesn't exist" errors; this caused the "save with all empty partitions" test to become
      // flaky (see #146). To work around this, add a small sleep and check again:
      if (!redshiftWrapper.tableExists(conn, tableName)) {
        Thread.sleep(1000)
        assert(redshiftWrapper.tableExists(conn, tableName))
      }
    } finally {
      redshiftWrapper.executeUpdate(conn, s"drop table if exists $tableName")
    }
  }
}
