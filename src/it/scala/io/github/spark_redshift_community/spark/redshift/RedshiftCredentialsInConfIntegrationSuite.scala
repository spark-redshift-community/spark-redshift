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

import io.github.spark_redshift_community.spark.redshift.data.JDBCWrapper
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StructField, StructType}

/**
 * This suite performs basic integration tests where the Redshift credentials have been
 * specified via `spark-redshift`'s configuration rather than as part of the JDBC URL.
 */
class RedshiftCredentialsInConfIntegrationSuite extends IntegrationSuiteBase {

  test("roundtrip save and load") {
    // This test is only valid for JDBC-based connections
    if (redshiftWrapper.isInstanceOf[JDBCWrapper]) {
      val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1)), 1),
        StructType(StructField("foo", IntegerType, true,
          new MetadataBuilder().putString("redshift_type", "int4").build()) :: Nil))
      val tableName = s"roundtrip_save_and_load_$randomSuffix"
      try {
        write(df)
          .option("url", jdbcUrlNoUserPassword)
          .option("user", AWS_REDSHIFT_USER)
          .option("password", AWS_REDSHIFT_PASSWORD)
          .option("dbtable", tableName)
          .save()
        assert(redshiftWrapper.tableExists(conn, tableName))
        val loadedDf = read
          .option("url", jdbcUrlNoUserPassword)
          .option("user", AWS_REDSHIFT_USER)
          .option("password", AWS_REDSHIFT_PASSWORD)
          .option("dbtable", tableName)
          .load()
        assert(loadedDf.schema === df.schema)
        checkAnswer(loadedDf, df.collect())
      } finally {
        redshiftWrapper.executeUpdate(conn, s"drop table if exists $tableName")
      }
    }
  }
}
