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
package io.github.spark_redshift_community.spark.redshift

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, MetadataBuilder}
import io.github.spark_redshift_community.spark.redshift.Parameters.{PARAM_SECRET_ID, PARAM_SECRET_REGION}

  /** Secrets manager integration suite performs basic integration test where authentication with
   * Redshift is done via passing a secret (containing Redshift credentials) instead of mentioning
   * them in JDBC URl or in User/Password option.
   */

class SecretsManagerIntegrationSuite extends IntegrationSuiteBase {

    ignore("roundtrip save and load") {
      withTempRedshiftTable("secretsmanager_roundtrip_save_and_load") { tableName =>
        val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1)), 1),
          StructType(StructField("foo", IntegerType, true,
        new MetadataBuilder().putString("redshift_type", "int4").build()) :: Nil))

        write(df)
          .option("url", jdbcUrlNoUserPassword)
//          .option(PARAM_SECRET_ID, AWS_SECRET_ID)
//          .option(PARAM_SECRET_REGION, AWS_SECRET_REGION)
          .option("dbtable", tableName)
          .mode(SaveMode.ErrorIfExists)
          .save()
        assert(DefaultJDBCWrapper.tableExists(conn, tableName))
        val loadedDf = read
          .option("url", jdbcUrlNoUserPassword)
//          .option(PARAM_SECRET_ID, AWS_SECRET_ID)
//          .option(PARAM_SECRET_REGION, AWS_SECRET_REGION)
          .option("dbtable", tableName)
          .load()
        assert(loadedDf.schema === df.schema)
        checkAnswer(loadedDf, df.collect())
      }
    }
  }

