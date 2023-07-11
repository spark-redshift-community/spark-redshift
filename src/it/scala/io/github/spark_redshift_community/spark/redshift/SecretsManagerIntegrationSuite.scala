package io.github.spark_redshift_community.spark.redshift

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, MetadataBuilder}
import io.github.spark_redshift_community.spark.redshift.Parameters.{PARAM_SECRET_ID, PARAM_SECRET_REGION}

  /** Secrets manager integration suite performs basic integration test where authentication with
   * Redshift is done via passing a secret (containing Redshift credentials) instead of mentioning
   * them in JDBC URl or in User/Password option.
   */

class SecretsManagerIntegrationSuite extends IntegrationSuiteBase {

    test("roundtrip save and load") {
      withTempRedshiftTable("secretsmanager_roundtrip_save_and_load") { tableName =>
        val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1)), 1),
          StructType(StructField("foo", IntegerType, true,
        new MetadataBuilder().putString("redshift_type", "int4").build()) :: Nil))

        write(df)
          .option("url", jdbcUrlNoUserPassword)
          .option(PARAM_SECRET_ID, AWS_SECRET_ID)
          .option(PARAM_SECRET_REGION, AWS_SECRET_REGION)
          .option("dbtable", tableName)
          .mode(SaveMode.ErrorIfExists)
          .save()
        assert(DefaultJDBCWrapper.tableExists(conn, tableName))
        val loadedDf = read
          .option("url", jdbcUrlNoUserPassword)
          .option(PARAM_SECRET_ID, AWS_SECRET_ID)
          .option(PARAM_SECRET_REGION, AWS_SECRET_REGION)
          .option("dbtable", tableName)
          .load()
        assert(loadedDf.schema === df.schema)
        checkAnswer(loadedDf, df.collect())
      }
    }
  }

