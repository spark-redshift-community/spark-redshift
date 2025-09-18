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
package io.github.spark_redshift_community.spark.redshift.test

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.secretsmanager._
import com.amazonaws.services.secretsmanager.model.{CreateSecretRequest, DeleteSecretRequest}
import io.github.spark_redshift_community.spark.redshift.Parameters.{PARAM_AUTO_PUSHDOWN, PARAM_DATA_API_USER, PARAM_SECRET_ID, PARAM_SECRET_REGION, PARAM_TEMPDIR_REGION}
import io.github.spark_redshift_community.spark.redshift.data.JDBCWrapper
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}

import scala.collection.mutable
import scala.util.Random

/**
 * Secrets manager integration suite performs basic integration test where authentication with
 * Redshift is done via passing a secret (containing Redshift credentials) instead of mentioning
 * them in JDBC URL or in User/Password option.
 */

class SecretsManagerIntegrationSuite extends IntegrationSuiteBase {

  val redshiftUsr = "test_usr"
  val redshiftPwd = Random.alphanumeric.take(6).mkString + "cT1@"
  val secretName = "test_secret" + Random.alphanumeric.take(6).mkString
  val secretValue = "{\"username\":\"" + s"$redshiftUsr" + "\",\"password\":\"" + s"$redshiftPwd" + "\"}"
  val secretRegion = AWS_S3_SCRATCH_SPACE_REGION
  val endpoint = s"secretsmanager.$secretRegion.amazonaws.com"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createNewSecret
    redshiftWrapper.executeUpdate(conn, s"CREATE USER $redshiftUsr PASSWORD '$redshiftPwd'")
  }

  override def afterAll(): Unit = {
    redshiftWrapper.executeUpdate(conn, s"DROP USER $redshiftUsr")
    deleteSecret()
    super.afterAll()
  }

  private def createSecretsManagerClient(): AWSSecretsManager = {
    val config = new AwsClientBuilder.EndpointConfiguration(endpoint, secretRegion)
    val clientBuilder = AWSSecretsManagerClientBuilder.standard()
      .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
      .withEndpointConfiguration(config)
    clientBuilder.build()
  }

  def createNewSecret: String = {
    val client = createSecretsManagerClient()
    val secretRequest = new CreateSecretRequest().withName(secretName)
      .withSecretString(secretValue)
    val secretResponse = client.createSecret(secretRequest)
    secretResponse.getARN
  }

  def deleteSecret(): String = {
    val client = createSecretsManagerClient()
    val secretRequest = new DeleteSecretRequest().withSecretId(secretName)
      .withForceDeleteWithoutRecovery(true)
    val secretResponse = client.deleteSecret(secretRequest)
    secretResponse.getARN
  }

  protected def getDefaultCredentials(): Map[String, String] = {
    val options: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
    options ++= super.defaultOptions()
    options.remove(PARAM_DATA_API_USER)
    options.toMap
  }
  private def getOptions(): Map[String, String] = {
    val options: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
    options += (PARAM_SECRET_ID -> secretName)
    if (redshiftWrapper.isInstanceOf[JDBCWrapper]) {
      options += ("url" -> jdbcUrlNoUserPassword)
      options += (PARAM_SECRET_REGION -> secretRegion)
    }
    options.toMap
  }

  test("Secret manager roundtrip save and load") {
    withTempRedshiftTable("secretsmanager_roundtrip_save_and_load") { tableName =>
      val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1)), 1),
        StructType(StructField("foo", IntegerType, true,
          new MetadataBuilder().putString("redshift_type", "int4").build()) :: Nil))

      df.write
        .format("io.github.spark_redshift_community.spark.redshift")
        .options(getDefaultCredentials())
        .options(getOptions())
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()
      assert(redshiftWrapper.tableExists(conn, tableName))
      val loadedDf = sqlContext.read
        .format("io.github.spark_redshift_community.spark.redshift")
        .options(getDefaultCredentials())
        .options(getOptions())
        .option("dbtable", tableName)
        .load()
      assert(loadedDf.schema === df.schema)
      checkAnswer(loadedDf, df.collect())
    }
  }
}
