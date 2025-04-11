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

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import java.util.Properties

class SecureJDBCValidationSuite extends AnyFunSuite with BeforeAndAfterEach {
  private var sc: SparkContext = _
  private var sqlContext: SQLContext = _

  override def afterEach(): Unit = {
    sc.stop()
    sc = null
    sqlContext = null
  }

  test("test redshift usage constraint enablement") {
    var sparkConf = new SparkConf()
    sc = new SparkContext("local", "UtilsSuite", sparkConf)
    sqlContext = new SQLContext(sc)
    assert(!Utils.isUnsecureJDBCConnectionRejected())
    assert(!Utils.isRedshiftS3ConnectionViaIAMRoleOnly())
    sc.stop()

    sparkConf = new SparkConf().set(
      "spark.datasource.redshift.community.redshift_s3_connection_iam_role_only", "true")
    sc = new SparkContext("local", "UtilsSuite", sparkConf)
    sqlContext = new SQLContext(sc)
    assert(!Utils.isUnsecureJDBCConnectionRejected())
    assert(Utils.isRedshiftS3ConnectionViaIAMRoleOnly())
    sc.stop()

    sparkConf = new SparkConf().set(
      "spark.datasource.redshift.community.reject_unsecure_jdbc_connection", "true")
    sc = new SparkContext("local", "UtilsSuite", sparkConf)
    sqlContext = new SQLContext(sc)
    assert(Utils.isUnsecureJDBCConnectionRejected())
    assert(!Utils.isRedshiftS3ConnectionViaIAMRoleOnly())
  }

  test("Test fail unsecure jdbc connection") {
    val sparkConf = new SparkConf().set(
      "spark.datasource.redshift.community.reject_unsecure_jdbc_connection", "true")
    sc = new SparkContext("local", "UtilsSuite", sparkConf)
    sqlContext = new SQLContext(sc)

    val properties = new Properties()

    // happy case
    Utils.checkJDBCSecurity("com.amazon.redshift.jdbc42.Driver",
      "jdbc:redshift://redshifthost:5439/database?user=username&password=pass",
      properties)

    // failure case
    intercept[RedshiftConstraintViolationException] {
      Utils.checkJDBCSecurity("org.postgresql.Driver",
        "jdbc:redshift://redshifthost:5439/database?user=username&password=pass",
        properties)
    }

    intercept[RedshiftConstraintViolationException] {
      Utils.checkJDBCSecurity("com.amazon.redshift.jdbc42.Driver",
        "jdbc:redshift://redshifthost:5439/database?user=username&password=pass&ssl=false",
        properties)
    }

    intercept[RedshiftConstraintViolationException] {
      Utils.checkJDBCSecurity("com.amazon.redshift.jdbc42.Driver",
        "jdbc:redshift://redshifthost:5439/database?ssl=false",
        properties)
    }

    intercept[RedshiftConstraintViolationException] {
      Utils.checkJDBCSecurity("com.amazon.redshift.jdbc42.Driver",
        "jdbc:redshift://redshifthost:5439/database?user=username;password=pass;ssl=false",
        properties)
    }

    intercept[RedshiftConstraintViolationException] {
      Utils.checkJDBCSecurity("com.amazon.redshift.jdbc42.Driver",
        "jdbc:redshift://redshifthost:5439/database?user=username&password=pass&SSL=false",
        properties)
    }

    properties.setProperty("ssl", "false")
    intercept[RedshiftConstraintViolationException] {
      Utils.checkJDBCSecurity("com.amazon.redshift.jdbc42.Driver",
        "jdbc:redshift://redshifthost:5439/database?user=username&password=pass&SSL=true",
        properties)
    }

    // do not fail if there's no constraint enabled
    sc.stop()
    sc = new SparkContext("local", "UtilsSuite", new SparkConf())
    sqlContext = new SQLContext(sc)
    assert(!Utils.isUnsecureJDBCConnectionRejected())

    Utils.checkJDBCSecurity("com.amazon.redshift.jdbc42.Driver",
      "jdbc:redshift://redshifthost:5439/database?user=username&password=pass&ssl=false",
      properties)
  }
}
