/*
 * Copyright 2015 TouchType Ltd
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

package io.github.spark_redshift_community.spark.redshift

import org.scalatest.matchers.should._
import org.scalatest.funsuite.AnyFunSuite

/**
  * Check validation of parameter config
  */
class ParametersSuite extends AnyFunSuite with Matchers {

  test("Minimal valid parameter map is accepted") {
    val params = Map(
      "tempdir" -> "s3://foo/bar",
      "dbtable" -> "test_schema.test_table",
      "url" -> "jdbc:redshift://foo/bar?user=user&password=password",
      "forward_spark_s3_credentials" -> "true",
      "include_column_list" -> "true")

    val mergedParams = Parameters.mergeParameters(params)

    mergedParams.rootTempDir should startWith(params("tempdir"))
    mergedParams.createPerQueryTempDir() should startWith(params("tempdir"))
    mergedParams.jdbcUrl shouldBe params("url")
    mergedParams.table shouldBe Some(TableName("", "test_schema", "test_table"))
    assert(mergedParams.forwardSparkS3Credentials)
    assert(mergedParams.includeColumnList)
    assert(!mergedParams.legacyJdbcRealTypeMapping)

    // Check that the defaults have been added
    (
      Parameters.DEFAULT_PARAMETERS
        - "forward_spark_s3_credentials"
        - "include_column_list"
    ).foreach {
      case (key, value) => mergedParams.parameters(key) shouldBe value
    }
  }

  test("createPerQueryTempDir() returns distinct temp paths") {
    val params = Map(
      "forward_spark_s3_credentials" -> "true",
      "tempdir" -> "s3://foo/bar",
      "dbtable" -> "test_table",
      "url" -> "jdbc:redshift://foo/bar?user=user&password=password")

    val mergedParams = Parameters.mergeParameters(params)

    mergedParams.createPerQueryTempDir() should not equal mergedParams.createPerQueryTempDir()
  }

  test("Errors are thrown when mandatory parameters are not provided") {
    def checkMerge(params: Map[String, String], err: String): Unit = {
      val e = intercept[IllegalArgumentException] {
        Parameters.mergeParameters(params)
      }
      assert(e.getMessage.contains(err))
    }

    val testURL = "jdbc:redshift://foo/bar?user=user&password=password"
    checkMerge(Map("dbtable" -> "test_table", "url" -> testURL), "tempdir")
    checkMerge(Map("tempdir" -> "s3://foo/bar", "url" -> testURL), "Redshift table name")
    checkMerge(Map("dbtable" -> "test_table", "tempdir" -> "s3://foo/bar"), "JDBC URL")
    checkMerge(Map("dbtable" -> "test_table", "tempdir" -> "s3://foo/bar", "url" -> testURL),
      "method for authenticating")
  }

  test("Must specify either 'dbtable' or 'query' parameter, but not both") {
    intercept[IllegalArgumentException] {
      Parameters.mergeParameters(Map(
        "forward_spark_s3_credentials" -> "true",
        "tempdir" -> "s3://foo/bar",
        "url" -> "jdbc:redshift://foo/bar?user=user&password=password"))
    }.getMessage should (include("dbtable") and include("query"))

    intercept[IllegalArgumentException] {
      Parameters.mergeParameters(Map(
        "forward_spark_s3_credentials" -> "true",
        "tempdir" -> "s3://foo/bar",
        "dbtable" -> "test_table",
        "query" -> "select * from test_table",
        "url" -> "jdbc:redshift://foo/bar?user=user&password=password"))
    }.getMessage should (include("dbtable") and include("query") and include("both"))

    Parameters.mergeParameters(Map(
      "forward_spark_s3_credentials" -> "true",
      "tempdir" -> "s3://foo/bar",
      "query" -> "select * from test_table",
      "url" -> "jdbc:redshift://foo/bar?user=user&password=password"))
  }

  test("Cannot specify credentials in both URL and ('user' or 'password') parameters") {
    intercept[IllegalArgumentException] {
      Parameters.mergeParameters(Map(
        "forward_spark_s3_credentials" -> "true",
        "tempdir" -> "s3://foo/bar",
        "query" -> "select * from test_table",
        "user" -> "user",
        "password" -> "password",
        "url" -> "jdbc:redshift://foo/bar?user=user&password=password"))
    }.getMessage should (include("credentials") and include("both"))

    Parameters.mergeParameters(Map(
      "forward_spark_s3_credentials" -> "true",
      "tempdir" -> "s3://foo/bar",
      "query" -> "select * from test_table",
      "url" -> "jdbc:redshift://foo/bar?user=user&password=password"))
  }

  test("Cannot specify a secret and credentials in URL") {
    val err = intercept[IllegalArgumentException] {
      Parameters.mergeParameters(Map(
        "forward_spark_s3_credentials" -> "true",
        "tempdir" -> "s3://foo/bar",
        "query" -> "select * from test_table",
        "secret.id" -> Parameters.PARAM_SECRET_ID,
        "secret.region" -> Parameters.PARAM_SECRET_REGION,
        "url" -> "jdbc:redshift://foo/bar?user=user&password=password"))
    }
    err.getMessage.contains("You cannot specify a secret and give credentials in URL")
  }

  test("Ensuring secret.id and secret.region values of MergedParameters are set") {
    val params = Parameters.mergeParameters(Map(
      "forward_spark_s3_credentials" -> "true",
      "tempdir" -> "s3://foo/bar",
      "query" -> "select * from test_table",
      "secret.id" -> Parameters.PARAM_SECRET_ID,
      "secret.region" -> Parameters.PARAM_SECRET_REGION,
      "url" -> "jdbc:redshift://foo/bar"))

    params.secretId.get shouldEqual Parameters.PARAM_SECRET_ID
    params.secretRegion.get shouldEqual Parameters.PARAM_SECRET_REGION
  }

  test("Cannot specify a secret and user/password parameters") {
    val err = intercept[IllegalArgumentException] {
      Parameters.mergeParameters(Map(
        "forward_spark_s3_credentials" -> "true",
        "tempdir" -> "s3://foo/bar",
        "query" -> "select * from test_table",
        "user" -> "user",
        "password" -> "password",
        "secret.id" -> Parameters.PARAM_SECRET_ID,
        "secret.region" -> Parameters.PARAM_SECRET_REGION,
        "url" -> "jdbc:redshift://foo/bar"))
    }
    err.getMessage.contains("You cannot give a secret and specify user/password options")
  }

  test("tempformat option is case-insensitive") {
    val params = Map(
      "forward_spark_s3_credentials" -> "true",
      "tempdir" -> "s3://foo/bar",
      "dbtable" -> "test_schema.test_table",
      "url" -> "jdbc:redshift://foo/bar?user=user&password=password")

    Parameters.mergeParameters(params + ("tempformat" -> "csv"))
    Parameters.mergeParameters(params + ("tempformat" -> "CSV"))

    intercept[IllegalArgumentException] {
      Parameters.mergeParameters(params + ("tempformat" -> "invalid-temp-format"))
    }
  }

  test("can only specify one Redshift to S3 authentication mechanism") {
    val e = intercept[IllegalArgumentException] {
      Parameters.mergeParameters(Map(
        "tempdir" -> "s3://foo/bar",
        "dbtable" -> "test_schema.test_table",
        "url" -> "jdbc:redshift://foo/bar?user=user&password=password",
        "forward_spark_s3_credentials" -> "true",
        "aws_iam_role" -> "role"))
    }
    assert(e.getMessage.contains("mutually-exclusive"))
  }

  test("preaction and postactions should be trimmed before splitting by semicolon") {
    val params = Parameters.mergeParameters(Map(
      "forward_spark_s3_credentials" -> "true",
      "tempdir" -> "s3://foo/bar",
      "dbtable" -> "test_schema.test_table",
      "url" -> "jdbc:redshift://foo/bar?user=user&password=password",
      "preactions" -> "update table1 set col1 = val1;update table1 set col2 = val2;  ",
      "postactions" -> "update table2 set col1 = val1;update table2 set col2 = val2;  "
    ))

    assert(params.preActions.length == 2)
    assert(params.preActions.head == "update table1 set col1 = val1")
    assert(params.preActions.last == "update table1 set col2 = val2")
    assert(params.postActions.length == 2)
    assert(params.postActions.head == "update table2 set col1 = val1")
    assert(params.postActions.last == "update table2 set col2 = val2")
  }

  test("Non identifier characters in user provided query group label are rejected") {
    val params = Map(
      "tempdir" -> "s3://foo/bar",
      "dbtable" -> "test_schema.test_table",
      "url" -> "jdbc:redshift://foo/bar?user=user&password=password",
      "forward_spark_s3_credentials" -> "true",
      "include_column_list" -> "true",
      Parameters.PARAM_USER_QUERY_GROUP_LABEL -> "hello!"
    )

    val exception = intercept[IllegalArgumentException] {
      Parameters.mergeParameters(params)
    }

    assert(exception.getMessage == "All characters in label option must " +
      "be valid unicode identifier parts (char.isUnicodeIdentifierPart == true), " +
      "'!' character not allowed")
  }

  test("tempdir_region allows pre-GA regions") {
    val params = Map(
      "tempdir" -> "s3://foo/bar",
      "dbtable" -> "test_schema.test_table",
      "url" -> "jdbc:redshift://foo/bar?user=user&password=password",
      "forward_spark_s3_credentials" -> "true",
      "tempdir_region" -> "pre-ga-region",
    )
    val mergedParams = Parameters.mergeParameters(params)

    assert(mergedParams.tempDirRegion.get == "pre-ga-region")
  }
}
