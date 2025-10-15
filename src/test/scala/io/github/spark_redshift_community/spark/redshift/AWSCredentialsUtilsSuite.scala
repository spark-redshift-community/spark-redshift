/*
 * Copyright 2015 Databricks
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

import io.github.spark_redshift_community.spark.redshift.Parameters
import io.github.spark_redshift_community.spark.redshift.Parameters.{MergedParameters, PARAM_TEMPORARY_AWS_ACCESS_KEY_ID, PARAM_TEMPORARY_AWS_SECRET_ACCESS_KEY, PARAM_TEMPORARY_AWS_SESSION_TOKEN}
import io.github.spark_redshift_community.spark.redshift.AWSCredentialsUtils
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, AwsSessionCredentials, DefaultCredentialsProvider}

import scala.language.implicitConversions

class AWSCredentialsUtilsSuite extends AnyFunSuite {

  val baseParams = Map(
    "tempdir" -> "s3://foo/bar",
    "dbtable" -> "test_schema.test_table",
    "url" -> "jdbc:redshift://foo/bar?user=user&password=password")

  private implicit def string2Params(tempdir: String): MergedParameters = {
    Parameters.mergeParameters(baseParams ++ Map(
      "tempdir" -> tempdir,
      "forward_spark_s3_credentials" -> "true"))
  }

  test("credentialsString with regular keys") {
    val creds = AwsBasicCredentials.create("ACCESSKEYID", "SECRET/KEY/WITH/SLASHES")
    val params =
      Parameters.mergeParameters(baseParams ++ Map("forward_spark_s3_credentials" -> "true"))
    assert(AWSCredentialsUtils.getRedshiftCredentialsString(params, creds) ===
      "aws_access_key_id=ACCESSKEYID;aws_secret_access_key=SECRET/KEY/WITH/SLASHES")
  }

  test("credentialsString with STS temporary keys") {
    val params = Parameters.mergeParameters(baseParams ++ Map(
      PARAM_TEMPORARY_AWS_ACCESS_KEY_ID -> "ACCESSKEYID",
      PARAM_TEMPORARY_AWS_SECRET_ACCESS_KEY -> "SECRET/KEY",
      PARAM_TEMPORARY_AWS_SESSION_TOKEN -> "SESSION/Token"))
    assert(AWSCredentialsUtils.getRedshiftCredentialsString(params, null) ===
      "aws_access_key_id=ACCESSKEYID;aws_secret_access_key=SECRET/KEY;token=SESSION/Token")
  }

  test("Configured IAM roles should take precedence") {
    val creds = AwsSessionCredentials.create("ACCESSKEYID", "SECRET/KEY", "SESSION/Token")
    val iamRole = "arn:aws:iam::123456789000:role/redshift_iam_role"
    val params = Parameters.mergeParameters(baseParams ++ Map("aws_iam_role" -> iamRole))
    assert(AWSCredentialsUtils.getRedshiftCredentialsString(params, null) ===
      s"aws_iam_role=$iamRole")
  }

  test("AWSCredentials.load() STS temporary keys should take precedence") {
    val conf = new Configuration(false)
    conf.set("fs.s3.awsAccessKeyId", "CONFID")
    conf.set("fs.s3.awsSecretAccessKey", "CONFKEY")

    val params = Parameters.mergeParameters(baseParams ++ Map(
      "tempdir" -> "s3://URIID:URIKEY@bucket/path",
      PARAM_TEMPORARY_AWS_ACCESS_KEY_ID -> "key_id",
      PARAM_TEMPORARY_AWS_SECRET_ACCESS_KEY -> "secret",
      PARAM_TEMPORARY_AWS_SESSION_TOKEN -> "token"
    ))

    val creds = AWSCredentialsUtils.load(params, conf).resolveCredentials()
    assert(creds.isInstanceOf[AwsSessionCredentials])
    assert(creds.accessKeyId() === "key_id")
    assert(creds.secretAccessKey() === "secret")
    assert(creds.asInstanceOf[AwsSessionCredentials].sessionToken() === "token")
  }

  test("AWSCredentials.load() credentials precedence for s3:// URIs") {
    {
      val creds = AWSCredentialsUtils.load("s3://URIID:URIKEY@bucket/path", null)
      assert(creds.isInstanceOf[DefaultCredentialsProvider])
    }

    {
      val creds = AWSCredentialsUtils.load("s3://bucket/path", null)
      assert(creds.isInstanceOf[DefaultCredentialsProvider])
    }

  }

  test("AWSCredentials.load() credentials precedence for s3n:// URIs") {
    {
      val creds = AWSCredentialsUtils.load("s3n://URIID:URIKEY@bucket/path", null)
      assert(creds.isInstanceOf[DefaultCredentialsProvider])
    }

    {
      val creds = AWSCredentialsUtils.load("s3n://bucket/path", null)
      assert(creds.isInstanceOf[DefaultCredentialsProvider])
    }

  }

  test("AWSCredentials.load() credentials precedence for s3a:// URIs") {
    {
      val creds = AWSCredentialsUtils.load("s3a://URIID:URIKEY@bucket/path", null)
      assert(creds.isInstanceOf[DefaultCredentialsProvider])
    }

    {
      val creds = AWSCredentialsUtils.load("s3a://bucket/path", null)
      assert(creds.isInstanceOf[DefaultCredentialsProvider])
    }

  }
}
