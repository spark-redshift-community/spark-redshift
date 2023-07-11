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

import java.net.URI
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule
import io.github.spark_redshift_community.spark.redshift.Parameters.{MergedParameters, PARAM_LEGACY_JDBC_REAL_TYPE_MAPPING, PARAM_OVERRIDE_NULLABLE}
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito
import org.mockito.Mockito.{never, verify, when}
import org.scalatestplus.mockito.MockitoSugar.mock
import org.scalatest.matchers.should._
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.Logger

import java.sql.Timestamp
import java.util.Properties

/**
 * Unit tests for helper functions
 */
class UtilsSuite extends AnyFunSuite with Matchers {

  test("joinUrls preserves protocol information") {
    Utils.joinUrls("s3n://foo/bar/", "/baz") shouldBe "s3n://foo/bar/baz/"
    Utils.joinUrls("s3n://foo/bar/", "/baz/") shouldBe "s3n://foo/bar/baz/"
    Utils.joinUrls("s3n://foo/bar/", "baz/") shouldBe "s3n://foo/bar/baz/"
    Utils.joinUrls("s3n://foo/bar/", "baz") shouldBe "s3n://foo/bar/baz/"
    Utils.joinUrls("s3n://foo/bar", "baz") shouldBe "s3n://foo/bar/baz/"
  }

  test("joinUrls preserves credentials") {
    assert(
      Utils.joinUrls("s3n://ACCESSKEY:SECRETKEY@bucket/tempdir", "subdir") ===
      "s3n://ACCESSKEY:SECRETKEY@bucket/tempdir/subdir/")
  }

  test("fixUrl produces Redshift-compatible equivalents") {
    Utils.fixS3Url("s3a://foo/bar/12345") shouldBe "s3://foo/bar/12345"
    Utils.fixS3Url("s3n://foo/bar/baz") shouldBe "s3://foo/bar/baz"
  }

  test("addEndpointToUrl produces urls with endpoints added to host") {
    Utils.addEndpointToUrl("s3a://foo/bar/12345") shouldBe "s3a://foo.s3.amazonaws.com/bar/12345"
    Utils.addEndpointToUrl("s3n://foo/bar/baz") shouldBe "s3n://foo.s3.amazonaws.com/bar/baz"
  }

  test("temp paths are random subdirectories of root") {
    val root = "s3n://temp/"
    val firstTempPath = Utils.makeTempPath(root)

    Utils.makeTempPath(root) should (startWith (root) and endWith ("/")
      and not equal root and not equal firstTempPath)
  }

  test("removeCredentialsFromURI removes AWS access keys") {
    def removeCreds(uri: String): String = {
      Utils.removeCredentialsFromURI(URI.create(uri)).toString
    }
    assert(removeCreds("s3n://bucket/path/to/temp/dir") === "s3n://bucket/path/to/temp/dir")
    assert(
      removeCreds("s3n://ACCESSKEY:SECRETKEY@bucket/path/to/temp/dir") ===
      "s3n://bucket/path/to/temp/dir")
  }

  test("getRegionForRedshiftCluster") {
    val redshiftUrl =
      "jdbc:redshift://example.secret.us-west-2.redshift.amazonaws.com:5439/database"
    assert(Utils.getRegionForRedshiftCluster("mycluster.example.com") === None)
    assert(Utils.getRegionForRedshiftCluster(redshiftUrl) === Some("us-west-2"))
  }

  test("checkThatBucketHasObjectLifecycleConfiguration when no rule") {
    // Configure a mock S3 client so that we don't hit errors when trying to access AWS in tests.
    val mockS3Client = mock[AmazonS3Client](Mockito.RETURNS_SMART_NULLS)

    when(mockS3Client.getBucketLifecycleConfiguration(anyString())).thenReturn(
      new BucketLifecycleConfiguration().withRules(
        new Rule().withStatus(BucketLifecycleConfiguration.DISABLED)
      ))
    assert(Utils.checkThatBucketHasObjectLifecycleConfiguration(
      "s3a://bucket/path/to/temp/dir", mockS3Client) === true)
  }

  test("checkThatBucketHasObjectLifecycleConfiguration when rule with prefix") {
    // Configure a mock S3 client so that we don't hit errors when trying to access AWS in tests.
    val mockS3Client = mock[AmazonS3Client](Mockito.RETURNS_SMART_NULLS)

    when(mockS3Client.getBucketLifecycleConfiguration(anyString())).thenReturn(
      new BucketLifecycleConfiguration().withRules(
        new Rule().withPrefix("/path/").withStatus(BucketLifecycleConfiguration.ENABLED)
      ))
    assert(Utils.checkThatBucketHasObjectLifecycleConfiguration(
      "s3a://bucket/path/to/temp/dir", mockS3Client) === true)
  }

  test("checkThatBucketHasObjectLifecycleConfiguration when rule without prefix") {
    // Configure a mock S3 client so that we don't hit errors when trying to access AWS in tests.
    val mockS3Client = mock[AmazonS3Client](Mockito.RETURNS_SMART_NULLS)

    when(mockS3Client.getBucketLifecycleConfiguration(anyString())).thenReturn(
      new BucketLifecycleConfiguration().withRules(
        new Rule().withStatus(BucketLifecycleConfiguration.ENABLED)
      ))
    assert(Utils.checkThatBucketHasObjectLifecycleConfiguration(
      "s3a://bucket/path/to/temp/dir", mockS3Client) === true)
  }

  test("checkThatBucketHasObjectLifecycleConfiguration when error in checking") {
    // Configure a mock S3 client so that we don't hit errors when trying to access AWS in tests.
    val mockS3Client = mock[AmazonS3Client](Mockito.RETURNS_SMART_NULLS)

    when(mockS3Client.getBucketLifecycleConfiguration(anyString()))
      .thenThrow(new NullPointerException())
    assert(Utils.checkThatBucketHasObjectLifecycleConfiguration(
      "s3a://bucket/path/to/temp/dir", mockS3Client) === false)
  }

  test("retry calls block correct number of times with correct delay") {
    val timeToSleep = 100
    var timesCalled = 0
    val startTime = System.currentTimeMillis()

    try {
      Utils.retry(1, timeToSleep) {
        timesCalled += 1
        throw new Exception("Failure")
      }
    } catch {
      case exception: Exception => assert(exception.getMessage == "Failure")
    }
    val timeTaken = System.currentTimeMillis() - startTime

    assert(timesCalled == 2)
    // check that at least timeToSleep passed but allow for slightly longer
    assert(timeTaken >= timeToSleep && timeTaken <= timeToSleep + 50)
  }
 val fakeCredentials: Map[String, String] =
   Map[String, String]("forward_spark_s3_credentials" -> "true",
     Parameters.PARAM_LEGACY_JDBC_REAL_TYPE_MAPPING -> "false",
     Parameters.PARAM_OVERRIDE_NULLABLE -> "false")

  test("collectMetrics logs buildinfo to INFO") {
    val mockLogger = mock[Logger]
    Utils.collectMetrics(MergedParameters(fakeCredentials), Some(mockLogger))

    verify(mockLogger).info(BuildInfo.toString)
  }

  test("collectMetrics outputs unique log to INFO when version includes -amzn- INFO") {
    val mockLogger = mock[Logger]
    Utils.collectMetrics(MergedParameters(fakeCredentials), Some(mockLogger))
    if (BuildInfo.version.contains("-amzn-")) {
      verify(mockLogger).info("amazon-spark-redshift-connector")
    }
  }

  test("collectMetrics logs to INFO level when ParamLegacyJDBCRealTypeMapping is enabled") {
    val mockLogger = mock[Logger]
    val fakeCredentialsOverride = fakeCredentials +
      (Parameters.PARAM_LEGACY_JDBC_REAL_TYPE_MAPPING -> "true")
    val params = MergedParameters(fakeCredentialsOverride)

    Utils.collectMetrics(params, Some(mockLogger))
      verify(mockLogger).info(s"${Parameters.PARAM_LEGACY_JDBC_REAL_TYPE_MAPPING} is enabled")
  }

  test("collectMetrics logs to INFO level when param OverrideNullable is enabled") {
    val mockLogger = mock[Logger]
    val fakeCredentialsOverride = fakeCredentials +
      (Parameters.PARAM_OVERRIDE_NULLABLE -> "true")
    val params = MergedParameters(fakeCredentialsOverride)

    Utils.collectMetrics(params, Some(mockLogger))
      verify(mockLogger).info(s"${Parameters.PARAM_OVERRIDE_NULLABLE} is enabled")
  }

  test("collectMetrics does not log when param LegacyJdbcRealTypeMapping is disabled") {
    val mockLogger = mock[Logger]
    val params = MergedParameters(fakeCredentials)

    Utils.collectMetrics(params, Some(mockLogger))
    verify(mockLogger, never()).info(s"${Parameters.PARAM_LEGACY_JDBC_REAL_TYPE_MAPPING} is enabled")
  }

  test("collectMetrics does not log when param OverrideNullable is disabled") {
    val mockLogger = mock[Logger]
    val params = MergedParameters(fakeCredentials)

    Utils.collectMetrics(params, Some(mockLogger))
    verify(mockLogger, never()).info(s"${Parameters.PARAM_OVERRIDE_NULLABLE} is enabled")
  }

  test("copyProperty and copyProperties map and convert matching parameter names") {
    val sourceProps = Map[String, String](
      "param1" -> "value1", // Ignore
      "param2" -> "value2", // Copy
      "param3" -> "value3", // Copy
      "jdbc.param4" -> "value4", // Copy
      "jdbc.param5" -> "value5", // Copy
      "secret.param6" -> "value6", // Copy
      "secret.param7" -> "value7", // Copy
      "jdbc." -> "jdbcValue", // Ignore
      "secret." -> "secretValue" // Ignore
    )

    val destProps = new Properties()
    Utils.copyProperty("param2", sourceProps, destProps)
    Utils.copyProperty("param3", "param33", sourceProps, destProps)
    Utils.copyProperties("^jdbc\\..+", "^jdbc\\.", "", sourceProps, destProps)
    Utils.copyProperties("^secret\\..+", "^secret\\.", "drivers\\.", sourceProps, destProps)

    assert(destProps.size() == 6)
    assert(destProps.getProperty("param2") == "value2")
    assert(destProps.getProperty("param33") == "value3")
    assert(destProps.getProperty("param4") == "value4")
    assert(destProps.getProperty("param5") == "value5")
    assert(destProps.getProperty("drivers.param6") == "value6")
    assert(destProps.getProperty("drivers.param7") == "value7")
  }
}
