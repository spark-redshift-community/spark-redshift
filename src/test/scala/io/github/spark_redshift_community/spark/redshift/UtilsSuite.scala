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
import io.github.spark_redshift_community.spark.redshift.Parameters.{MergedParameters, PARAM_HOST_CONNECTOR, PARAM_LEGACY_JDBC_REAL_TYPE_MAPPING, PARAM_LEGACY_MAPPING_SHORT_TO_INT, PARAM_LEGACY_TRIM_CSV_WRITES, PARAM_OVERRIDE_NULLABLE}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito
import org.mockito.Mockito.{never, verify, when}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatestplus.mockito.MockitoSugar.mock
import org.scalatest.matchers.should._
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.Logger

import java.sql.Timestamp
import java.util
import java.util.Properties

/**
 * Unit tests for helper functions
 */
class UtilsSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private var sc: SparkContext = _
  private var sqlContext: SQLContext = _

  private val fakeCredentials: Map[String, String] =
    Map[String, String]("forward_spark_s3_credentials" -> "true",
      Parameters.PARAM_LEGACY_JDBC_REAL_TYPE_MAPPING -> "false",
      Parameters.PARAM_LEGACY_TRIM_CSV_WRITES -> "false",
      Parameters.PARAM_LEGACY_MAPPING_SHORT_TO_INT -> "false",
      Parameters.PARAM_OVERRIDE_NULLABLE -> "false",
      "tempdir" -> "s3a://bucket/path/to/temp/dir",
      "url" -> "jdbc:redshift://redshift/database")
  private val fakeParams = MergedParameters(fakeCredentials)
      
  override def beforeAll(): Unit = {
    sc = new SparkContext("local", "UtilsSuite")
    sqlContext = new SQLContext(sc)
  }

  override def afterAll(): Unit = {
    sc.stop()
    sc = null
    sqlContext = null
  }

  private def resetAll(): Unit = {
    sqlContext.sparkSession.sql(s"reset ${Utils.CONNECTOR_LABEL_SPARK_CONF}")
    sqlContext.sparkSession.sql(s"reset ${Utils.CONNECTOR_TRACE_ID_SPARK_CONF}")
    unsetEnv(Utils.CONNECTOR_SERVICE_NAME_ENV_VAR)
    unsetEnv(Utils.CONNECTOR_TRACE_ID_ENV_VAR)
  }
  override def beforeEach(): Unit = resetAll()
  override def afterEach(): Unit = resetAll()

  test("joinUrls preserves protocol information") {
    Utils.joinUrls("s3n://foo/bar/", "/baz") shouldBe "s3n://foo/bar/baz/"
    Utils.joinUrls("s3n://foo/bar/", "/baz/") shouldBe "s3n://foo/bar/baz/"
    Utils.joinUrls("s3n://foo/bar/", "baz/") shouldBe "s3n://foo/bar/baz/"
    Utils.joinUrls("s3n://foo/bar/", "baz") shouldBe "s3n://foo/bar/baz/"
    Utils.joinUrls("s3n://foo/bar", "baz") shouldBe "s3n://foo/bar/baz/"
  }

  def getEditableEnv: util.Map[String, String] = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    field.get(System.getenv()).asInstanceOf[util.Map[String, String]]
  }

  def setEnv(key: String, value: String): Unit = {
    getEditableEnv.put(key, value)
  }

  def unsetEnv(key: String): Unit = {
    getEditableEnv.remove(key)
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
      fakeParams, mockS3Client) === true)
  }

  test("checkThatBucketHasObjectLifecycleConfiguration when rule with prefix") {
    // Configure a mock S3 client so that we don't hit errors when trying to access AWS in tests.
    val mockS3Client = mock[AmazonS3Client](Mockito.RETURNS_SMART_NULLS)

    when(mockS3Client.getBucketLifecycleConfiguration(anyString())).thenReturn(
      new BucketLifecycleConfiguration().withRules(
        new Rule().withPrefix("/path/").withStatus(BucketLifecycleConfiguration.ENABLED)
      ))
    assert(Utils.checkThatBucketHasObjectLifecycleConfiguration(
      fakeParams, mockS3Client) === true)
  }

  test("checkThatBucketHasObjectLifecycleConfiguration when rule without prefix") {
    // Configure a mock S3 client so that we don't hit errors when trying to access AWS in tests.
    val mockS3Client = mock[AmazonS3Client](Mockito.RETURNS_SMART_NULLS)

    when(mockS3Client.getBucketLifecycleConfiguration(anyString())).thenReturn(
      new BucketLifecycleConfiguration().withRules(
        new Rule().withStatus(BucketLifecycleConfiguration.ENABLED)
      ))
    assert(Utils.checkThatBucketHasObjectLifecycleConfiguration(
      fakeParams, mockS3Client) === true)
  }

  test("checkThatBucketHasObjectLifecycleConfiguration when error in checking") {
    // Configure a mock S3 client so that we don't hit errors when trying to access AWS in tests.
    val mockS3Client = mock[AmazonS3Client](Mockito.RETURNS_SMART_NULLS)

    when(mockS3Client.getBucketLifecycleConfiguration(anyString()))
      .thenThrow(new NullPointerException())
    assert(Utils.checkThatBucketHasObjectLifecycleConfiguration(
      fakeParams, mockS3Client) === false)
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

  test("collectMetrics logs buildinfo to INFO") {
    val mockLogger = mock[Logger]
    Utils.collectMetrics(fakeParams, Some(mockLogger))

    verify(mockLogger).info(BuildInfo.toString)
  }

  test("collectMetrics logs to INFO level when ParamLegacyJDBCRealTypeMapping is enabled") {
    val mockLogger = mock[Logger]
    val fakeCredentialsOverride = fakeCredentials +
      (Parameters.PARAM_LEGACY_JDBC_REAL_TYPE_MAPPING -> "true")
    val params = MergedParameters(fakeCredentialsOverride)

    Utils.collectMetrics(params, Some(mockLogger))
      verify(mockLogger).info(s"${Parameters.PARAM_LEGACY_JDBC_REAL_TYPE_MAPPING} is enabled")
  }

  test("collectMetrics logs to INFO level when param LegacyTrimCSV is enabled") {
    val mockLogger = mock[Logger]
    val fakeCredentialsOverride = fakeCredentials +
      (Parameters.PARAM_LEGACY_TRIM_CSV_WRITES -> "true")
    val params = MergedParameters(fakeCredentialsOverride)

    Utils.collectMetrics(params, Some(mockLogger))
    verify(mockLogger).info(s"${Parameters.PARAM_LEGACY_TRIM_CSV_WRITES} is enabled")
  }

  test("collectMetrics logs to INFO level when param legacyMappingShortToInt is enabled") {
    val mockLogger = mock[Logger]
    val fakeCredentialsOverride = fakeCredentials +
      (Parameters.PARAM_LEGACY_MAPPING_SHORT_TO_INT -> "true")
    val params = MergedParameters(fakeCredentialsOverride)

    Utils.collectMetrics(params, Some(mockLogger))
    verify(mockLogger).info(s"${Parameters.PARAM_LEGACY_MAPPING_SHORT_TO_INT} is enabled")
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
    val params = fakeParams

    Utils.collectMetrics(params, Some(mockLogger))
    verify(mockLogger, never()).info(s"${Parameters.PARAM_LEGACY_JDBC_REAL_TYPE_MAPPING} is enabled")
  }

  test("collectMetrics does not log when param LegacyTrimCSV is disabled") {
    val mockLogger = mock[Logger]
    val params = fakeParams

    Utils.collectMetrics(params, Some(mockLogger))
    verify(mockLogger, never()).info(s"${Parameters.PARAM_LEGACY_TRIM_CSV_WRITES} is enabled")
  }

  test("collectMetrics does not log when param legacyMappingShortToInt is disabled") {
    val mockLogger = mock[Logger]
    val params = fakeParams

    Utils.collectMetrics(params, Some(mockLogger))
    verify(mockLogger, never()).info(s"${Parameters.PARAM_LEGACY_MAPPING_SHORT_TO_INT} is enabled")
  }

  test("collectMetrics does not log when param OverrideNullable is disabled") {
    val mockLogger = mock[Logger]
    val params = fakeParams

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

  test("User provided service is trimmed in queryGroupInfo") {
    val params = fakeParams
    val longString = ("a" * 10) + ("b" * 10)
    val expectedService = "a" * 10
    val expectedString = s""""svc":"$expectedService""""
    setEnv(Utils.CONNECTOR_SERVICE_NAME_ENV_VAR, longString)
    val actualQueryGroup = Utils.queryGroupInfo(Utils.Read, params, sqlContext)
    assert(actualQueryGroup.contains(expectedString))
  }

  test("User provided host connector is trimmed in queryGroupInfo") {
    val longString = ("a" * 10) + ("b" * 10)
    val expectedHost = "a" * 10
    val expectedString = s""""hst":"$expectedHost""""
    val params = MergedParameters(fakeCredentials + (PARAM_HOST_CONNECTOR -> longString))
    val actualQueryGroup = Utils.queryGroupInfo(Utils.Read, params, sqlContext)
    assert(actualQueryGroup.contains(expectedString))
  }

  test("User provided label from property is trimmed in queryGroupInfo") {
    val longString = ("a" * 100) + ("b" * 100)
    val expectedLabel = "a" * 100
    val expectedString = s""""lbl":"$expectedLabel""""
    val params = MergedParameters(fakeCredentials + ("label" -> longString))
    val actualQueryGroup = Utils.queryGroupInfo(Utils.Read, params, sqlContext)
    assert(actualQueryGroup.contains(expectedString))
  }

  test("User provided label from config is trimmed in queryGroupInfo") {
    val params = fakeParams
    val longString = ("a" * 100) + ("b" * 100)
    val expectedLabel = "a" * 100
    val expectedString = s""""lbl":"$expectedLabel""""
    sqlContext.sparkSession.sql(
      s"set ${Utils.CONNECTOR_LABEL_SPARK_CONF} = $longString")
    val actualQueryGroup = Utils.queryGroupInfo(Utils.Read, params, sqlContext)
    assert(actualQueryGroup.contains(expectedString))
  }

  test("User provided trace from config is trimmed in queryGroupInfo") {
    val params = fakeParams
    val longString = ("a" * 75) + ("b" * 75)
    val expectedTrace = "a" * 75
    val expectedString = s""""tid":"$expectedTrace""""
    sqlContext.sparkSession.sql(
      s"set ${Utils.CONNECTOR_TRACE_ID_SPARK_CONF} = $longString")
    val actualQueryGroup = Utils.queryGroupInfo(Utils.Read, params, sqlContext)
    assert(actualQueryGroup.contains(expectedString))
  }

  test("User provided trace from env is trimmed in queryGroupInfo") {
    val params = fakeParams
    val longString = ("a" * 75) + ("b" * 75)
    val expectedTrace = "a" * 75
    val expectedString = s""""tid":"$expectedTrace""""
    setEnv(Utils.CONNECTOR_TRACE_ID_ENV_VAR, longString)
    val actualQueryGroup = Utils.queryGroupInfo(Utils.Read, params, sqlContext)
    assert(actualQueryGroup.contains(expectedString))
  }

  test("Maximum queryGroupInfo is no more than 320 characters in length") {
    setEnv(Utils.CONNECTOR_SERVICE_NAME_ENV_VAR, "a" * 1000)
    val params = MergedParameters(fakeCredentials + (PARAM_HOST_CONNECTOR -> "b" * 1000))
    sqlContext.sparkSession.sql(s"set ${Utils.CONNECTOR_LABEL_SPARK_CONF} = ${"c" * 1000}")
    sqlContext.sparkSession.sql(s"set ${Utils.CONNECTOR_TRACE_ID_SPARK_CONF} = ${"d" * 1000}")
    val actualQueryGroup = Utils.queryGroupInfo(Utils.Write, params, sqlContext)
    assert(actualQueryGroup.length <= 320)
  }

  test("pre-GA regions are permitted") {
    val specifiedRegion = Utils.getDefaultTempDirRegion(Some("pre-ga-region"))
    assert(specifiedRegion == "pre-ga-region")

    val defaultRegion = Utils.getDefaultTempDirRegion(None)
    assert(defaultRegion.isEmpty == false)
  }

  /*
   * Validate Utils.getResourceIdForARN()
   * See: https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html
   */
  test("Verify splitting resource identifiers from ARNs") {
    assert(Utils.getResourceIdForARN(
      "arn:partition:service:region:account-id:resource-id") == "resource-id")
    assert(Utils.getResourceIdForARN(
      "arn:partition:service:region:account-id:resource-type/resource-id") == "resource-id")
    assert(Utils.getResourceIdForARN(
      "arn:partition:service:region:account-id:resource-type:resource-id") == "resource-id")
  }

  test("Ensure default app name is non empty and only alpha characters") {
    assert(Utils.getApplicationName(fakeParams).matches("^[a-zA-Z]+$"))
  }

  test("Test application name with host service name.") {
    assert(Utils.getApplicationName(fakeParams).equals(Utils.DEFAULT_APP_NAME))

    // Valid
    setEnv(Utils.CONNECTOR_SERVICE_NAME_ENV_VAR, "MySvcName")
    assert(Utils.getApplicationName(fakeParams).equals(Utils.DEFAULT_APP_NAME + "MySvcName"))

    // Invalid
    setEnv(Utils.CONNECTOR_SERVICE_NAME_ENV_VAR, "MySvc Name")
    assert(Utils.getApplicationName(fakeParams).equals(Utils.DEFAULT_APP_NAME))

    // Invalid
    setEnv(Utils.CONNECTOR_SERVICE_NAME_ENV_VAR, "MySvcName1")
    assert(Utils.getApplicationName(fakeParams).equals(Utils.DEFAULT_APP_NAME))
  }


  test("Test application name with host connector name.") {
    var params = fakeParams
    assert(Utils.getApplicationName(params).equals(Utils.DEFAULT_APP_NAME))

    // Valid
    params = MergedParameters(fakeCredentials + (PARAM_HOST_CONNECTOR -> "MyHstName"))
    assert(Utils.getApplicationName(params).equals(Utils.DEFAULT_APP_NAME + "MyHstName"))

    // Invalid
    params = MergedParameters(fakeCredentials + (PARAM_HOST_CONNECTOR -> "MyHst Name"))
    assert(Utils.getApplicationName(params).equals(Utils.DEFAULT_APP_NAME))

    // Invalid
    params = MergedParameters(fakeCredentials + (PARAM_HOST_CONNECTOR -> "MyHstName1"))
    assert(Utils.getApplicationName(params).equals(Utils.DEFAULT_APP_NAME))
  }

  test("Test application name with both host service and host connector names.") {
    var params = fakeParams
    assert(Utils.getApplicationName(params).equals(Utils.DEFAULT_APP_NAME))

    // Valid
    setEnv(Utils.CONNECTOR_SERVICE_NAME_ENV_VAR, "MySvcName")
    params = MergedParameters(fakeCredentials + (PARAM_HOST_CONNECTOR -> "MyHstName"))
    assert(Utils.getApplicationName(params).equals(Utils.DEFAULT_APP_NAME + "MySvcName" + "MyHstName"))

    // Invalid service
    setEnv(Utils.CONNECTOR_SERVICE_NAME_ENV_VAR, "MySvc Name")
    params = MergedParameters(fakeCredentials + (PARAM_HOST_CONNECTOR -> "MyHstName"))
    assert(Utils.getApplicationName(params).equals(Utils.DEFAULT_APP_NAME + "MyHstName"))

    // Invalid host
    setEnv(Utils.CONNECTOR_SERVICE_NAME_ENV_VAR, "MySvcName")
    params = MergedParameters(fakeCredentials + (PARAM_HOST_CONNECTOR -> "MyHst Name"))
    assert(Utils.getApplicationName(params).equals(Utils.DEFAULT_APP_NAME + "MySvcName"))

    // Invalid both
    setEnv(Utils.CONNECTOR_SERVICE_NAME_ENV_VAR, "MySvc Name")
    params = MergedParameters(fakeCredentials + (PARAM_HOST_CONNECTOR -> "MyHst Name"))
    assert(Utils.getApplicationName(params).equals(Utils.DEFAULT_APP_NAME))
  }

  test("Test supported label names") {
    // Default
    var params = fakeParams
    var actualQueryGroup = Utils.queryGroupInfo(Utils.Read, params, sqlContext)
    assert(actualQueryGroup.contains(""""lbl":"""""))

    // Valid
    params = MergedParameters(fakeCredentials + ("label" -> "Label1"))
    actualQueryGroup = Utils.queryGroupInfo(Utils.Read, params, sqlContext)
    assert(actualQueryGroup.contains(""""lbl":"Label1""""))

    // Valid
    params = MergedParameters(fakeCredentials + ("label" -> "Label_1"))
    actualQueryGroup = Utils.queryGroupInfo(Utils.Read, params, sqlContext)
    assert(actualQueryGroup.contains(""""lbl":"Label_1""""))

    // Invalid
    params = MergedParameters(fakeCredentials + ("label" -> "Label 1"))
    actualQueryGroup = Utils.queryGroupInfo(Utils.Read, params, sqlContext)
    assert(actualQueryGroup.contains(""""lbl":"""""))

    // Invalid
    params = MergedParameters(fakeCredentials + ("label" -> "Label-1"))
    actualQueryGroup = Utils.queryGroupInfo(Utils.Read, params, sqlContext)
    assert(actualQueryGroup.contains(""""lbl":"""""))
  }

  test("Test supported trace identifiers") {
    // Default is Spark application id
    var actualQueryGroup = Utils.queryGroupInfo(Utils.Read, fakeParams, sqlContext)
    assert(actualQueryGroup.contains(s""""tid":"${sc.applicationId}"""))

    // Valid
    sqlContext.sparkSession.sql(s"set ${Utils.CONNECTOR_TRACE_ID_SPARK_CONF} = Trace-1")
    actualQueryGroup = Utils.queryGroupInfo(Utils.Read, fakeParams, sqlContext)
    assert(actualQueryGroup.contains(""""tid":"Trace-1""""))

    // Valid
    sqlContext.sparkSession.sql(s"set ${Utils.CONNECTOR_TRACE_ID_SPARK_CONF} = Trace_1")
    actualQueryGroup = Utils.queryGroupInfo(Utils.Read, fakeParams, sqlContext)
    assert(actualQueryGroup.contains(""""tid":"Trace_1""""))

    // Valid
    sqlContext.sparkSession.sql(s"set ${Utils.CONNECTOR_TRACE_ID_SPARK_CONF} = 1Trace")
    actualQueryGroup = Utils.queryGroupInfo(Utils.Read, fakeParams, sqlContext)
    assert(actualQueryGroup.contains(s""""tid":"1Trace""""))

    // Invalid - Default to spark application id.
    sqlContext.sparkSession.sql(s"set ${Utils.CONNECTOR_TRACE_ID_SPARK_CONF} = Trace 1")
    actualQueryGroup = Utils.queryGroupInfo(Utils.Read, fakeParams, sqlContext)
    assert(actualQueryGroup.contains(s""""tid":"${sc.applicationId}"""))
  }

  test("Test label property has precedence over Spark configuration") {
    val params = MergedParameters(fakeCredentials + ("label" -> "Label1"))
    sqlContext.sparkSession.sql(s"set ${Utils.CONNECTOR_LABEL_SPARK_CONF} = Label2")
    val actualQueryGroup = Utils.queryGroupInfo(Utils.Read, params, sqlContext)
    assert(actualQueryGroup.contains(""""lbl":"Label1""""))
  }

  test("Test trace configuration setting has precedence over env var") {
    val params = fakeParams
    sqlContext.sparkSession.sql(s"set ${Utils.CONNECTOR_TRACE_ID_SPARK_CONF} = Trace-1")
    setEnv(Utils.CONNECTOR_TRACE_ID_ENV_VAR, "Trace-2")
    val actualQueryGroup = Utils.queryGroupInfo(Utils.Read, params, sqlContext)
    assert(actualQueryGroup.contains(""""tid":"Trace-1""""))
  }
}
