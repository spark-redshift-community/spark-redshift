/*
 * Copyright 2015-2018 Snowflake Computing
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

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.Regions
import io.github.spark_redshift_community.spark.redshift.Parameters.MergedParameters
import com.amazonaws.services.s3.model.{BucketLifecycleConfiguration, HeadBucketRequest}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client, AmazonS3URI}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SQLContext
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI
import java.sql.Timestamp
import java.util.{Properties, UUID}
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal


object RedshiftFailMessage {
  // Note: don't change the message context except necessary
  final val FAIL_PUSHDOWN_STATEMENT = "pushdown failed"
  final val FAIL_PUSHDOWN_GENERATE_QUERY = "pushdown failed in generateQueries"
  final val FAIL_PUSHDOWN_SET_TO_EXPR = "pushdown failed in setToExpr"
  final val FAIL_PUSHDOWN_AGGREGATE_EXPRESSION = "pushdown failed for aggregate expression"
  final val FAIL_PUSHDOWN_UNSUPPORTED_CONVERSION = "pushdown failed for unsupported conversion"
  final val FAIL_PUSHDOWN_UNSUPPORTED_JOIN = "pushdown failed for unsupported join"
  final val FAIL_PUSHDOWN_UNSUPPORTED_UNION = "pushdown failed for Spark feature: UNION by name"
}

class RedshiftPushdownException(message: String)
  extends Exception(message)

class RedshiftPushdownUnsupportedException(message: String,
                                            val unsupportedOperation: String,
                                            val details: String,
                                            val isKnownUnsupportedOperation: Boolean)
  extends Exception(message)

/**
 * Various arbitrary helper functions
 */
private[redshift] object Utils {

  private val log = LoggerFactory.getLogger(getClass)

  val lastBuildStmt: mutable.Map[String, String] = mutable.Map[String, String]()

  def classForName(className: String): Class[_] = {
    val classLoader =
      Option(Thread.currentThread().getContextClassLoader).getOrElse(this.getClass.getClassLoader)
    // scalastyle:off
    Class.forName(className, true, classLoader)
    // scalastyle:on
  }

  /**
   * Joins prefix URL a to path suffix b, and appends a trailing /, in order to create
   * a temp directory path for S3.
   */
  def joinUrls(a: String, b: String): String = {
    a.stripSuffix("/") + "/" + b.stripPrefix("/").stripSuffix("/") + "/"
  }

  /**
   * Redshift COPY and UNLOAD commands don't support s3n or s3a, but users may wish to use them
   * for data loads. This function converts the URL back to the s3:// format.
   */
  def fixS3Url(url: String): String = {
    url.replaceAll("s3[an]://", "s3://")
  }

  /**
   * Factory method to create new S3URI in order to handle various library incompatibilities with
   * older AWS Java Libraries
   */
  def createS3URI(url: String): AmazonS3URI = {
    try {
      // try to instantiate AmazonS3URI with url
      new AmazonS3URI(url)
    } catch {
      case e: IllegalArgumentException if e.getMessage.
        startsWith("Invalid S3 URI: hostname does not appear to be a valid S3 endpoint") => {
        new AmazonS3URI(addEndpointToUrl(url))
      }
    }
  }

  /**
   * Since older AWS Java Libraries do not handle S3 urls that have just the bucket name
   * as the host, add the endpoint to the host
   */
  def addEndpointToUrl(url: String, domain: String = "s3.amazonaws.com"): String = {
    val uri = new URI(url)
    val hostWithEndpoint = uri.getHost + "." + domain
    new URI(uri.getScheme,
      uri.getUserInfo,
      hostWithEndpoint,
      uri.getPort,
      uri.getPath,
      uri.getQuery,
      uri.getFragment).toString
  }

  /**
   * Returns a copy of the given URI with the user credentials removed.
   */
  def removeCredentialsFromURI(uri: URI): URI = {
    new URI(
      uri.getScheme,
      null, // no user info
      uri.getHost,
      uri.getPort,
      uri.getPath,
      uri.getQuery,
      uri.getFragment)
  }

  // Visible for testing
  private[redshift] var lastTempPathGenerated: String = null

  /**
   * Creates a randomly named temp directory path for intermediate data
   */
  def makeTempPath(tempRoot: String): String = {
    val _lastTempPathGenerated = Utils.joinUrls(tempRoot, UUID.randomUUID().toString)
    lastTempPathGenerated = _lastTempPathGenerated
    _lastTempPathGenerated
  }

  /**
   * Checks whether the S3 bucket for the given UI has an object lifecycle configuration to
   * ensure cleanup of temporary files. If no applicable configuration is found, this method logs
   * a helpful warning for the user.
   * @return {Boolean} true if check has been executed or
   *                   false if an error prevent the check (useful for testing).
   */
  def checkThatBucketHasObjectLifecycleConfiguration(
      tempDir: String,
      s3Client: AmazonS3): Boolean = {
    try {
      val s3URI = createS3URI(Utils.fixS3Url(tempDir))
      val bucket = s3URI.getBucket
      assert(bucket != null, "Could not get bucket from S3 URI")
      val key = Option(s3URI.getKey).getOrElse("")
      val hasMatchingBucketLifecycleRule: Boolean = {
        val rules = Option(s3Client.getBucketLifecycleConfiguration(bucket))
          .map(_.getRules.asScala)
          .getOrElse(Seq.empty)
        rules.exists { rule =>
          // Note: this only checks that there is an active rule which matches the temp directory;
          // it does not actually check that the rule will delete the files. This check is still
          // better than nothing, though, and we can always improve it later.
          rule.getStatus == BucketLifecycleConfiguration.ENABLED &&
            (rule.getPrefix == null || key.startsWith(rule.getPrefix))
        }
      }
      if (!hasMatchingBucketLifecycleRule) {
        log.warn(s"The S3 bucket $bucket does not have an object lifecycle configuration to " +
          "ensure cleanup of temporary files. Consider configuring `tempdir` to point to a " +
          "bucket with an object lifecycle policy that automatically deletes files after an " +
          "expiration period. For more information, see " +
          "https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html")
      }
      true
    } catch {
      case NonFatal(e) =>
        log.warn("An error occurred while trying to read the S3 bucket lifecycle configuration")
        false
    }
  }

  /**
   * Given a URI, verify that the Hadoop FileSystem for that URI is not the S3 block FileSystem.
   * `spark-redshift` cannot use this FileSystem because the files written to it will not be
   * readable by Redshift (and vice versa).
   */
  def assertThatFileSystemIsNotS3BlockFileSystem(uri: URI, hadoopConfig: Configuration): Unit = {
    val fs = FileSystem.get(uri, hadoopConfig)
    // Note that we do not want to use isInstanceOf here, since we're only interested in detecting
    // exact matches. We compare the class names as strings in order to avoid introducing a binary
    // dependency on classes which belong to the `hadoop-aws` JAR, as that artifact is not present
    // in some environments (such as EMR). See #92 for details.
    if (fs.getClass.getCanonicalName == "org.apache.hadoop.fs.s3.S3FileSystem") {
      throw new IllegalArgumentException(
        "spark-redshift does not support the S3 Block FileSystem. Please reconfigure `tempdir` to" +
        "use a s3n:// or s3a:// scheme.")
    }
  }

  /**
   * Attempts to retrieve the region of the S3 bucket.
   */
  def getRegionForS3Bucket(tempDir: String, s3Client: AmazonS3): Option[String] = {
    try {
      val s3URI = createS3URI(Utils.fixS3Url(tempDir))
      val bucket = s3URI.getBucket
      assert(bucket != null, "Could not get bucket from S3 URI")
      val region = s3Client.headBucket(new HeadBucketRequest(bucket)).getBucketRegion match {
        // Map "US Standard" to us-east-1
        case null | "US" => "us-east-1"
        case other => other
      }
      Some(region)
    } catch {
      case NonFatal(e) =>
        log.warn("An error occurred while trying to determine the S3 bucket's region", e)
        None
    }
  }

  /**
   * Attempts to determine the region of a Redshift cluster based on its URL. It may not be possible
   * to determine the region in some cases, such as when the Redshift cluster is placed behind a
   * proxy.
   */
  def getRegionForRedshiftCluster(url: String): Option[String] = {
    val regionRegex = """.*\.([^.]+)\.redshift\.amazonaws\.com.*""".r
    url match {
      case regionRegex(region) => Some(region)
      case _ => None
    }
  }

  def checkRedshiftAndS3OnSameRegion(params: MergedParameters, s3Client: AmazonS3): Unit = {
    for (
      redshiftRegion <- Utils.getRegionForRedshiftCluster(params.jdbcUrl);
      s3Region <- Utils.getRegionForS3Bucket(params.rootTempDir, s3Client)
    ) {
      if ((redshiftRegion != s3Region) && params.tempDirRegion.isEmpty) {
        log.error("The Redshift cluster and S3 bucket are in different regions " +
          s"($redshiftRegion and $s3Region, respectively). In order to perform this cross-region " +
          s"""operation, you should set the tempdir_region parameter to '$s3Region'. """ +
          "For more details on cross-region usage, see the README.")
      }
    }
  }

  /**
   * Performs the same check as checkRedshiftAndS3OnSameRegion but ignores the tempdir_region
   * since when parquet is used as the copy format, the region copy option is not available.
   * In addition if the check is failed throw an exception to prevent execution. This stricter
   * check is only applicable when using the connector to write.
   * @param params parameters configured on the connector
   * @param s3Client S3 client to use when getting the s3 bucket region
   */
  def checkRedshiftAndS3OnSameRegionParquetWrite
  (params: MergedParameters, s3Client: AmazonS3): Unit = {
    val redshiftRegion = Utils.getRegionForRedshiftCluster(params.jdbcUrl)
    if (redshiftRegion.isEmpty) {
      log.warn("Unable to determine region for redshift cluster, copy may fail if " +
        "S3 bucket region does not match redshift cluster region.")
      return
    }
    val s3Region = Utils.getRegionForS3Bucket(params.rootTempDir, s3Client)
    if (s3Region.isEmpty) {
      log.warn("Unable to determine region for S3 bucket, copy may fail if redshift cluster" +
        " region does not match S3 bucket region.")
      return
    }
    if (redshiftRegion.get != s3Region.get) {
      log.error("The Redshift cluster and S3 bucket are in different regions " +
        s"($redshiftRegion and $s3Region, respectively). Cross-region copy operation is not " +
        "available when tempformat is set to parquet")
      throw new IllegalArgumentException("Redshift cluster and S3 bucket are in different " +
        "regions when tempformat is set to parquet")
    }
  }

  def getDefaultTempDirRegion(tempDirRegion: Option[String]): Regions = {
    // If the user provided a region, use it above everything else.
    if (tempDirRegion.isDefined) return Regions.fromName(tempDirRegion.get)

    // Either the user didn't provide a region or its malformed. Try to use the
    // connector's region as the tempdir region since they are usually collocated.
    // If they aren't, S3's default provider chain will help resolve the difference.
    val currRegion = Regions.getCurrentRegion()

    // If the user didn't provide a valid tempdir region and we cannot determine
    // the connector's region, the connector is likely running outside of AWS.
    // In this case, warn the user about the performance penalty of not specifying
    // the tempdir region.
    if (currRegion == null) {
      log.warn(
        s"The connector cannot automatically determine a region for 'tempdir'. It " +
          "is highly recommended that the 'tempdir_region' parameter is set to " +
          "avoid a performance penalty while trying to automatically determine " +
          "a region, especially when operating outside of AWS.")
    }

    // If all else fails, pick a default region.
    if (currRegion != null) Regions.fromName(currRegion.getName()) else Regions.US_EAST_1
  }

  def s3ClientBuilder: (AWSCredentialsProvider, MergedParameters) => AmazonS3 =
    (awsCredentials, mergedParameters) => {
      AmazonS3Client.builder()
        .withRegion(Utils.getDefaultTempDirRegion(mergedParameters.tempDirRegion))
        .withForceGlobalBucketAccessEnabled(true)
        .withCredentials(awsCredentials)
        .build()
  }

  def collectMetrics(params: MergedParameters, logger: Option[Logger] = None): Unit = {
    val metricLogger = logger.getOrElse(log)

    // Emit the build information.
    metricLogger.info(BuildInfo.toString)

    // Track legacy parameters for deprecation
    if (params.legacyJdbcRealTypeMapping) {
      metricLogger.info(s"${Parameters.PARAM_LEGACY_JDBC_REAL_TYPE_MAPPING} is enabled")
    }
    if (params.overrideNullable) {
      metricLogger.info(s"${Parameters.PARAM_OVERRIDE_NULLABLE} is enabled")
    }

  }

  val DEFAULT_APP_NAME = "spark-redshift-connector"
  private val CONNECTOR_SERVICE_NAME_ENV_VAR = "AWS_SPARK_REDSHIFT_CONNECTOR_SERVICE_NAME"

  /**
   * Retrieve the name of the service using the connector.
   * Is none if environment variable is unset or empty.
   * @return trimmed service name
   */
  def connectorServiceName: Option[String] = sys.env.
    get(CONNECTOR_SERVICE_NAME_ENV_VAR).filter(_.trim.nonEmpty).map(_.trim)


  val CONNECTOR_TRACE_ID_ENV_VAR = "AWS_SPARK_REDSHIFT_CONNECTOR_TRACE_ID"
  val CONNECTOR_TRACE_ID_SPARK_CONF = "spark.datasource.redshift.community.trace_id"
  /**
   * Retrieve the spark configuration for traceId.
   * If not provided in spark configuration check environment variable.
   * If neither are provided then use applicationId.
   * @param sqlContext Context to check for
   * @return
   */
  def traceId(sqlContext: SQLContext): String = {
    val configuredValue = sqlContext.
      getConf(CONNECTOR_TRACE_ID_SPARK_CONF, sys.env.getOrElse(CONNECTOR_TRACE_ID_ENV_VAR, "")).trim
    val valueIsNotValid = configuredValue.
      exists(char => !(char.isUnicodeIdentifierPart || char == '-'))
    if (valueIsNotValid) {
      log.warn("Configured trace id is not valid. " +
        "It must only contain characters that are valid unicode identifier parts or '-'.")
    }
    if (valueIsNotValid || configuredValue.isEmpty) {
      sqlContext.sparkContext.applicationId
    } else {
      configuredValue
    }
  }

  sealed trait MetricOperation
  case object Read extends MetricOperation
  case object Write extends MetricOperation

  /**
   * Provides a string of json to be used for gathering metrics to be
   * sent to redshift as the query group for all queries associated
   * with a particular spark query. Limits the svc and lbl fields to
   * ensure the overall length does not exceed the redshift allowed
   * 320 characters for a query group.
   * @param operation the type of operation performed
   * @param label A user provided identifier to associate with a query
   *              should be sanitized before use with this function
   * @param sqlContext the sqlContext to check for a trace id
   * @return a string to be used as a query group
   */
  def queryGroupInfo(operation: MetricOperation, label: String, sqlContext: SQLContext): String = {
    val MAX_SVC_LENGTH = 30
    val MAX_LBL_LENGTH = 100
    val MAX_TID_LENGTH = 75
    val svcName = connectorServiceName.getOrElse("")
    val tid = traceId(sqlContext)
    val trimmedSvcName = svcName.substring(0, math.min(svcName.length, MAX_SVC_LENGTH))
    val trimmedLabel = label.substring(0, math.min(label.length, MAX_LBL_LENGTH))
    val trimmedTID = tid.substring(0, math.min(tid.length, MAX_TID_LENGTH))
    s"""{"${DEFAULT_APP_NAME}":{"svc":"${trimmedSvcName}",""" +
      s""""ver":"${BuildInfo.version}","op":"${operation}","lbl":"$trimmedLabel",""" +
      s""""tid":"${trimmedTID}"}}""".stripMargin
  }

  /**
   * Perform a block until it succeeds or retry count reaches zero.
   * Success is determined by whether the passed block throws
   * an exception or not. If the block is not successful and retry count
   * is zero this function will throw the last exception thrown while attempting the passed block.
   * @param count Number of times to attempt the block
   * @param delay Milliseconds to wait between attempts
   * @param retryBlock Block to execute
   * @tparam T Return type of the passed block
   * @return Result of the blocks successful execution
   */
  @tailrec
  def retry[T](count: Int, delay: Long)(retryBlock: => T): T = {
    try {
     retryBlock
    } catch {
      case e: Throwable => {
        if (count <= 0) {
          throw e
        }
        log.warn(s"Sleeping $delay milliseconds before proceeding to retry redshift operation;" +
          s" $count retries remain")
        Thread.sleep(delay)
        retry(count - 1, delay)(retryBlock)
      }
    }
  }

  /**
   * Copies a property (if it exists) from one set of properties to another.
   *
   * @param name        The name of the property.
   * @param sourceProps The set of source properties.
   * @param destProps   The set of destination properties.
   */
  def copyProperty(name: String,
                   sourceProps: Map[String, String],
                   destProps: Properties): Unit = {
    copyProperty(name, name, sourceProps, destProps)
  }

  /**
   * Copies a property (if it exists) from one set of properties to another while also
   * renaming the property.
   *
   * @param searchName  The name of the property to search within sourceProps.
   * @param replaceName The replacement name to write into destProps.
   * @param sourceProps The set of source properties.
   * @param destProps   The set of destination properties.
   */
  def copyProperty(searchName: String,
                   replaceName: String,
                   sourceProps: Map[String, String],
                   destProps: Properties): Unit = {
    sourceProps.get(searchName).foreach(destProps.setProperty(replaceName, _))
  }

  /**
   * Copy a set of properties from one collection to another with string replacement using
   * regular expressions.
   *
   * @param matchRegex   The regex used to match a property name.
   * @param searchRegex  The search regex used for replacing a property name.
   * @param replaceName  The name to use when replacing the property name.
   * @param sourceProps  The set of source properties.
   * @param destProps    The set of destination properties.
   */
  def copyProperties(matchRegex: String,
                     searchRegex: String,
                     replaceName: String,
                     sourceProps: Map[String, String],
                     destProps: Properties): Unit = {
    sourceProps.foreach {
      case (key, value) =>
        if (key.matches(matchRegex)) {
          destProps.setProperty(key.replaceFirst(searchRegex, replaceName), value)
        }
    }
  }
}
