/*
 *
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

import com.amazonaws.auth.{AWSCredentialsProvider, BasicSessionCredentials}

/**
 * All user-specifiable parameters for spark-redshift, along with their validation rules and
 * defaults.
 */
private[redshift] object Parameters {

  val PARAM_AUTO_PUSHDOWN: String = "autopushdown"
  val PARAM_PUSHDOWN_S3_RESULT_CACHE: String = "autopushdown.s3_result_cache"
  val PARAM_UNLOAD_S3_FORMAT: String = "unload_s3_format"
  val PARAM_COPY_RETRY_COUNT: String = "copyretrycount"
  val PARAM_COPY_DELAY: String = "copydelay"
  val PARAM_LEGACY_JDBC_REAL_TYPE_MAPPING: String = "legacy_jdbc_real_type_mapping"
  val PARAM_LEGACY_TRIM_CSV_WRITES: String = "legacy_trim_csv_writes"
  val PARAM_OVERRIDE_NULLABLE: String = "overridenullable"
  val PARAM_TEMPDIR_REGION: String = "tempdir_region"
  val PARAM_SECRET_ID: String = "secret.id"
  val PARAM_SECRET_REGION: String = "secret.region"
  val PARAM_USER_QUERY_GROUP_LABEL: String = "label"
  val PARAM_DATA_API_REGION: String = "data_api_region"
  val PARAM_DATA_API_DATABASE: String = "data_api_database"
  val PARAM_DATA_API_USER: String = "data_api_user"
  val PARAM_DATA_API_CLUSTER: String = "data_api_cluster"
  val PARAM_DATA_API_WORKGROUP: String = "data_api_workgroup"
  val PARAM_DATA_API_RETRY_DELAY_MIN = "data_api_retry_delay_min"
  val PARAM_DATA_API_RETRY_DELAY_MAX = "data_api_retry_delay_max"
  val PARAM_DATA_API_RETRY_DELAY_MULT = "data_api_retry_delay_mult"
  val PARAM_TEMPDIR_WRITE_ONLY = "tempdir_write_only"
  val PARAM_TEMPORARY_AWS_ACCESS_KEY_ID = "temporary_aws_access_key_id"
  val PARAM_TEMPORARY_AWS_SECRET_ACCESS_KEY = "temporary_aws_secret_access_key"
  val PARAM_TEMPORARY_AWS_SESSION_TOKEN = "temporary_aws_session_token"
  val PARAM_LEGACY_MAPPING_SHORT_TO_INT: String = "legacy_mapping_short_to_int"
  val PARAM_SKIP_DSW_WORKAROUND = "skip_dsw_workaround"
  val PARAM_IS_DELETE: String = "is_delete"

  val DEFAULT_PARAMETERS: Map[String, String] = Map(
    // Notes:
    // * tempdir, dbtable and url have no default and they *must* be provided
    // * sortkeyspec has no default, but is optional
    // * distkey has no default, but is optional unless using diststyle KEY
    // * jdbcdriver has no default, but is optional
    // * sse_kms_key has no default, but is optional

    "forward_spark_s3_credentials" -> "false",
    "tempformat" -> "AVRO",
    "csvnullstring" -> "@NULL@",
    "overwrite" -> "false",
    "diststyle" -> "EVEN",
    "usestagingtable" -> "true",
    "preactions" -> ";",
    "postactions" -> ";",
    "include_column_list" -> "false",
    PARAM_AUTO_PUSHDOWN -> "true",
    PARAM_PUSHDOWN_S3_RESULT_CACHE -> "false",
    PARAM_UNLOAD_S3_FORMAT -> "PARQUET", // values: PARQUET, TEXT
    PARAM_OVERRIDE_NULLABLE -> "false",
    PARAM_COPY_RETRY_COUNT -> "2",
    PARAM_COPY_DELAY -> "0",
    PARAM_LEGACY_JDBC_REAL_TYPE_MAPPING -> "false",
    PARAM_LEGACY_TRIM_CSV_WRITES -> "false",
    PARAM_TEMPDIR_REGION -> "",
    PARAM_USER_QUERY_GROUP_LABEL -> "",
    PARAM_DATA_API_RETRY_DELAY_MIN -> "100.0", // milliseconds
    PARAM_DATA_API_RETRY_DELAY_MAX -> "250.0", // milliseconds
    PARAM_DATA_API_RETRY_DELAY_MULT -> "1.25",  // Multiplier
    PARAM_USER_QUERY_GROUP_LABEL -> "",
    PARAM_LEGACY_MAPPING_SHORT_TO_INT -> "false",
    PARAM_SKIP_DSW_WORKAROUND -> "false",
    PARAM_IS_DELETE -> "false"
  )

  val VALID_TEMP_FORMATS = Set("AVRO", "CSV", "CSV GZIP", "PARQUET")
  val DEFAULT_RETRY_DELAY: Long = 30000

  private def dataAPICredentials(userParameters: Map[String, String]): Boolean = {
    userParameters.contains(PARAM_DATA_API_REGION) ||
    userParameters.contains(PARAM_DATA_API_DATABASE) ||
    userParameters.contains(PARAM_DATA_API_USER) ||
    userParameters.contains(PARAM_DATA_API_CLUSTER) ||
    userParameters.contains(PARAM_DATA_API_WORKGROUP)
  }

  private def checkUnsupportedParameters(userParameters: Map[String, String],
                                                incompatibleType: String,
                                                unsupportedParams: String*): Unit = {
    for (unsupportedParam <- unsupportedParams) {
      if (userParameters.contains(unsupportedParam)) {
        throw new IllegalArgumentException(
          s"The parameter '$unsupportedParam' is not compatible with $incompatibleType credentials.")
      }
    }
  }

  private def validateUserParameters(userParameters: Map[String, String]): Unit = {
    // Common validations
    if (!userParameters.contains("tempdir")) {
      throw new IllegalArgumentException("'tempdir' is required for all Redshift loads and saves")
    }
    if (userParameters.contains("tempformat") &&
      !VALID_TEMP_FORMATS.contains(userParameters("tempformat").toUpperCase)) {
      throw new IllegalArgumentException(
        s"""Invalid temp format: ${userParameters("tempformat")}; """ +
          s"valid formats are: ${VALID_TEMP_FORMATS.mkString(", ")}")
    }
    if (!userParameters.contains("dbtable") && !userParameters.contains("query")) {
      throw new IllegalArgumentException(
        "You must specify a Redshift table name with the 'dbtable' parameter or a query with the " +
          "'query' parameter.")
    }
    if (userParameters.contains("dbtable") && userParameters.contains("query")) {
      throw new IllegalArgumentException(
        "You cannot specify both the 'dbtable' and 'query' parameters at the same time.")
    }

    // Credential validations - specialized for JDBC vs DataAPI
    if (!dataAPICredentials(userParameters)) { // JDBC credentials
      if (!userParameters.contains("url")) {
        throw new IllegalArgumentException("A JDBC URL must be provided with 'url' parameter")
      }
      val credsInURL = userParameters.get("url")
        .filter(url => url.contains("user=") || url.contains("password=") || url.contains("DbUser="))
      if (userParameters.contains("user") || userParameters.contains("password")) {
        if (credsInURL.isDefined) {
          throw new IllegalArgumentException(
            "You cannot specify credentials in both the URL and as user/password options")
        }
      }
      if (credsInURL.isDefined && userParameters.contains(PARAM_SECRET_ID)) {
        throw new IllegalArgumentException(
          "You cannot give a secret and specify credentials in URL"
        )
      }
      if ((userParameters.contains("user") || userParameters.contains("password")) &&
        userParameters.contains(PARAM_SECRET_ID)) {
        throw new IllegalArgumentException(
          "You cannot give a secret and specify user/password options"
        )
      }
    } else { // Data API credentials
      // Check for JDBC parameters that are not compatible with DataAPI
      checkUnsupportedParameters(userParameters, "Data API",
        "user", "password", "url", "jdbcdriver", PARAM_SECRET_REGION, "secret.vpcEndpointUrl",
        "secret.vpcEndpointRegion", PARAM_USER_QUERY_GROUP_LABEL)
      userParameters.foreach {
        case (key, _) =>
          if (key.matches("^jdbc\\..+")) {
            throw new IllegalArgumentException(
              s"The parameter '$key' is not compatible with Data API credentials.")
          }
      }

      // Make sure the user isn't trying to authenticate in conflicting ways.
      // The user could also not be providing either of these in the case of using IAM.
      if (userParameters.contains(PARAM_DATA_API_USER) &&
        userParameters.contains(PARAM_SECRET_ID)) {
        throw new IllegalArgumentException(
          "You cannot specify both the 'data_api_user' and provide a secret at the same time.")
      }

      if (!userParameters.contains(PARAM_DATA_API_DATABASE)) {
        throw new IllegalArgumentException(
          "You must specify a Redshift database name with the 'data_api_database' parameter " +
            "to use the Redshift Data API")
      }

      if (userParameters.contains(PARAM_DATA_API_CLUSTER) &&
          userParameters.contains(PARAM_DATA_API_WORKGROUP)) {
        throw new IllegalArgumentException(
          "The parameters 'data_api_cluster' and 'data_api_workgroup' are mutually-exclusive.")
      }
    }

    // Validate the query group (if any)
    if (userParameters.get(PARAM_USER_QUERY_GROUP_LABEL).
      exists(_.exists(!_.isUnicodeIdentifierPart))) {
      val invalid = userParameters(PARAM_USER_QUERY_GROUP_LABEL).
        find(!_.isUnicodeIdentifierPart).get
      throw new IllegalArgumentException(
        "All characters in label option must be valid unicode identifier parts " +
          s"(char.isUnicodeIdentifierPart == true), '${invalid}' character not allowed"
      )
    }
  }

  /**
   * Merge user parameters with the defaults, preferring user parameters if specified
   */
  def mergeParameters(userParameters: Map[String, String]): MergedParameters = {
    // Make sure the user provided valid parameters.
    validateUserParameters(userParameters)

    // Merge all the parameters with our defaults.
    MergedParameters(DEFAULT_PARAMETERS ++ userParameters)
  }

  /**
   * Adds validators and accessors to string map
   */
  case class MergedParameters(parameters: Map[String, String]) {

    require(temporaryAWSCredentials.isDefined || iamRole.isDefined || forwardSparkS3Credentials,
      "You must specify a method for authenticating Redshift's connection to S3 (aws_iam_role," +
        " forward_spark_s3_credentials, or temporary_aws_*. For a discussion of the differences" +
        " between these options, please see the README.")

    require(Seq(
      temporaryAWSCredentials.isDefined,
      iamRole.isDefined,
      forwardSparkS3Credentials).count(_ == true) == 1,
      "The aws_iam_role, forward_spark_s3_credentials, and temporary_aws_*. options are " +
        "mutually-exclusive; please specify only one.")

    require(Seq(
      jdbcUrl.isDefined,
      dataApiCluster.isDefined,
      dataApiWorkgroup.isDefined).count(_ == true) == 1,
      "The url, data_api_cluster, and data_api_workgroup options are" +
        " mutually-exclusive; please specify only one.")

    /**
     * A root directory to be used for intermediate data exchange, expected to be on S3, or
     * somewhere that can be written to and read from by Redshift. Make sure that AWS credentials
     * are available for S3.
     */
    def rootTempDir: String = parameters("tempdir") // Default or Read Only
    def rootTempDirWriteOnly: Option[String] = parameters.get(PARAM_TEMPDIR_WRITE_ONLY)

    /**
     * AWS region where the 'tempdir' is located. Setting this option will improve connector
     * performance for interactions with 'tempdir' as well as automatically supply this region
     * as part of COPY and UNLOAD operations during connector writes and reads to Redshift.
     *
     * If the region is not specified, the connector will attempt to use the default S3 provider
     * chain for resolving where the 'tempdir' region is located. In some cases, such as when the
     * connector is being used outside of an AWS environment, this resolution will fail. Therefore,
     * this setting is highly recommended in the following situations:
     *  1) When the connector is running outside of AWS as automatic region discovery using the
     *     aws java sdk will fail and negatively affect connector performance.
     *  2) When 'tempdir' is in a different region than the Redshift cluster as using this
     *     setting alleviates the need to supply the region manually using the 'extracopyoptions'
     *     and 'extraunloadoptions' parameters.
     *  3) When the connector is running in a different region than 'tempdir' as it improves
     *     the connector's access performance of 'tempdir'.
     */
    def tempDirRegion: Option[String] = {
      val regionName = parameters.getOrElse(PARAM_TEMPDIR_REGION, "")
      if (regionName.isEmpty) None else Some(regionName)
    }

    /**
     * The format in which to save temporary files in S3. Defaults to "AVRO"; the other allowed
     * values are "CSV" and "CSV GZIP" for CSV and gzipped CSV, respectively.
     */
    def tempFormat: String = parameters("tempformat").toUpperCase

    /**
     * The String value to write for nulls when using CSV.
     * This should be a value which does not appear in your actual data.
     */
    def nullString: String = parameters("csvnullstring")

    /**
     * Creates a per-query subdirectory in the [[rootTempDir]], with a random UUID.
     * @param readOnly  Whether Spark will only read from the directory (and not write to it).
     */
    def createPerQueryTempDir(readOnly: Boolean): String = {
      val tempRoot = if (!readOnly && rootTempDirWriteOnly.isDefined) {
        rootTempDirWriteOnly.get
      } else {
        rootTempDir
      }
      Utils.makeTempPath(tempRoot)
    }

    /**
     * The Redshift table to be used as the target when loading or writing data.
     */
    def table: Option[TableName] = parameters.get("dbtable").map(_.trim).flatMap { dbtable =>
      // We technically allow queries to be passed using `dbtable` as long as they are wrapped
      // in parentheses. Valid SQL identifiers may contain parentheses but cannot begin with them,
      // so there is no ambiguity in ignoring subqeries here and leaving their handling up to
      // the `query` function defined below.
      if (dbtable.startsWith("(") && dbtable.endsWith(")")) {
        None
      } else {
        Some(TableName.parseFromEscaped(dbtable))
      }
    }

    /**
     * The Redshift query to be used as the target when loading data.
     */
    def query: Option[String] = parameters.get("query").orElse {
      parameters.get("dbtable")
        .map(_.trim)
        .filter(t => t.startsWith("(") && t.endsWith(")"))
        .map(t => t.drop(1).dropRight(1))
    }

    /**
    * User and password to be used to authenticate to Redshift
    */
    def credentials: Option[(String, String)] = {
      for (
        user <- parameters.get("user");
        password <- parameters.get("password")
      ) yield (user, password)
    }

    /**
    * Secret Id to be used to authenticate to Redshift.
    */
    def secretId: Option[String] = parameters.get(PARAM_SECRET_ID)

    /**
    * Secret Region is the region value where your secret resides.
    */
    def secretRegion: Option[String] = parameters.get(PARAM_SECRET_REGION)
    /**
     * A JDBC URL, of the format:
     *
     *    jdbc:subprotocol://host:port/database?user=username&password=password
     *
     * Where:
     *  - subprotocol can be postgresql or redshift, depending on which JDBC driver you have loaded.
     *    Note however that one Redshift-compatible driver must be on the classpath and match this
     *    URL.
     *  - host and port should point to the Redshift master node, so security groups and/or VPC will
     *    need to be configured to allow access from the Spark driver
     *  - database identifies a Redshift database name
     *  - user and password are credentials to access the database, which must be embedded in this
     *    URL for JDBC
     */
    def jdbcUrl: Option[String] = parameters.get("url")

    /**
     * The JDBC driver class name. This is used to make sure the driver is registered before
     * connecting over JDBC.
     */
    def jdbcDriver: Option[String] = parameters.get("jdbcdriver")

    /**
     * Set the Redshift table distribution style, which can be one of: EVEN, KEY or ALL. If you set
     * it to KEY, you'll also need to use the distkey parameter to set the distribution key.
     *
     * Default is EVEN.
     */
    def distStyle: Option[String] = parameters.get("diststyle")

    /**
     * The name of a column in the table to use as the distribution key when using DISTSTYLE KEY.
     * Not set by default, as default DISTSTYLE is EVEN.
     */
    def distKey: Option[String] = parameters.get("distkey")

    /**
     * A full Redshift SORTKEY specification. For full information, see latest Redshift docs:
     * http://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html
     *
     * Examples:
     *   SORTKEY (my_sort_column)
     *   COMPOUND SORTKEY (sort_col1, sort_col2)
     *   INTERLEAVED SORTKEY (sort_col1, sort_col2)
     *
     * Not set by default - table will be unsorted.
     *
     * Note: appending data to a table with a sort key only makes sense if you know that the data
     * being added will be after the data already in the table according to the sort order. Redshift
     * does not support random inserts according to sort order, so performance will degrade if you
     * try this.
     */
    def sortKeySpec: Option[String] = parameters.get("sortkeyspec")

    /**
     * DEPRECATED: see PR #157.
     *
     * When true, data is always loaded into a new temporary table when performing an overwrite.
     * This is to ensure that the whole load process succeeds before dropping any data from
     * Redshift, which can be useful if, in the event of failures, stale data is better than no data
     * for your systems.
     *
     * Defaults to true.
     */
    def useStagingTable: Boolean = parameters("usestagingtable").toBoolean

    /**
     * Extra options to append to the Redshift COPY command (e.g. "MAXERROR 100").
     */
    def extraCopyOptions: String = parameters.getOrElse("extracopyoptions", "")

    /**
     * Extra options to append to the Redshift UNLOAD command (e.g. ENCRYPTED).
     */
    def extraUnloadOptions: String = parameters.getOrElse("extraunloadoptions", "")

    /**
      * Description of the table, set using the SQL COMMENT command.
      */
    def description: Option[String] = parameters.get("description")

    /**
     * A user provided label to add to query group as value for key lbl
     */
    def user_query_group_label: String = parameters(PARAM_USER_QUERY_GROUP_LABEL)

    /**
      * List of semi-colon separated SQL statements to run before write operations.
      * This can be useful for running DELETE operations to clean up data
      *
      * If the action string contains %s, the table name will be substituted in, in case a staging
      * table is being used.
      *
      * Defaults to empty.
      */
    def preActions: Array[String] = parameters("preactions").trim.split(";")

    /**
      * List of semi-colon separated SQL statements to run after successful write operations.
      * This can be useful for running GRANT operations to make your new tables readable to other
      * users and groups.
      *
      * If the action string contains %s, the table name will be substituted in, in case a staging
      * table is being used.
      *
      * Defaults to empty.
     */
    def postActions: Array[String] = parameters("postactions").trim.split(";")

    /**
      * The IAM role that Redshift should assume for COPY/UNLOAD operations.
      */
    def iamRole: Option[String] = parameters.get("aws_iam_role")

    /**
     * If true then this library will automatically discover the credentials that Spark is
     * using to connect to S3 and will forward those credentials to Redshift over JDBC.
     */
    def forwardSparkS3Credentials: Boolean = parameters("forward_spark_s3_credentials").toBoolean

    /**
     * Temporary AWS credentials which are passed to Redshift. These only need to be supplied by
     * the user when Hadoop is configured to authenticate to S3 via IAM roles assigned to EC2
     * instances.
     */
    def temporaryAWSCredentials: Option[AWSCredentialsProvider] = {
      for (
        accessKey <- parameters.get(PARAM_TEMPORARY_AWS_ACCESS_KEY_ID);
        secretAccessKey <- parameters.get(PARAM_TEMPORARY_AWS_SECRET_ACCESS_KEY);
        sessionToken <- parameters.get(PARAM_TEMPORARY_AWS_SESSION_TOKEN)
      ) yield {
        AWSCredentialsUtils.staticCredentialsProvider(
          new BasicSessionCredentials(accessKey, secretAccessKey, sessionToken))
      }
    }

    /**
     * If true then this library will extract the column list from the schema to
     * include in the COPY command (e.g. `COPY "PUBLIC"."tablename" ("column1" [,"column2", ...])`)
     */
    def includeColumnList: Boolean = parameters("include_column_list").toBoolean

    /**
     * The AWS SSE-KMS key to use for encryption during UNLOAD operations
     * instead of AWS's default encryption
     */
    def sseKmsKey: Option[String] = parameters.get("sse_kms_key")

    def autoPushdown: Boolean = parameters(PARAM_AUTO_PUSHDOWN).toBoolean
    def pushdownS3ResultCache: Boolean = parameters(PARAM_PUSHDOWN_S3_RESULT_CACHE).toBoolean
    def unloadS3Format: String = parameters(PARAM_UNLOAD_S3_FORMAT).toUpperCase

    /**
     * Number of times to retry redshift copy
     * @return Int
     */
    def copyRetryCount : Int = parameters(PARAM_COPY_RETRY_COUNT).toInt

    /**
     * Milliseconds to sleep between copy retry attempts.
     * Negative and zero values are treated as 30s.
     * @return Long
     */
    def copyDelay : Long = {
      val configuredValue = parameters(PARAM_COPY_DELAY).toLong
      if (configuredValue > 0) configuredValue else DEFAULT_RETRY_DELAY
    }

    /**
     * Enables the use of doubles for real to support legacy applications
     */
    def legacyJdbcRealTypeMapping: Boolean =
      parameters(PARAM_LEGACY_JDBC_REAL_TYPE_MAPPING).toBoolean

    /**
     * Trims leading and trailing whitespace when using CSV-based formats (CSV and CSV GZIP) for
     * tempformat during writes. Added for backwards support with legacy applications.
     */
    def legacyTrimCSVWrites: Boolean =
      parameters(PARAM_LEGACY_TRIM_CSV_WRITES).toBoolean

    /**
     * Enables the use of Mapping Short to Integer to support legacy applications
     */
    def legacyMappingShortToInt: Boolean =
      parameters(PARAM_LEGACY_MAPPING_SHORT_TO_INT).toBoolean

    /**
     * Turns empty strings into nulls
     */
    def overrideNullable: Boolean =
      parameters(PARAM_OVERRIDE_NULLABLE).toBoolean


    /**
     * DataAPI credentials
     */
    def dataApiRegion: Option[String] = parameters.get(PARAM_DATA_API_REGION)
    def dataApiDatabase: Option[String] = parameters.get(PARAM_DATA_API_DATABASE)
    def dataApiUser: Option[String] = parameters.get(PARAM_DATA_API_USER)
    def dataApiCluster: Option[String] = parameters.get(PARAM_DATA_API_CLUSTER)
    def dataApiWorkgroup: Option[String] = parameters.get(PARAM_DATA_API_WORKGROUP)
    def dataApiRetryDelayMin: Double = parameters(PARAM_DATA_API_RETRY_DELAY_MIN).toDouble
    def dataApiRetryDelayMax: Double = parameters(PARAM_DATA_API_RETRY_DELAY_MAX).toDouble
    def dataApiRetryDelayMult: Double = parameters(PARAM_DATA_API_RETRY_DELAY_MULT).toDouble

    /*
     * Whether we are using DataAPI-based connections or not.
     */
    def dataAPICreds: Boolean = {
      dataApiRegion.isDefined || dataApiDatabase.isDefined || dataApiUser.isDefined ||
      dataApiCluster.isDefined || dataApiWorkgroup.isDefined
    }

    def skipDSWWorkaround: Boolean
      = parameters.getOrElse(PARAM_SKIP_DSW_WORKAROUND, "false").toBoolean

    /*
     * Gets a unique cluster name based upon the available parameters.
     */
    def uniqueClusterName: String = {
      if (jdbcUrl.isDefined) {
        "JDBC:" + jdbcUrl.get
      } else if (dataApiCluster.isDefined) {
        "DataAPI:Cluster:" + dataApiCluster.get
      } else if (dataApiWorkgroup.isDefined) {
        "DataAPI:Workgroup:" + dataApiWorkgroup.get
      } else {
        "" // Unspecified (i.e., Service-managed compute)
      }
    }
 
    /**
     * Delete support
     */
    def isDelete: Boolean = parameters(PARAM_IS_DELETE).toBoolean
  }
}
