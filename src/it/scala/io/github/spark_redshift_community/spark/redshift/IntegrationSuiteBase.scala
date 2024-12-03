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

package io.github.spark_redshift_community.spark.redshift

import io.github.spark_redshift_community.spark.redshift.Parameters._
import io.github.spark_redshift_community.spark.redshift.data.{RedshiftConnection, RedshiftWrapper, RedshiftWrapperFactory}

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.matchers.should._

import scala.collection.mutable
import scala.util.Random


/**
 * Base class for writing integration tests which run against a real Redshift cluster.
 */
trait IntegrationSuiteBase
  extends QueryTest
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  protected def loadConfigFromEnv(envVarName: String, isRequired:Boolean = true): String = {
    Option(System.getenv(envVarName)).getOrElse {
      if (isRequired)
        fail(s"Must set $envVarName environment variable")
      else
        null
    }
  }

  // The following configurations must be set in order to run these tests. In Travis, these
  // environment variables are set using Travis's encrypted environment variables feature:
  // http://docs.travis-ci.com/user/environment-variables/#Encrypted-Variables

  // JDBC URL listed in the AWS console (should not contain username and password).
  protected val AWS_REDSHIFT_JDBC_URL: String = loadConfigFromEnv("AWS_REDSHIFT_JDBC_URL")
  protected val AWS_REDSHIFT_USER: String = loadConfigFromEnv("AWS_REDSHIFT_USER")
  protected val AWS_REDSHIFT_PASSWORD: String = loadConfigFromEnv("AWS_REDSHIFT_PASSWORD")
  protected val AWS_ACCESS_KEY_ID: String = loadConfigFromEnv("AWS_ACCESS_KEY_ID")
  protected val AWS_SECRET_ACCESS_KEY: String = loadConfigFromEnv("AWS_SECRET_ACCESS_KEY")
  protected val AWS_SESSION_TOKEN: String = loadConfigFromEnv("AWS_SESSION_TOKEN", isRequired = false)
  // Path to a directory in S3 (e.g. 's3a://bucket-name/path/to/scratch/space').
  protected val AWS_S3_SCRATCH_SPACE: String = loadConfigFromEnv("AWS_S3_SCRATCH_SPACE")
  protected val AWS_S3_SCRATCH_SPACE_REGION: String = loadConfigFromEnv("AWS_S3_SCRATCH_SPACE_REGION")

  protected val AWS_DATA_API_REGION: String = loadConfigFromEnv("AWS_DATA_API_REGION", isRequired = false)
  protected val AWS_DATA_API_DATABASE: String = loadConfigFromEnv("AWS_DATA_API_DATABASE", isRequired = false)
  protected val AWS_DATA_API_USER: String = loadConfigFromEnv("AWS_DATA_API_USER", isRequired = false)
  protected val AWS_DATA_API_CLUSTER: String = loadConfigFromEnv("AWS_DATA_API_CLUSTER", isRequired = false)
  protected val AWS_DATA_API_WORKGROUP: String = loadConfigFromEnv("AWS_DATA_API_WORKGROUP", isRequired = false)
  protected val AWS_REDSHIFT_INTERFACE: String = loadConfigFromEnv("AWS_REDSHIFT_INTERFACE", isRequired = false)

  protected val DATA_API_INTERFACE = "DataAPI"
  protected val JDBC_INTERFACE = "JDBC"

  require(AWS_S3_SCRATCH_SPACE.contains("s3a"), "must use s3a:// URL")

  protected val auto_pushdown: String = "false"

  protected def jdbcUrl: String = {
    s"$AWS_REDSHIFT_JDBC_URL?user=$AWS_REDSHIFT_USER&password=$AWS_REDSHIFT_PASSWORD&ssl=true"
  }

  protected def jdbcUrlNoUserPassword: String = {
    s"$AWS_REDSHIFT_JDBC_URL?ssl=true"
  }
  /**
   * Random suffix appended appended to table and directory names in order to avoid collisions
   * between separate Travis builds.
   */
  protected val randomSuffix: String = Math.abs(Random.nextLong()).toString

  protected val tempDir: String = AWS_S3_SCRATCH_SPACE + randomSuffix + "/"

  /**
   * Spark Context with Hadoop file overridden to point at our local test data file for this suite,
   * no-matter what temp directory was generated and requested.
   */
  protected var sc: SparkContext = _
  protected var sqlContext: SQLContext = _
  protected var redshiftWrapper: RedshiftWrapper = _
  protected var conn: RedshiftConnection = _
  protected var sparkVersion: ComparableVersion = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = new SparkContext("local", "RedshiftSourceSuite")
    sparkVersion = ComparableVersion(sc.version)
    // Bypass Hadoop's FileSystem caching mechanism so that we don't cache the credentials:
    sc.hadoopConfiguration.setBoolean("fs.s3.impl.disable.cache", true)
    sc.hadoopConfiguration.setBoolean("fs.s3n.impl.disable.cache", true)
    sc.hadoopConfiguration.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    sc.hadoopConfiguration.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    if (AWS_SESSION_TOKEN != null) {
      sc.hadoopConfiguration.set("fs.s3a.session.token", AWS_SESSION_TOKEN)
      sc.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    } else {
      sc.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    }
    sc.hadoopConfiguration.set("fs.s3a.bucket.probe", "2")
    sc.hadoopConfiguration.setBoolean("fs.s3a.bucket.all.committer.magic.enabled", true)

    val params: Map[String, String] = defaultOptions() + ("dbtable" -> "fake_table")
    val mergedParams = Parameters.mergeParameters(params)
    redshiftWrapper = RedshiftWrapperFactory(mergedParams)
    conn = redshiftWrapper.getConnector(mergedParams)
  }

  override def afterAll(): Unit = {
    try {
      val conf = new Configuration(false)
      conf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
      conf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
      if (AWS_SESSION_TOKEN != null) {
        conf.set("fs.s3a.session.token", AWS_SESSION_TOKEN)
      }
      // Bypass Hadoop's FileSystem caching mechanism so that we don't cache the credentials:
      conf.setBoolean("fs.s3.impl.disable.cache", true)
      conf.setBoolean("fs.s3n.impl.disable.cache", true)
      conf.setBoolean("fs.s3a.impl.disable.cache", true)
      conf.set("fs.s3.impl", classOf[InMemoryS3AFileSystem].getCanonicalName)
      conf.set("fs.s3a.impl", classOf[InMemoryS3AFileSystem].getCanonicalName)
      val fs = FileSystem.get(URI.create(tempDir), conf)
      fs.delete(new Path(tempDir), true)
      fs.close()
    } finally {
      try {
        conn.close()
      } finally {
        try {
          sc.stop()
        } finally {
          super.afterAll()
        }
      }
    }
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    sqlContext = SparkSession.builder().enableHiveSupport().getOrCreate().sqlContext
  }

  protected def defaultOptions(): Map[String, String] = {
    val options: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()

    // Add the default options
    options += ("forward_spark_s3_credentials" -> "true")
    options += ("tempdir" -> tempDir)
    options += (PARAM_TEMPDIR_REGION -> AWS_S3_SCRATCH_SPACE_REGION)
    options += (PARAM_AUTO_PUSHDOWN -> auto_pushdown)

    // Add the Redshift interface-specific options
    if ((AWS_REDSHIFT_INTERFACE == null) || (AWS_REDSHIFT_INTERFACE == JDBC_INTERFACE)) {
      options += ("url" -> jdbcUrl)
    } else if (AWS_REDSHIFT_INTERFACE == DATA_API_INTERFACE) {
      if (AWS_DATA_API_REGION != null) {
        options += (PARAM_DATA_API_REGION -> AWS_DATA_API_REGION)
      }
      if (AWS_DATA_API_DATABASE != null) {
        options += (PARAM_DATA_API_DATABASE -> AWS_DATA_API_DATABASE)
      }
      if (AWS_DATA_API_USER != null) {
        options += (PARAM_DATA_API_USER -> AWS_DATA_API_USER)
      }
      if (AWS_DATA_API_CLUSTER != null) {
        options += (PARAM_DATA_API_CLUSTER -> AWS_DATA_API_CLUSTER)
      }
      if (AWS_DATA_API_WORKGROUP != null) {
        options += (PARAM_DATA_API_WORKGROUP -> AWS_DATA_API_WORKGROUP)
      }
    }
    else {
      throw new IllegalArgumentException(
        s"Unrecognized AWS_REDSHIFT_INTERFACE: $AWS_REDSHIFT_INTERFACE")
    }

    options.toMap
  }

  /**
   * Create a new DataFrameReader using common options for reading from Redshift.
   */
  protected def read: DataFrameReader = {
    sqlContext.read
      .format("io.github.spark_redshift_community.spark.redshift")
      .options(defaultOptions())
  }

  /**
   * Create a new DataFrameWriter using common options for writing to Redshift.
   */
  protected def write(df: DataFrame): DataFrameWriter[Row] = {
    df.write
      .format("io.github.spark_redshift_community.spark.redshift")
      .options(defaultOptions())
  }

  protected def createTestDataInRedshift(tableName: String): Unit = {
    redshiftWrapper.executeUpdate(conn,
      s"""
         |create table $tableName (
         |testbyte int2,
         |testbool boolean,
         |testdate date,
         |testdouble float8,
         |testfloat float4,
         |testint int4,
         |testlong int8,
         |testshort int2,
         |teststring varchar(256),
         |testtimestamp timestamp
         |)
      """.stripMargin
    )
    // scalastyle:off
    redshiftWrapper.executeUpdate(conn,
      s"""
         |insert into $tableName values
         |(null, null, null, null, null, null, null, null, null, null),
         |(0, null, '2015-07-03', 0.0, -1.0, 4141214, 1239012341823719, null, 'f', '2015-07-03 12:34:56.000'),
         |(0, false, null, -1234152.12312498, 100000.0, null, 1239012341823719, 24, '___|_123', null),
         |(1, false, '2015-07-02', 0.0, 0.0, 42, 1239012341823719, -13, 'asdf', '2015-07-02 00:00:00.000'),
         |(1, true, '2015-07-01', 1234152.12312498, 1.0, 42, 1239012341823719, 23, 'Unicode''s樂趣', '2015-07-01 00:00:00.001')
         """.stripMargin
    )
    // scalastyle:on
  }

  protected def withTempRedshiftTable[T](namePrefix: String)(body: String => T): T = {
    val tableName = s"$namePrefix$randomSuffix"
    try {
      body(tableName)
    } finally {
      redshiftWrapper.executeUpdate(conn, s"drop table if exists $tableName")
    }
  }

  protected def withTwoTempRedshiftTables[T](namePrefix1: String, namePrefix2: String)
                                           (body: (String, String) => T): T = {
    val tableName1 = s"$namePrefix1$randomSuffix"
    val tableName2 = s"$namePrefix2$randomSuffix"
    try {
      body(tableName1, tableName2)
    } finally {
      redshiftWrapper.executeUpdate(conn, s"drop table if exists $tableName1")
      redshiftWrapper.executeUpdate(conn, s"drop table if exists $tableName2")
    }
  }

  /**
   * Save the given DataFrame to Redshift, then load the results back into a DataFrame and check
   * that the returned DataFrame matches the one that we saved.
   *
   * @param tableName the table name to use
   * @param df the DataFrame to save
   * @param expectedSchemaAfterLoad if specified, the expected schema after loading the data back
   *                                from Redshift. This should be used in cases where you expect
   *                                the schema to differ due to reasons like case-sensitivity.
   * @param saveMode the [[SaveMode]] to use when writing data back to Redshift
   */
  def testRoundtripSaveAndLoad(
      tableName: String,
      df: DataFrame,
      expectedSchemaAfterLoad: Option[StructType] = None,
      saveMode: SaveMode = SaveMode.ErrorIfExists): Unit = {
    try {
      write(df)
        .option("dbtable", tableName)
        .mode(saveMode)
        .save()
      // Check that the table exists. It appears that creating a table in one connection then
      // immediately querying for existence from another connection may result in spurious "table
      // doesn't exist" errors; this caused the "save with all empty partitions" test to become
      // flaky (see #146). To work around this, add a small sleep and check again:
      if (!redshiftWrapper.tableExists(conn, tableName)) {
        Thread.sleep(1000)
        assert(redshiftWrapper.tableExists(conn, tableName))
      }
      val loadedDf = read.option("dbtable", tableName).load()
      assert(loadedDf.schema === expectedSchemaAfterLoad.getOrElse(df.schema))
      checkAnswer(loadedDf, df.collect())
    } finally {
      redshiftWrapper.executeUpdate(conn, s"drop table if exists $tableName")
    }
  }
}
