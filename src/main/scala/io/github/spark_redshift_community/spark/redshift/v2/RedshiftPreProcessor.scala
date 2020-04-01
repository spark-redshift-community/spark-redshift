/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.spark_redshift_community.spark.redshift.v2

import java.io.InputStreamReader
import java.net.URI

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import com.eclipsesource.json.Json
import io.github.spark_redshift_community.spark.redshift.{AWSCredentialsUtils, DefaultJDBCWrapper, FilterPushdown, JDBCWrapper, Utils}
import io.github.spark_redshift_community.spark.redshift.Parameters.MergedParameters
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._

class RedshiftPreProcessor(spark: SparkSession,
    schemaOpt: Option[StructType],
    requiredSchema: StructType,
    params: MergedParameters,
    pushedFilters: Array[Filter]) extends Logging {

  val jdbcWrapper: JDBCWrapper = DefaultJDBCWrapper

  private def buildUnloadStmt(
    requiredColumns: Array[String],
    filters: Array[Filter],
    creds: AWSCredentialsProvider): (String, String) = {
    assert(!requiredColumns.isEmpty)
    val tempDir = params.createPerQueryTempDir()
    val whereClause = FilterPushdown.buildWhereClause(schemaOpt.get, filters)
    val tableNameOrSubquery = params.getTableNameOrSubquery
    // Always quote column names:
    val columnList = requiredColumns.map(col => s""""$col"""").mkString(", ")
    val credsString: String =
      AWSCredentialsUtils.getRedshiftCredentialsString(params, creds.getCredentials)
    val query = {
      // Since the query passed to UNLOAD will be enclosed in single quotes, we need to escape
      // any backslashes and single quotes that appear in the query itself
      val escapedTableNameOrSubqury = tableNameOrSubquery.replace("\\", "\\\\").replace("'", "\\'")
      s"SELECT $columnList FROM $escapedTableNameOrSubqury $whereClause"
    }
    // We need to remove S3 credentials from the unload path URI because they will conflict with
    // the credentials passed via `credsString`.
    val fixedUrl = Utils.fixS3Url(Utils.removeCredentialsFromURI(new URI(tempDir)).toString)

    val sql = if (params.getUnloadFormat == "csv") {
      s"""
          |UNLOAD ('$query') TO '$fixedUrl'
          |WITH CREDENTIALS '$credsString'
          |MANIFEST
          |ESCAPE
          |""".stripMargin

    } else {
      s"""
          |UNLOAD ('$query') TO '$fixedUrl'
          |WITH CREDENTIALS '$credsString'
          |FORMAT AS PARQUET
          |MANIFEST
          |""".stripMargin
    }
    (sql, tempDir)
  }

  def unloadDataToS3(): Seq[String] = {
    val conf = SparkSession.getActiveSession.get.sparkContext.hadoopConfiguration
    val creds = AWSCredentialsUtils.load(params, conf)
    val s3ClientFactory: AWSCredentialsProvider => AmazonS3Client =
      awsCredentials => new AmazonS3Client(awsCredentials)
    for (
      redshiftRegion <- Utils.getRegionForRedshiftCluster(params.jdbcUrl);
      s3Region <- Utils.getRegionForS3Bucket(params.rootTempDir, s3ClientFactory(creds))
    ) {
      if (redshiftRegion != s3Region) {
        // We don't currently support `extraunloadoptions`, so even if Amazon _did_ add a `region`
        // option for this we wouldn't be able to pass in the new option. However, we choose to
        // err on the side of caution and don't throw an exception because we don't want to break
        // existing workloads in case the region detection logic is wrong.
        logError("The Redshift cluster and S3 bucket are in different regions " +
          s"($redshiftRegion and $s3Region, respectively). Redshift's UNLOAD command requires " +
          s"that the Redshift cluster and Amazon S3 bucket be located in the same region, so " +
          s"this read will fail.")
      }
    }
    Utils.checkThatBucketHasObjectLifecycleConfiguration(params.rootTempDir, s3ClientFactory(creds))
    if (schemaOpt.nonEmpty) {
      // Unload data from Redshift into a temporary directory in S3:
      val schema = schemaOpt.get
      val prunedSchema = pruneSchema(schema, requiredSchema.map(_.name))
      val (unloadSql, tempDir) = buildUnloadStmt(prunedSchema,
        Array.empty, creds)
      val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, params.credentials)
      try {
        jdbcWrapper.executeInterruptibly(conn.prepareStatement(unloadSql))
      } finally {
        conn.close()
      }
      // Read the MANIFEST file to get the list of S3 part files that were written by Redshift.
      // We need to use a manifest in order to guard against S3's eventually-consistent listings.
      val filesToRead: Seq[String] = {
        val cleanedTempDirUri =
          Utils.fixS3Url(Utils.removeCredentialsFromURI(URI.create(tempDir)).toString)
        val s3URI = Utils.createS3URI(cleanedTempDirUri)
        val s3Client = s3ClientFactory(creds)
        val is = s3Client.getObject(s3URI.getBucket, s3URI.getKey + "manifest").getObjectContent
        val s3Files = try {
          val entries = Json.parse(new InputStreamReader(is)).asObject().get("entries").asArray()
          entries.iterator().asScala.map(_.asObject().get("url").asString()).toSeq
        } finally {
          is.close()
        }
        // The filenames in the manifest are of the form s3://bucket/key, without credentials.
        // If the S3 credentials were originally specified in the tempdir's URI, then we need to
        // reintroduce them here
        s3Files.map { file =>
          tempDir.stripSuffix("/") + '/' + file.stripPrefix(cleanedTempDirUri).stripPrefix("/")
        }
      }

      // val prunedSchema = pruneSchema(schema, schema.map(_.name).toArray)
      filesToRead
    } else {
      {
        // In the special case where no columns were requested, issue a `count(*)` against Redshift
        // rather than unloading data.
        // Fixme: add where clause

        val countQuery = s"SELECT count(*) FROM ${params.getTableNameOrSubquery}"
        log.info(countQuery)
        val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, params.credentials)
        try {
          val results = jdbcWrapper.executeQueryInterruptibly(conn.prepareStatement(countQuery))
          if (results.next()) {
            val numRows = results.getLong(1)
            val parallelism = spark.conf.get("spark.sql.shuffle.partitions", "200").toInt
            val emptyRow = RowEncoder(StructType(Seq.empty)).toRow(Row(Seq.empty))
            spark.sparkContext
              .parallelize(1L to numRows, parallelism)
              .map(_ => emptyRow)
              .asInstanceOf[RDD[Row]]
          } else {
            throw new IllegalStateException("Could not read count from Redshift")
          }
          // FIXME
          Seq.empty[String]
        } finally {
          conn.close()
        }
      }
    }
  }

  private def pruneSchema(schema: StructType, columns: Seq[String]): Array[String] = {
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    // FIXME handle exceptions
    columns.map(name => fieldMap(name).name).toArray
  }

  def process(): Seq[String] = {
    unloadDataToS3()
  }
}
