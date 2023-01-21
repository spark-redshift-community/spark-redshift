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
import java.time.LocalDateTime

import scala.collection.JavaConverters._

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.eclipsesource.json.Json
import io.github.spark_redshift_community.spark.redshift.Parameters.MergedParameters
import io.github.spark_redshift_community.spark.redshift.{AWSCredentialsUtils, DefaultJDBCWrapper, FilterPushdown, JDBCWrapper, Utils}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SaveMode, SparkSession}



class RedshiftPreProcessor(schemaOpt: Option[StructType],
    requiredSchema: StructType,
    params: MergedParameters,
    pushedFilters: Array[Filter]) extends Logging {

  val jdbcWrapper: JDBCWrapper = DefaultJDBCWrapper
  val CACHE_MANIFEST = "last_unload"

  private def buildUnloadStmt(
    requiredColumns: Array[String],
    filters: Array[Filter],
    creds: AWSCredentialsProvider): (String, String, LocalDateTime) = {
    assert(schemaOpt.isDefined)
    val whereClause = FilterPushdown.buildWhereClause(schemaOpt.get, filters)
    val tableNameOrSubquery = params.getTableNameOrSubquery

    val query = {
      val columnList = requiredColumns.map(col => s""""$col"""").mkString(", ")
      // Since the query passed to UNLOAD will be enclosed in single quotes, we need to escape
      // any backslashes and single quotes that appear in the query itself
      val escapedTableNameOrSubquery = tableNameOrSubquery.replace("\\", "\\\\").replace("'", "\\'")
      s"SELECT $columnList FROM $escapedTableNameOrSubquery $whereClause"
    }
    val timestamp = java.time.LocalDateTime.now
    val tempDir = params.createPerTableTempDir(tableNameOrSubquery, query, timestamp)
    
    val credsString: String =
      AWSCredentialsUtils.getRedshiftCredentialsString(params, creds.getCredentials)
    // We need to remove S3 credentials from the unload path URI because they will conflict with
    // the credentials passed via `credsString`.
    val fixedUrl = Utils.fixS3Url(Utils.removeCredentialsFromURI(new URI(tempDir)).toString)

    val sql = if (params.getUnloadFormat == "csv") {
      s"""
         |UNLOAD ('$query') TO '$fixedUrl'
         |WITH CREDENTIALS '$credsString'
         |MANIFEST
         |ESCAPE
         |NULL AS '${params.nullString}'
         |""".stripMargin

    } else {
      s"""
         |UNLOAD ('$query') TO '$fixedUrl'
         |WITH CREDENTIALS '$credsString'
         |FORMAT AS PARQUET
         |MANIFEST
         |""".stripMargin
    }

    (sql, tempDir, timestamp)
  }
  
  private def writeEmptyParquetAndManifest(s3Client: AmazonS3Client, s3Bucket: String,
    s3Key: String, spark: SparkSession, requiredSchema: StructType): Unit = {
    logWarning(s"Writing empty parquet file, because no rows matched in redshift")
    val emptyDf = spark.createDataFrame(spark.emptyDataFrame.rdd, requiredSchema)
    val targetS3Path = s"s3://$s3Bucket/$s3Key/"
    emptyDf.write.mode(SaveMode.Overwrite).parquet(targetS3Path)
    // get the unique parquet file url
    val parquetUrl = spark.read.parquet(targetS3Path).inputFiles(0)
    val manifestContent =
      s"""
      |{
      |  "entries": [
      |    {"url":"$parquetUrl", "meta": { "content_length": 0 }}
      |  ]
      |}
      |""".stripMargin
    s3Client.putObject(s3Bucket, s3Key + "manifest", manifestContent)
  }
  
  /**
   * Look for a cache manifest in a given query path, if the timestamp
   * matches the TTL then return the contained path otherwise, the current one
   * @param currentCacheDir
   * @param tableMinutesTTL
   * @return
   */
  private def considerCache(currentCacheDir: String, tableMinutesTTL: Int): String = {
    val fs = FileSystem.get(URI.create(currentCacheDir),
      SparkSession.active.sparkContext.hadoopConfiguration)
    val lastUnloadPath = new Path(
      currentCacheDir.replaceAll("[^/]+/?$", ""), CACHE_MANIFEST)
      if(fs.exists(lastUnloadPath)){
        val lastCacheDir = fs.open(lastUnloadPath).readUTF().split("\n")
        val creationTime = LocalDateTime.parse(lastCacheDir(0))
        if (LocalDateTime.now().minusSeconds(tableMinutesTTL * 60).isBefore(creationTime)) {
          logWarning(s"Reuse cached unloaded: ${lastCacheDir(1)}")
          return lastCacheDir(1)
        }
      }
     currentCacheDir
  }
  
  /**
   * Upload a file with the path of the current cache path
   * it contains both timestamp and currentCacheDir separated by a newline
   * @param currentCacheDir
   */
  private def createFileManifest(currentCacheDir: String, timestamp: LocalDateTime) = {
    val fs = FileSystem.get(URI.create(currentCacheDir), SparkSession.active.sparkContext.hadoopConfiguration)
    val lastUnload = fs.create(new Path(currentCacheDir.replaceAll("[^/]+/?$", ""),
      CACHE_MANIFEST), true)
    val manifestContent =
      s"""${timestamp.toString}
         |$currentCacheDir""".stripMargin
    lastUnload.writeUTF(manifestContent)
    lastUnload.close()
  }
  
  def unloadDataToS3(): Seq[String] = {
    assert(SparkSession.getActiveSession.isDefined, "SparkSession not initialized")
    val conf = SparkSession.getActiveSession.get.sparkContext.hadoopConfiguration
    val creds = AWSCredentialsUtils.load(params, conf)
    val s3ClientFactory: AWSCredentialsProvider => AmazonS3Client =
      awsCredentials => new AmazonS3Client(awsCredentials)
    val s3Client = s3ClientFactory(creds)
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
      val (unloadSql, tempDir, timestamp) = buildUnloadStmt(prunedSchema,
        pushedFilters, creds)
      val candidateTempDir = if (params.tableMinutesTTL > 0) {
        considerCache(tempDir, params.tableMinutesTTL)
      } else tempDir
      if (candidateTempDir == tempDir){ // when cache has no candidate dataset then unload
        val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, params.credentials)
        try {
          jdbcWrapper.executeInterruptibly(conn.prepareStatement(unloadSql))
        } catch {
          case e: Exception =>
              logInfo("Error occurred when unloading data", e)
        } finally {
          conn.close()
        }
       createFileManifest(tempDir, timestamp)
      }
      // Read the MANIFEST file to get the list of S3 part files that were written by Redshift.
      // We need to use a manifest in order to guard against S3's eventually-consistent listings.
      val filesToRead: Seq[String] = {
        val cleanedTempDirUri =
          Utils.fixS3Url(Utils.removeCredentialsFromURI(URI.create(candidateTempDir)).toString)
        val s3URI = Utils.createS3URI(cleanedTempDirUri)
        // In parquet file mode, empty results in nothing on s3.
        // As a workaround we write en empty parquet file and get its file listing
        if(params.getUnloadFormat.equals("parquet")) {
          try {
            s3Client.getObject(s3URI.getBucket, s3URI.getKey + "manifest").getObjectContent
          } catch {
            case _: AmazonS3Exception => writeEmptyParquetAndManifest(
              s3Client,
              s3URI.getBucket, s3URI.getKey,
              SparkSession.getActiveSession.get,
              this.requiredSchema)
          }
        }
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
          candidateTempDir.stripSuffix("/") + '/' + file.stripPrefix(cleanedTempDirUri).stripPrefix("/")
        }
      }

      filesToRead
    } else {
      Seq.empty[String]
    }
  }

  private def pruneSchema(schema: StructType, columns: Seq[String]): Array[String] = {
    if (columns.isEmpty) {
      Array(schema.head.name)
    } else {
      val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
      columns.map(name => fieldMap(name).name).toArray
    }
  }

  def process(): Seq[String] = {
    unloadDataToS3()
  }
}
