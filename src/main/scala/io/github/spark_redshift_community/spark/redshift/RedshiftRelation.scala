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
import com.amazonaws.services.s3.AmazonS3Client
import com.eclipsesource.json.Json
import io.github.spark_redshift_community.spark.redshift.Conversions.parquetDataTypeConvert
import io.github.spark_redshift_community.spark.redshift.DefaultJDBCWrapper.DataBaseOperations
import io.github.spark_redshift_community.spark.redshift.Parameters.MergedParameters
import io.github.spark_redshift_community.spark.redshift.Utils.checkRedshiftAndS3OnSameRegion
import io.github.spark_redshift_community.spark.redshift.pushdown.{RedshiftSQLStatement, SqlToS3TempCache}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.slf4j.LoggerFactory

import java.io.InputStreamReader
import java.net.URI
import java.sql.Connection
import scala.collection.JavaConverters._

/**
 * Data Source API implementation for Amazon Redshift database tables
 */
private[redshift] case class RedshiftRelation(
    jdbcWrapper: JDBCWrapper,
    s3ClientFactory: AWSCredentialsProvider => AmazonS3Client,
    params: MergedParameters,
    userSchema: Option[StructType])
    (@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedFilteredScan
  with InsertableRelation {

  private val log = LoggerFactory.getLogger(getClass)

  if (sqlContext != null) {
    Utils.assertThatFileSystemIsNotS3BlockFileSystem(
      new URI(params.rootTempDir), sqlContext.sparkContext.hadoopConfiguration)
  }

  private val tableNameOrSubquery =
    params.query.map(q => s"($q)").orElse(params.table.map(_.toString)).get

  override lazy val schema: StructType = {
    userSchema.getOrElse {
      val tableNameOrSubquery =
        params.query.map(q => s"($q)").orElse(params.table.map(_.toString)).get
      val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, params.credentials)
      try {
        jdbcWrapper.resolveTable(conn, tableNameOrSubquery)
      } finally {
        conn.close()
      }
    }
  }

  override def toString: String = s"RedshiftRelation($tableNameOrSubquery)"

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val saveMode = if (overwrite) {
      SaveMode.Overwrite
    } else {
      SaveMode.Append
    }
    val writer = new RedshiftWriter(jdbcWrapper, s3ClientFactory)
    writer.saveToRedshift(sqlContext, data, saveMode, params)
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filterNot(filter => FilterPushdown.buildFilterExpression(schema, filter).isDefined)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val creds = AWSCredentialsUtils.load(params, sqlContext.sparkContext.hadoopConfiguration)
    checkRedshiftAndS3OnSameRegion(params.jdbcUrl, params.rootTempDir, s3ClientFactory(creds))
    Utils.checkThatBucketHasObjectLifecycleConfiguration(params.rootTempDir, s3ClientFactory(creds))

    if (requiredColumns.isEmpty) {
      // In the special case where no columns were requested, issue a `count(*)` against Redshift
      // rather than unloading data.
      val whereClause = FilterPushdown.buildWhereClause(schema, filters)
      val countQuery = s"SELECT count(*) FROM $tableNameOrSubquery $whereClause"
      log.debug(s"""buildScan: ${countQuery}""")
      val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, params.credentials)
      try {
        val results = jdbcWrapper.executeQueryInterruptibly(conn.prepareStatement(countQuery))
        if (results.next()) {
          val numRows = results.getLong(1)
          val parallelism = sqlContext.getConf("spark.sql.shuffle.partitions", "200").toInt
          val emptyRow = RowEncoder(StructType(Seq.empty)).createSerializer().apply(Row(Seq.empty))
          sqlContext.sparkContext
            .parallelize(1L to numRows, parallelism)
            .map(_ => emptyRow)
            .asInstanceOf[RDD[Row]]
        } else {
          throw new IllegalStateException("Could not read count from Redshift")
        }
      } finally {
        conn.close()
      }
    } else {
      // Unload data from Redshift into a temporary directory in S3:
      val tempDir = params.createPerQueryTempDir()
      val unloadSql = buildUnloadStmt(requiredColumns, filters, tempDir, creds, params.sseKmsKey)
      val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, params.credentials)
      try {
        jdbcWrapper.executeInterruptibly(conn.prepareStatement(unloadSql))
      } finally {
        conn.close()
      }

      val filesToRead: Seq[String] = getFilesToRead(creds, tempDir)

      val prunedSchema = pruneSchema(schema, requiredColumns)

      readRDD(prunedSchema, filesToRead)
    }
  }

  override def needConversion: Boolean = false

  private def buildUnloadStmt(
      requiredColumns: Array[String],
      filters: Array[Filter],
      tempDir: String,
      creds: AWSCredentialsProvider,
      sseKmsKey: Option[String]): String = {
    assert(!requiredColumns.isEmpty)
    // Always quote column names:
    val columnList = requiredColumns.map(col => s""""$col"""").mkString(", ")
    val whereClause = FilterPushdown.buildWhereClause(schema, filters)
    val credsString: String =
      AWSCredentialsUtils.getRedshiftCredentialsString(params, creds.getCredentials)
    val query = {
      // Since the query passed to UNLOAD will be enclosed in single quotes, we need to escape
      // any backslashes and single quotes that appear in the query itself
      val escapedTableNameOrSubqury = tableNameOrSubquery.replace("\\", "\\\\").replace("'", "\\'")
      s"SELECT $columnList FROM $escapedTableNameOrSubqury $whereClause"
    }

    // Save the last query so it can be inspected
    Utils.lastBuildStmt = query

    log.debug(s"""buildUnloadStmt: ${query}""")


    // We need to remove S3 credentials from the unload path URI because they will conflict with
    // the credentials passed via `credsString`.
    val fixedUrl = Utils.fixS3Url(Utils.removeCredentialsFromURI(new URI(tempDir)).toString)

    val sseKmsClause = sseKmsKey.map(key => s"KMS_KEY_ID '$key' ENCRYPTED").getOrElse("")
    val unloadStmt = s"UNLOAD ('$query') TO '$fixedUrl' WITH CREDENTIALS '$credsString'" +
      s" ESCAPE MANIFEST NULL AS '${params.nullString}'" +
      s" $sseKmsClause"

    unloadStmt
  }

  private def buildUnloadStmt(
                               statement: RedshiftSQLStatement,
                               schema: StructType,
                               tempDir: String,
                               creds: AWSCredentialsProvider,
                               sseKmsKey: Option[String]): String = {
    assert(schema.nonEmpty)

    val credsString: String =
      AWSCredentialsUtils.getRedshiftCredentialsString(params, creds.getCredentials)

    // Since the query passed to UNLOAD will be enclosed in single quotes, we need to escape
    // any backslashes and single quotes that appear in the query itself
    val query = statement.statementString.replace("\\", "\\\\").replace("'", "\\'")

    // Save the last query so it can be inspected
    Utils.lastBuildStmt = query

    log.debug(s"""buildUnloadStmt with RedshiftSQLStatement: ${query}""")


    // We need to remove S3 credentials from the unload path URI because they will conflict with
    // the credentials passed via `credsString`.
    val fixedUrl = Utils.fixS3Url(Utils.removeCredentialsFromURI(new URI(tempDir)).toString)

    val sseKmsClause = sseKmsKey.map(key => s"KMS_KEY_ID '$key' ENCRYPTED").getOrElse("")
    val unloadStmt = if (params.pushdownUnloadS3Format == "PARQUET") {
      s"UNLOAD ('SELECT * FROM ($query)') TO '$fixedUrl'" +
      s" WITH CREDENTIALS '$credsString'" +
      s" FORMAT AS PARQUET  MANIFEST" +
      s" $sseKmsClause"
    } else {
      s"UNLOAD ('SELECT * FROM ($query)') TO '$fixedUrl'" +
      s" WITH CREDENTIALS '$credsString'" +
      s" ESCAPE MANIFEST NULL AS '${params.nullString}'" +
      s" $sseKmsClause"
    }

    unloadStmt
  }

  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }

// Build the RDD from a query string, generated by RedshiftStrategy.
// Type can be InternalRow to comply with SparkPlan's doExecute().
def buildScanFromSQL[Row](statement: RedshiftSQLStatement,
                                  schema: Option[StructType]): RDD[Row] = {
    log.debug("buildScanFromSQL:" + statement.statementString)

    val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, params.credentials)
    val resultSchema: StructType = getResultSchema(statement, schema, conn)

    val creds = AWSCredentialsUtils.load(params, sqlContext.sparkContext.hadoopConfiguration)
    checkRedshiftAndS3OnSameRegion(params.jdbcUrl, params.rootTempDir, s3ClientFactory(creds))
    Utils.checkThatBucketHasObjectLifecycleConfiguration(params.rootTempDir, s3ClientFactory(creds))

    // If the same query was run before, get the result s3 path from the cache.
    val tempDir = if (params.pushdownS3ResultCache) {
      SqlToS3TempCache
        .getS3Path(statement.statementString)
        .orElse {
          UnloadDataToS3(statement, conn, resultSchema, creds)
        }
    } else {
      UnloadDataToS3(statement, conn, resultSchema, creds)
    }

    val filesToRead: Seq[String] = getFilesToRead(creds, tempDir.get)
  if (params.pushdownUnloadS3Format == "PARQUET") {
    readRDDFromParquet(resultSchema, filesToRead)
  } else {
    readRDD(resultSchema, filesToRead)
  }
  }

  // Unload data from Redshift into a temporary directory in S3
  private def UnloadDataToS3[Row](statement: RedshiftSQLStatement,
                                  conn: Connection,
                                  resultSchema: StructType,
                                  creds: AWSCredentialsProvider): Option[String] = {

    val newTempDir = params.createPerQueryTempDir()
    val unloadSql = buildUnloadStmt(statement,
                                    resultSchema,
                                    newTempDir,
                                    creds,
                                    params.sseKmsKey)

    try {
      jdbcWrapper.executeInterruptibly(conn.prepareStatement(unloadSql))
      SqlToS3TempCache.setS3Path(statement.statementString, newTempDir)
    } finally {
      conn.close()
    }
    Some(newTempDir)
  }

  private def readRDD[T](resultSchema: StructType, filesToRead: Seq[String]): RDD[T] = {
    sqlContext.read
      .format(classOf[RedshiftFileFormat].getName)
      .schema(resultSchema)
      .option("nullString", params.nullString)
      .load(filesToRead: _*)
      .queryExecution.executedPlan.execute().asInstanceOf[RDD[T]]
  }

  private def readRDDFromParquet[T](resultSchema: StructType, filesToRead: Seq[String]): RDD[T] = {

    val reader = sqlContext.read
      .format("parquet")

    if(filesToRead.isEmpty) reader.schema(resultSchema)

    reader.load(filesToRead: _*)
    .rdd
    .map{row =>
      val typeConvertedRow = row.toSeq.zipWithIndex.map {
        case (f, i) =>
          parquetDataTypeConvert(f, resultSchema.fields(i).dataType)
      }
      InternalRow(typeConvertedRow: _*)
    }
    .asInstanceOf[RDD[T]]
  }

  // Read the MANIFEST file to get the list of S3 part files that were written by Redshift.
  // We need to use a manifest in order to guard against S3's eventually-consistent listings.
  private def getFilesToRead[Row](creds: AWSCredentialsProvider, tempDir: String) = {

    val cleanedTempDirUri =
      Utils.fixS3Url(Utils.removeCredentialsFromURI(URI.create(tempDir)).toString)
    val s3URI = Utils.createS3URI(cleanedTempDirUri)
    val s3Client = s3ClientFactory(creds)

    if(s3Client.doesObjectExist(s3URI.getBucket, s3URI.getKey + "manifest")) {
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
    else {
      // If manifest doesn't exist, likely it is because the resultset is empty.
      // For empty resultset, Redshift sometimes doesn't create any files. So return empty Seq.
      // An unlikely scenario is the result files are removed from S3.
      log.debug(s"${s3URI}/manifest not found")
      Seq.empty[String]
    }

  }

  private def getResultSchema[Row](statement: RedshiftSQLStatement,
                                   schema: Option[StructType],
                                   conn: Connection) = {
    val resultSchema = schema.getOrElse(
      try {
        conn.tableSchema(statement, params)
      } finally {
        conn.close()
      })

    if (resultSchema.isEmpty) {
      throw new Exception("resultSchema isEmpty for " + statement.statementString)
    }
    resultSchema
  }
}