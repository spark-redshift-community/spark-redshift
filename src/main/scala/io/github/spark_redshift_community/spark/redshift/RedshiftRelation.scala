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
import com.amazonaws.services.s3.AmazonS3
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.spark_redshift_community.spark.redshift
import io.github.spark_redshift_community.spark.redshift.Conversions.parquetDataTypeConvert
import io.github.spark_redshift_community.spark.redshift.Parameters.{MergedParameters, PARAM_OVERRIDE_NULLABLE}
import io.github.spark_redshift_community.spark.redshift.RedshiftRelation.schemaTypesMatch
import io.github.spark_redshift_community.spark.redshift.data.{RedshiftConnection, RedshiftWrapper}
import io.github.spark_redshift_community.spark.redshift.pushdown.{RedshiftSQLStatement, SqlToS3TempCache}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext, SaveMode}

import java.io.InputStreamReader
import java.net.URI
import scala.collection.JavaConverters._

/**
 * Data Source API implementation for Amazon Redshift database tables
 */
case class RedshiftRelation(
    redshiftWrapper: RedshiftWrapper,
    s3ClientFactory: (AWSCredentialsProvider, MergedParameters) => AmazonS3,
    params: MergedParameters,
    userSchema: Option[StructType])
    (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation
    with Logging {

  if (params.checkS3BucketUsage && (sqlContext != null)) {
    Utils.assertThatFileSystemIsNotS3BlockFileSystem(
      new URI(params.rootTempDir), sqlContext.sparkContext.hadoopConfiguration)
  }

  private val tableNameOrSubquery =
    params.query.map(q => s"($q)").orElse(params.table.map(_.toString)).get

  override lazy val schema: StructType = {
    userSchema.getOrElse {
      val tableNameOrSubquery =
        params.query.map(q => s"($q)").orElse(params.table.map(_.toString)).get

      val conn = redshiftWrapper.getConnector(params)
      try {
        redshiftWrapper.resolveTable(conn, tableNameOrSubquery, Some(params))
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
    val writer = new RedshiftWriter(redshiftWrapper, s3ClientFactory)
    writer.saveToRedshift(sqlContext, data, saveMode, params)
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filterNot(filter => FilterPushdown.buildFilterExpression(schema, filter).isDefined)
  }

  private def checkS3BucketUsage(params: MergedParameters,
                                 credsProvider: AWSCredentialsProvider): Unit = {

    if (!params.checkS3BucketUsage) return

    val s3Client: AmazonS3 = s3ClientFactory(credsProvider, params)

    // Make sure a cross-region read has the necessary connector parameters set.
    Utils.checkRedshiftAndS3OnSameRegion(params, s3Client)

    // Make sure that the bucket has a lifecycle configuration set to automatically clean-up.
    Utils.checkThatBucketHasObjectLifecycleConfiguration(params, s3Client)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    val credsProvider = AWSCredentialsUtils.load(
      params, sqlContext.sparkContext.hadoopConfiguration)

    checkS3BucketUsage(params, credsProvider)

    Utils.collectMetrics(params)
    val queryGroup = Utils.queryGroupInfo(Utils.Read, params, sqlContext)

    if (requiredColumns.isEmpty) {
      // In the special case where no columns were requested, issue a `count(*)` against Redshift
      // rather than unloading data.
      val whereClause = FilterPushdown.buildWhereClause(schema, filters)
      val countQuery = s"SELECT count(*) FROM $tableNameOrSubquery $whereClause"
      val conn = redshiftWrapper.getConnectorWithQueryGroup(params, queryGroup)
      try {
        log.info("Getting number of rows from Redshift")
        val results = redshiftWrapper.executeQueryInterruptibly(conn, countQuery)
        if (results.next()) {
          val numRows = results.getLong(1)
          log.info("Number of rows is {}", numRows)
          val parallelism = sqlContext.getConf("spark.sql.shuffle.partitions", "200").toInt
          val emptyRow = RowEncoderUtils.expressionEncoderForSchema(StructType(Seq.empty)).
            createSerializer().apply(Row(Seq.empty))
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
      val unloadSql = buildUnloadStmt(
        requiredColumns, filters, tempDir, credsProvider, params.sseKmsKey)
      val conn = redshiftWrapper.getConnectorWithQueryGroup(params, queryGroup)
      try {
        log.info("Unloading data from Redshift")
        redshiftWrapper.executeInterruptibly(conn, unloadSql)
      } finally {
        conn.close()
      }

      val filesToRead: Seq[String] = getFilesToRead(tempDir, sqlContext.sparkContext)

      val prunedSchema = pruneSchema(schema, requiredColumns)

      if (params.unloadS3Format == "PARQUET") {
        readRDDFromParquet(prunedSchema, filesToRead)
      } else {
        readRDD(prunedSchema, filesToRead)
      }
    }
  }

  override def needConversion: Boolean = false

  private def buildUnloadStmt(
                               requiredColumns: Array[String],
                               filters: Array[Filter],
                               tempDir: String,
                               credsProvider: AWSCredentialsProvider,
                               sseKmsKey: Option[String]): String = {
    assert(!requiredColumns.isEmpty)
    // Always quote column names:
    val columnList = requiredColumns.map(col => s""""$col"""").mkString(", ")
    val whereClause = FilterPushdown.buildWhereClause(schema, filters, escapeQuote = true)
    val credsString: String =
      AWSCredentialsUtils.getRedshiftCredentialsString(params, credsProvider.getCredentials)
    val query = {
      // Since the query passed to UNLOAD will be enclosed in single quotes, we need to escape
      // any backslashes and single quotes that appear in the query itself
      val escapedTableNameOrSubqury = tableNameOrSubquery.replace("\\", "\\\\").replace("'", "\\'")
      s"SELECT $columnList FROM $escapedTableNameOrSubqury $whereClause"
    }

    // Save the last query so it can be inspected
    Utils.lastBuildStmt(Thread.currentThread.getName) = query

    // We need to remove S3 credentials from the unload path URI because they will conflict with
    // the credentials passed via `credsString`.
    val fixedUrl = Utils.fixS3Url(Utils.removeCredentialsFromURI(new URI(tempDir)).toString)

    // Build up the unload statement
    val unloadClause = s"UNLOAD ('$query') TO '$fixedUrl'"
    val credClause = s"WITH CREDENTIALS '$credsString'"
    val manifestClause = if (params.unloadS3Format == "PARQUET") {
      s"FORMAT AS PARQUET  MANIFEST"
    } else {
      s"ESCAPE MANIFEST NULL AS '${params.nullString}'"
    }
    val sseKmsClause = sseKmsKey.map(key => s"KMS_KEY_ID '$key' ENCRYPTED").getOrElse("")
    val regionClause = params.tempDirRegion.map(region => s"REGION AS '$region'").getOrElse("")
    val extraClause = s"${params.extraUnloadOptions}"

    val unloadStmtList = unloadClause :: credClause :: manifestClause :: sseKmsClause ::
      regionClause :: extraClause :: Nil
    unloadStmtList.mkString(" ")
  }

  private def buildUnloadStmt(
                               statement: RedshiftSQLStatement,
                               schema: StructType,
                               tempDir: String,
                               credsProvider: AWSCredentialsProvider,
                               sseKmsKey: Option[String],
                               threadName: String): String = {
    assert(schema.nonEmpty)

    val credsString: String =
      AWSCredentialsUtils.getRedshiftCredentialsString(params, credsProvider.getCredentials)

    // Since the query passed to UNLOAD will be enclosed in single quotes, we need to escape
    // any backslashes and single quotes that appear in the query itself
    val query = statement.statementString.replace("\\", "\\\\").replace("'", "\\'")

    // Save the last query so it can be inspected
    Utils.lastBuildStmt(threadName) = query

    // We need to remove S3 credentials from the unload path URI because they will conflict with
    // the credentials passed via `credsString`.
    val fixedUrl = Utils.fixS3Url(Utils.removeCredentialsFromURI(new URI(tempDir)).toString)

    // Build up the unload statement
    val unloadClause = s"UNLOAD ('SELECT * FROM ($query)') TO '$fixedUrl'"
    val credClause = s"WITH CREDENTIALS '$credsString'"
    val manifestClause = if (params.unloadS3Format == "PARQUET") {
      s"FORMAT AS PARQUET  MANIFEST"
    } else {
      s"ESCAPE MANIFEST NULL AS '${params.nullString}'"
    }
    val sseKmsClause = sseKmsKey.map(key => s"KMS_KEY_ID '$key' ENCRYPTED").getOrElse("")
    val regionClause = params.tempDirRegion.map(region => s"REGION AS '$region'").getOrElse("")
    val extraClause = s"${params.extraUnloadOptions}"

    val unloadStmtList = unloadClause :: credClause :: manifestClause :: sseKmsClause ::
      regionClause :: extraClause :: Nil
    unloadStmtList.mkString(" ")
  }

  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }

  // Build the RDD from a query string, generated by RedshiftStrategy.
  // Type can be InternalRow to comply with SparkPlan's doExecute().
  def buildScanFromSQL[Row](statement: RedshiftSQLStatement,
                            schema: Option[StructType],
                            threadName: String = Thread.currentThread.getName): RDD[Row] = {
    // We must use a connection without a query group for retrieving the schema because
    // Data API only supports parameters with single statement executions and setting
    // the query group requires a batch execution.
    val conn = redshiftWrapper.getConnector(params)
    val resultSchema: StructType = try {
      getResultSchema(statement, schema, conn)
    } finally {
      conn.close()
    }

    val credsProvider =
      AWSCredentialsUtils.load(params, sqlContext.sparkContext.hadoopConfiguration)
    val queryGroup = Utils.queryGroupInfo(Utils.Read, params, sqlContext)
    val connWithQG = redshiftWrapper.getConnectorWithQueryGroup(params, queryGroup)
    val tempDir: Option[String] = try {
      checkS3BucketUsage(params, credsProvider)
      Utils.collectMetrics(params)

      // If the same query was run before, get the result s3 path from the cache.
      // Otherwise, unload the data.
      GetCachedS3QueryPath(statement, threadName)
        .orElse(unloadDataToS3(statement, connWithQG, resultSchema, credsProvider, threadName))
    } finally {
      connWithQG.close()
    }

    val filesToRead: Seq[String] = getFilesToRead(tempDir.get, sqlContext.sparkContext)
    if (params.unloadS3Format == "PARQUET") {
      readRDDFromParquet(resultSchema, filesToRead)
    } else {
      readRDD(resultSchema, filesToRead)
    }
  }

  def runDMLFromSQL(statement: RedshiftSQLStatement): Seq[Row] = {
    // Generate the DML string.
    val statementStr = statement.statementString

    // Save the last query so it can be inspected
    Utils.lastBuildStmt(Thread.currentThread.getName) = statementStr

    // Setup the connection.
    val queryGroup = Utils.queryGroupInfo(Utils.Write, params, sqlContext)
    val conn = redshiftWrapper.getConnectorWithQueryGroup(params, queryGroup)
    redshiftWrapper.setAutoCommit(conn, false)
    try {
      // If the client is writing to a data share, set the database context for data sharing writes.
      if (params.table.isDefined && params.table.get.unescapedDatabaseName.nonEmpty) {
        val useDbStr = s"""use ${params.table.get.escapedDatabaseName}"""
        redshiftWrapper.executeInterruptibly(conn, useDbStr)
      }

      // Execute the DML statement
      val affectedRows = redshiftWrapper.executeUpdate(conn, statementStr)
      redshiftWrapper.commit(conn)
      Seq(Row(affectedRows))
    }
    finally {
      conn.close()
    }
  }

  // Return cached s3 query path (if exists)
  private def GetCachedS3QueryPath(statement: RedshiftSQLStatement,
                                   threadName: String): Option[String] = {
    // If we are not using the cache, abort.
    if (!params.pushdownS3ResultCache) {
      return None
    }

    // Look for a cached path
    val cachedPath = SqlToS3TempCache.getS3Path(statement.statementString)

    // If we found a cached query path, treat it as the last query so it can be inspected.
    if (cachedPath.isDefined) {
      // Since the query passed to UNLOAD will be enclosed in single quotes, we need to escape
      // any backslashes and single quotes that appear in the query itself
      Utils.lastBuildStmt(threadName) =
        statement.statementString.replace("\\", "\\\\").replace("'", "\\'")
    }

    // Return whether we found a cached path
    cachedPath
  }

  // Unload data from Redshift into a temporary directory in S3
  // Note: Connection is passed to this method, & responsibility of
  // managing connection lies to the caller. closing connection here
  // leads to failure if caller method perform another operation on it
  private def unloadDataToS3(statement: RedshiftSQLStatement,
                             conn: RedshiftConnection,
                             resultSchema: StructType,
                             credsProvider: AWSCredentialsProvider,
                             threadName: String): Option[String] = {

    val newTempDir = params.createPerQueryTempDir()
    val unloadSql = buildUnloadStmt(statement,
      resultSchema,
      newTempDir,
      credsProvider,
      params.sseKmsKey,
      threadName)
    log.info("Unloading data from Redshift")
    redshiftWrapper.executeInterruptibly(conn, unloadSql)
    SqlToS3TempCache.setS3Path(statement.statementString, newTempDir)

    Some(newTempDir)
  }

  /**
   * Copy the inputSchema but replace any fields of StructType, ArrayType, or MapType
   * with otherwise identical fields that are of StringType. Retains name, nullability and metadata.
   *
   * @param inputSchema schema to modify
   * @return Copied schema with complex types replaced with string types
   */
  private def convertComplexTypesToString(inputSchema: StructType): StructType = {
    StructType(inputSchema.map(field => field.dataType match {
      case _: StructType | _: ArrayType |
           _: MapType => StructField(field.name, StringType, field.nullable, field.metadata)
      case _ => field
    }))
  }

  /**
   * Convert a schema to a mapping of field/column names to a projection of the column of
   * the same name passed to the from_json function along with its datatype for fields of
   * StructType, ArrayType, or MapType. This mapping can be passed to dataframe.withColumns
   * to add these projections as columns to the dataframe.
   *
   * @param inputSchema schema to convert to a mapping
   * @return A mapping of column names to their projection
   */
  private def mapComplexTypesToJson(inputSchema: StructType): Map[String, Column] = {
    inputSchema.fields.filter({ field =>
      field.dataType match {
        case _: StructType | _: ArrayType | _: MapType => true
        case _ => false
      }
    }).map({ field => (field.name -> from_json(col(field.name), field.dataType)) }).toMap
  }

  private def readRDD[T](resultSchema: StructType, filesToRead: Seq[String]): RDD[T] = {
    log.info("Reading S3 Text files")

    // convert complex types to string types since they are loaded from redshift as json
    // This is also necessary to use the from_json function as it only works on strings
    val noRepeatSchema = StructType(resultSchema.zipWithIndex.map { case (f, index) =>
      StructField(s"field${index}", f.dataType, f.nullable, f.metadata)
    })
    val modifiedSchema = convertComplexTypesToString(noRepeatSchema)

    val dataFrame = sqlContext.read
      .format(classOf[RedshiftFileFormat].getName)
      .schema(modifiedSchema)
      .option("nullString", params.nullString)
      .option(PARAM_OVERRIDE_NULLABLE, params.overrideNullable)
      .load(filesToRead: _*)

    val mapping = mapComplexTypesToJson(noRepeatSchema)

    if (mapping.nonEmpty) {
      dataFrame.withColumns(mapping).queryExecution.executedPlan.execute().asInstanceOf[RDD[T]]
    } else {
      dataFrame.queryExecution.executedPlan.execute().asInstanceOf[RDD[T]]
    }
  }


  private def readRDDFromParquet[T](resultSchema: StructType, filesToRead: Seq[String]): RDD[T] = {
    log.info("Reading S3 Parquet files")

    val reader = sqlContext.read
      .format("parquet")

    // convert complex types to string types since they are loaded from redshift as json
    // This is also necessary to use the from_json function as it only works on strings
    val modifiedSchema = convertComplexTypesToString(resultSchema)

    if (filesToRead.isEmpty) reader.schema(modifiedSchema)
    // cannot pass params.overrideNullable directly as it results in unserializable task
    val overrideNullable = params.overrideNullable

    val data = reader.load(filesToRead: _*)

    // use the actual schema to get the names of columns for the mapping
    val mapping = mapComplexTypesToJson(StructType(data.schema.fields.zip(resultSchema).map {
      case (actualField, expectedField) => StructField(
        actualField.name, expectedField.dataType, expectedField.nullable, expectedField.metadata)
    }))

    val jsonLoaded = if (mapping.nonEmpty) {
      data.withColumns(mapping)
    } else {
      data
    }
    val jsonLoadedSchema = jsonLoaded.schema
    val schemasDoNotMatch = !schemaTypesMatch(resultSchema, jsonLoadedSchema)

    // determine if conversion is needed
    val conversionNeeded = resultSchema.fields.exists({ field: StructField =>
      field.dataType match {
        case StringType =>
          field.metadata.contains("redshift_type") &&
            Seq("super", "bpchar").contains(field.metadata.getString("redshift_type"))
        case TimestampType | ShortType | ByteType => true
        case TimestampNTZTypeExtractor(_) if !redshift.legacyTimestampHandling => true
        case _ => false
      }
    }) || overrideNullable || schemasDoNotMatch

    if (schemasDoNotMatch) {
      log.warn("Expected schema does not match schema of loaded parquet")
    }

    if (conversionNeeded) {
      jsonLoaded.queryExecution.executedPlan.execute.map({ row: InternalRow =>
        val typeConvertedRow = row.toSeq(jsonLoadedSchema).zipWithIndex.map {
          case (f, i) =>
            parquetDataTypeConvert(f, resultSchema.fields(i).dataType,
              if (resultSchema.fields(i).metadata.contains("redshift_type")) {
                resultSchema.fields(i).metadata.getString("redshift_type")
              } else null, overrideNullable)
        }
        InternalRow(typeConvertedRow: _*)
      }).mapPartitions(
        { iter =>
          val projection = UnsafeProjection.create(resultSchema)
          iter.map(projection)
        }).asInstanceOf[RDD[T]]
    } else {
      jsonLoaded.queryExecution.executedPlan.execute().asInstanceOf[RDD[T]]
    }
  }

  // Read the MANIFEST file to get the list of S3 part files that were written by Redshift.
  // We need to use a manifest in order to guard against S3's eventually-consistent listings.
  private def getFilesToRead(tempDir: String, sc: SparkContext): Seq[String] = {

    val tempDirUri = URI.create(tempDir)
    val cleanedTempDirUri = Utils.removeCredentialsFromURI(tempDirUri)
    // Extract the scheme from tempDir (e.g., s3, s3a, etc.)
    val tempDirScheme = tempDirUri.getScheme

    // Extract the credentials (if any) from the original tempDir URI
    val credentials = extractCredentialsFromUri(tempDirUri)

    // Use Hadoop's FileSystem to interact with S3
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(cleanedTempDirUri, conf)
    val manifestPath = new Path(s"${cleanedTempDirUri.toString}/manifest")

    if (fs.exists(manifestPath)) {
      val is = fs.open(manifestPath)
      val s3Files = try {
        log.info("Begin finding S3 files to read")
        val mapper = new ObjectMapper
        val entries = mapper.readTree(new InputStreamReader(is)).get("entries")
        val results = entries.iterator().asScala.map(_.get("url").asText()).toSeq
        log.info("Found {} S3 file(s)", results.length)
        results
      } finally {
        is.close()
        log.info("End finding S3 files to read")
      }

      // Reintroduce credentials (if any) and ensure the scheme matches the tempDir scheme
      // The filenames in the manifest are of the form s3://bucket/key, without credentials.
      // If the S3 credentials were originally specified in the tempdir, then we need to
      // reintroduce them here
      val correctedS3Files = s3Files.map { file =>
        addCredentialsAndEnsureScheme(file, credentials, tempDirScheme)
      }

      correctedS3Files

    } else {
      // If manifest doesn't exist, likely it is because the result set is empty.
      // For empty result set, Redshift sometimes doesn't create any files. So return empty Seq.
      // An unlikely scenario is the result files are removed from S3.
      log.debug("Manifest path not found")
      Seq.empty[String]
    }
  }

  // Function to extract credentials (if present) from the URI
  private def extractCredentialsFromUri(uri: URI): Option[String] = {
    Option(uri.getUserInfo)
  }

  // Function to reintroduce credentials and ensure the same scheme
  private def addCredentialsAndEnsureScheme(filePath: String,
                                            credentials: Option[String],
                                            scheme: String): String = {
    val fileUri = URI.create(filePath)

    // Extract the bucket (host part)
    val bucket = fileUri.getHost

    // Extract the prefix path, which is the part after the bucket
    val prefixPath = fileUri.getPath.stripPrefix("/")

    // Reconstruct the file path without credentials and scheme
    val filePathWithoutSchemeAndCreds = s"$bucket/$prefixPath"

    // Add credentials (if available) and scheme to the constructed path
    credentials match {
      case Some(creds) =>
        s"$scheme://$creds@$filePathWithoutSchemeAndCreds"
      case None =>
        s"$scheme://$filePathWithoutSchemeAndCreds"
    }
  }

  // Note: this method should not cause side effect.
  // The connection is managed by the caller method.
  private def getResultSchema(statement: RedshiftSQLStatement,
                              schema: Option[StructType],
                              conn: RedshiftConnection): StructType = {
    val resultSchema = schema.getOrElse(
      redshiftWrapper.tableSchema(conn, statement, params)
    )

    if (resultSchema.isEmpty) {
      throw new Exception("resultSchema isEmpty for " + statement.statementString)
    }
    resultSchema
  }
}

private[redshift] object RedshiftRelation {
  /**
   * Check if two schemas match in types ignoring differences of metadata and name.
   * Intended only for use in readRDDFromParquet.
   * @param schema1 Schema to compare
   * @param schema2 Schema to compare
   * @return If the schemas have the same types in all fields
   */
  def schemaTypesMatch(schema1: DataType, schema2: DataType): Boolean =
    (schema1, schema2) match {
      case (s1: StructType, s2: StructType) if s1.length != s2.length =>
        throw new IllegalStateException("Schema types do not match in length")
      case (s1: StructType, s2: StructType) => s1.fields.zip(s2.fields).
        forall({
          case (f1: StructField, f2: StructField) => schemaTypesMatch(f1.dataType, f2.dataType)})
      case (ArrayType(dt1, _), ArrayType(dt2, _)) => schemaTypesMatch(dt1, dt2)
      case (MapType(kt1, vt1, _), MapType(kt2, vt2, _)) =>
        schemaTypesMatch(kt1, kt2) && schemaTypesMatch(vt1, vt2 )
      case _ => schema1.typeName == schema2.typeName
    }
}