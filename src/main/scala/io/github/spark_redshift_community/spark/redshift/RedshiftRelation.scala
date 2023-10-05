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
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.github.spark_redshift_community.spark.redshift.Conversions.parquetDataTypeConvert
import io.github.spark_redshift_community.spark.redshift.DefaultJDBCWrapper.DataBaseOperations
import io.github.spark_redshift_community.spark.redshift.Parameters.{MergedParameters, PARAM_OVERRIDE_NULLABLE}
import io.github.spark_redshift_community.spark.redshift.RedshiftRelation.schemaTypesMatch
import io.github.spark_redshift_community.spark.redshift.pushdown.{RedshiftSQLStatement, SqlToS3TempCache}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext, SaveMode}

import java.io.InputStreamReader
import java.net.URI
import java.sql.Connection
import scala.collection.JavaConverters._

/**
 * Data Source API implementation for Amazon Redshift database tables
 */
private[redshift] case class RedshiftRelation(
    jdbcWrapper: JDBCWrapper,
    s3ClientFactory: (AWSCredentialsProvider, MergedParameters) => AmazonS3,
    params: MergedParameters,
    userSchema: Option[StructType])
    (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation
    with Logging {

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
      val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, Some(params))
      try {
        jdbcWrapper.resolveTable(conn, tableNameOrSubquery, Some(params))
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

  private def checkS3BucketUsage(params: MergedParameters, s3Client: AmazonS3): Unit = {
    // Make sure a cross-region read has the necessary connector parameters set.
    Utils.checkRedshiftAndS3OnSameRegion(params, s3Client)

    // Make sure that the bucket has a lifecycle configuration set to automatically clean-up.
    Utils.checkThatBucketHasObjectLifecycleConfiguration(params.rootTempDir, s3Client)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val creds = AWSCredentialsUtils.load(params, sqlContext.sparkContext.hadoopConfiguration)
    checkS3BucketUsage(params, s3ClientFactory(creds, params))
    Utils.collectMetrics(params)
    val queryGroup = Utils.queryGroupInfo(Utils.Read, params.user_query_group_label, sqlContext)

    if (requiredColumns.isEmpty) {
      // In the special case where no columns were requested, issue a `count(*)` against Redshift
      // rather than unloading data.
      val whereClause = FilterPushdown.buildWhereClause(schema, filters)
      val countQuery = s"SELECT count(*) FROM $tableNameOrSubquery $whereClause"
      val conn = jdbcWrapper.getConnectorWithQueryGroup(
        params.jdbcDriver, params.jdbcUrl, Some(params), queryGroup)
      try {
        log.info("Getting number of rows from Redshift")
        val results = jdbcWrapper.executeQueryInterruptibly(conn.prepareStatement(countQuery))
        if (results.next()) {
          val numRows = results.getLong(1)
          log.info("Number of rows is {}", numRows)
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
      val conn = jdbcWrapper.getConnectorWithQueryGroup(
        params.jdbcDriver, params.jdbcUrl, Some(params), queryGroup)
      try {
        log.info("Unloading data from Redshift")
        jdbcWrapper.executeInterruptibly(conn.prepareStatement(unloadSql))
      } finally {
        conn.close()
      }

      val filesToRead: Seq[String] = getFilesToRead(creds, tempDir)

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
      creds: AWSCredentialsProvider,
      sseKmsKey: Option[String]): String = {
    assert(!requiredColumns.isEmpty)
    // Always quote column names:
    val columnList = requiredColumns.map(col => s""""$col"""").mkString(", ")
    val whereClause = FilterPushdown.buildWhereClause(schema, filters, escapeQuote = true)
    val credsString: String =
      AWSCredentialsUtils.getRedshiftCredentialsString(params, creds.getCredentials)
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
                               creds: AWSCredentialsProvider,
                               sseKmsKey: Option[String],
                               threadName: String): String = {
    assert(schema.nonEmpty)

    val credsString: String =
      AWSCredentialsUtils.getRedshiftCredentialsString(params, creds.getCredentials)

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
    val queryGroup = Utils.queryGroupInfo(Utils.Read, params.user_query_group_label, sqlContext)
    val conn = jdbcWrapper.getConnectorWithQueryGroup(
      params.jdbcDriver, params.jdbcUrl, Some(params), queryGroup)
    val creds = AWSCredentialsUtils.load(params, sqlContext.sparkContext.hadoopConfiguration)

    val (resultSchema, tempDir) = try {

      val resultSchema: StructType = getResultSchema(statement, schema, conn)

      checkS3BucketUsage(params, s3ClientFactory(creds, params))
      Utils.collectMetrics(params)

      // If the same query was run before, get the result s3 path from the cache.
      // Otherwise, unload the data.
      val tempDir = GetCachedS3QueryPath(statement, threadName)
        .orElse {
          UnloadDataToS3(statement, conn, resultSchema, creds, threadName)
        }

      (resultSchema, tempDir)
    } finally {
      conn.close()
    }

    val filesToRead: Seq[String] = getFilesToRead(creds, tempDir.get)
    if (params.unloadS3Format == "PARQUET") {
      readRDDFromParquet(resultSchema, filesToRead)
    } else {
      readRDD(resultSchema, filesToRead)
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
  private def UnloadDataToS3(statement: RedshiftSQLStatement,
                                  conn: Connection,
                                  resultSchema: StructType,
                                  creds: AWSCredentialsProvider,
                             threadName: String): Option[String] = {

    val newTempDir = params.createPerQueryTempDir()
    val unloadSql = buildUnloadStmt(statement,
                                    resultSchema,
                                    newTempDir,
                                    creds,
                                    params.sseKmsKey,
                                    threadName)
    log.info("Unloading data from Redshift")
    jdbcWrapper.executeInterruptibly(conn.prepareStatement(unloadSql))
    SqlToS3TempCache.setS3Path(statement.statementString, newTempDir)

    Some(newTempDir)
  }

  /**
   * Copy the inputSchema but replace any fields of StructType, ArrayType, or MapType
   * with otherwise identical fields that are of StringType. Retains name, nullability and metadata.
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
   * @param inputSchema schema to convert to a mapping
   * @return A mapping of column names to their projection
   */
  private def mapComplexTypesToJson(inputSchema: StructType): Map[String, Column] = {
    inputSchema.fields.filter({field => field.dataType match {
        case _ : StructType | _: ArrayType | _: MapType => true
        case _ => false
      }
    }).map({field => (field.name -> from_json(col(field.name), field.dataType))}).toMap
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
    val conversionNeeded = resultSchema.fields.exists({field: StructField => field.dataType match {
      case StringType =>
        field.metadata.contains("redshift_type") &&
          Seq("super", "bpchar").contains(field.metadata.getString("redshift_type"))
      case TimestampType | ShortType | ByteType => true
      case _ => false
    }}) || overrideNullable || schemasDoNotMatch

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
  private def getFilesToRead[Row](creds: AWSCredentialsProvider, tempDir: String) = {

    val cleanedTempDirUri =
      Utils.fixS3Url(Utils.removeCredentialsFromURI(URI.create(tempDir)).toString)
    val s3URI = Utils.createS3URI(cleanedTempDirUri)
    val s3Client = s3ClientFactory(creds, params)

    if(s3Client.doesObjectExist(s3URI.getBucket, s3URI.getKey + "manifest")) {
      val is = s3Client.getObject(s3URI.getBucket, s3URI.getKey + "manifest").getObjectContent
      val s3Files = try {
        log.info("Begin finding S3 files to read")
        val mapper = new ObjectMapper
        mapper.registerModule(DefaultScalaModule)
        val entries = mapper.readTree(new InputStreamReader(is)).get("entries")
        val results = entries.iterator().asScala.map(_.get("url").asText()).toSeq
        log.info("Found {} S3 file(s)", results.length)
        results
      } finally {
        is.close()
        log.info("End finding S3 files to read")
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

  // Note: this method should not cause side effect.
  // The connection is managed by the caller method.
  private def getResultSchema(statement: RedshiftSQLStatement,
                              schema: Option[StructType],
                              conn: Connection): StructType = {
    val resultSchema = schema.getOrElse(
        conn.tableSchema(statement, params)
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