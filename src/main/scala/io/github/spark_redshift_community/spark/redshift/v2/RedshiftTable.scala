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

import java.util

import scala.collection.JavaConverters._

import io.github.spark_redshift_community.spark.redshift.Parameters.MergedParameters
import io.github.spark_redshift_community.spark.redshift.{JDBCWrapper, Parameters}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, V1_BATCH_WRITE}
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.execution.datasources.{FileFormat, FileStatusCache, InMemoryFileIndex}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class RedshiftTable(tableName: String,
    spark: SparkSession,
    options: CaseInsensitiveStringMap,
    JDBCWrapper: JDBCWrapper,
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat])
  extends Table with SupportsRead with SupportsWrite {

  val params: MergedParameters = Parameters.mergeParameters(options.asScala.toMap)


  /**
   * The string that represents the format that this data source provider uses. This is
   * overridden by children to provide a nice alias for the data source. For example:
   *
   * {{{
   *   override def formatName(): String = "ORC"
   * }}}
   */

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val fileStatusCache = FileStatusCache.getOrCreate(spark)
    val index = new InMemoryFileIndex(
      spark, Seq.empty, params.parameters, userSpecifiedSchema, fileStatusCache)

    RedshiftScanBuilder(spark, index, schema, userSpecifiedSchema.get, params)
  }

  /**
   * A name to identify this table. Implementations should provide a meaningful name, like the
   * database and table name from catalog, or the location of files for this table.
   */
  override def name(): String = "redshift"

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val mergedOptions = new JdbcOptionsInWrite(
      params.parameters ++ info.options.asCaseSensitiveMap().asScala)
    RedshiftWriteBuilder(schema, mergedOptions)
  }
  override def schema(): StructType = userSpecifiedSchema.get
  override def capabilities(): util.Set[TableCapability] = Set(BATCH_READ, V1_BATCH_WRITE).asJava
}
