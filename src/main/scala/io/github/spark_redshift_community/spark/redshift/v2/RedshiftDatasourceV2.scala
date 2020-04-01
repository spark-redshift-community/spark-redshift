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

import io.github.spark_redshift_community.spark.redshift.{DefaultJDBCWrapper, Parameters, RedshiftFileFormat}
import io.github.spark_redshift_community.spark.redshift.Parameters.MergedParameters
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

class RedshiftDatasourceV2 extends FileDataSourceV2 with DataSourceRegister with Logging {

  /**
   * Returns a V1 [[FileFormat]] class of the same file data source.
   * This is a solution for the following cases:
   * 1. File datasource V2 implementations cause regression. Users can disable the problematic data
   * source via SQL configuration and fall back to FileFormat.
   * 2. Catalog support is required, which is still under development for data source V2.
   */
  // FIXME
  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[RedshiftFileFormat]

  private var params: MergedParameters = _

  private val jdbcWrapper = DefaultJDBCWrapper

  override def shortName(): String = "redshift"

  var schema: Option[StructType] = None

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    initParams(options)
    // FIXME
    RedshiftTable("tableName", sparkSession, options,
      jdbcWrapper, schema, fallbackFileFormat)
  }

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    initParams(options, Some(schema))
    RedshiftTable("tableName",
      sparkSession, options, jdbcWrapper, Some(schema), fallbackFileFormat)
  }

  override def getPaths(map: CaseInsensitiveStringMap): Seq[String] = {
    Seq(map.get("tempdir"))
  }

  def getSchema(userSpecifiedSchema: Option[StructType] = None): Option[StructType] = {
    if (schema.isEmpty) {
      schema = Option(userSpecifiedSchema.getOrElse {
        val tableNameOrSubquery =
          params.query.map(q => s"($q)").orElse(params.table.map(_.toString)).get
        val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, params.credentials)
        try {
          jdbcWrapper.resolveTable(conn, tableNameOrSubquery)
        } finally {
          conn.close()
        }
      })
    }
    schema
  }

  private def initParams(options: CaseInsensitiveStringMap,
    userSpecifiedSchema: Option[StructType] = None): Unit = {
    params = Parameters.mergeParameters(options.asScala.toMap)
    schema = getSchema(userSpecifiedSchema)
  }
}
