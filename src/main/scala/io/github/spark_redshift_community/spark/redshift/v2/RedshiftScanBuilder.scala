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

import io.github.spark_redshift_community.spark.redshift.Parameters.MergedParameters
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownFilters}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits.OptionsHelper
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.datasources.{FileStatusCache, InMemoryFileIndex, PartitioningAwareFileIndex}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StringType, StructType}

case class RedshiftScanBuilder(
    spark: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    params: MergedParameters)
  extends FileScanBuilder(spark, fileIndex, dataSchema) with SupportsPushDownFilters{

  private var filters: Array[Filter] = Array.empty

  override def build(): Scan = {
    val index = preBuild()

    val convertedReadSchema = StructType(readDataSchema()
      .copy().map(field => field.copy(dataType = StringType)))
    val convertedDataSchema = StructType(dataSchema.copy().map(x => x.copy(dataType = StringType)))
    val delegate = if (params.parameters.getOrElse("unloadformat", "csv").toLowerCase()== "csv") {
      val options = (params.parameters + ("delimiter" -> "|")).asOptions
      CSVScan(spark, index, convertedDataSchema, convertedReadSchema,
        readPartitionSchema(), options, pushedFilters())
    } else {
      val options = params.parameters.asOptions
      ParquetScan(spark, spark.sessionState.newHadoopConf(), index, dataSchema,
        readDataSchema(), readPartitionSchema(), pushedFilters(), options)
    }
    RedshiftScan(delegate, readDataSchema(), params)
  }

  private def preBuild(): PartitioningAwareFileIndex = {
    val preProcessor = new RedshiftPreProcessor(spark, Some(dataSchema), readDataSchema(),
      params, pushedFilters())
    val paths = preProcessor.process()
    // This is a non-streaming file based datasource.
    val rootPathsSpecified = paths.map(p => new Path(p))
    val fileStatusCache = FileStatusCache.getOrCreate(spark)
    val caseSensitiveMap = params.parameters
    new InMemoryFileIndex(
      spark, rootPathsSpecified, caseSensitiveMap, Some(dataSchema), fileStatusCache)
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    this.filters = filters
    filters
  }

  override def pushedFilters(): Array[Filter] = {
    filters
  }


}