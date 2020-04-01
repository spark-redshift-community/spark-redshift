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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.types.StructType

case class RedshiftScan(scan: FileScan, schema: StructType,
    params: MergedParameters) extends FileScan {
  /**
   * Create a new `FileScan` instance from the current one
   * with different `partitionFilters` and `dataFilters`
   */
  override def withFilters(partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression]): FileScan = scan.withFilters(partitionFilters, dataFilters)

  override def createReaderFactory(): PartitionReaderFactory = {
    RedshiftPartitionReaderFactory(scan.createReaderFactory(), schema, params)
  }

  override def sparkSession: SparkSession = scan.sparkSession

  override def fileIndex: PartitioningAwareFileIndex = scan.fileIndex

  override def readDataSchema: StructType = scan.readDataSchema

  override def readPartitionSchema: StructType = scan.readPartitionSchema

  override def partitionFilters: Seq[Expression] = scan.partitionFilters

  override def dataFilters: Seq[Expression] = scan.partitionFilters
}
