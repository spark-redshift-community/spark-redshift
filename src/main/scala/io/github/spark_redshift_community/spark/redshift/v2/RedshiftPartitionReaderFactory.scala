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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.types.StructType

case class RedshiftPartitionReaderFactory(readerFactory: PartitionReaderFactory,
    schema: StructType, params: MergedParameters)
  extends FilePartitionReaderFactory {

  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    val filePartReaderFactory = readerFactory.asInstanceOf[FilePartitionReaderFactory]
    new RedshiftPartitionReader(filePartReaderFactory.buildReader(file), schema, params)
  }
}
