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

import io.github.spark_redshift_community.spark.redshift.{Conversions, Parameters}
import io.github.spark_redshift_community.spark.redshift.Parameters.MergedParameters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

class RedshiftPartitionReader(reader: PartitionReader[InternalRow], schema: StructType,
  params: MergedParameters)
  extends PartitionReader[InternalRow] {

  val converter: Array[String] => InternalRow = {
    Conversions.createRowConverter(schema,
      Parameters.DEFAULT_PARAMETERS("csvnullstring"))
  }

  private val isCSVFormat = params.getUnloadFormat == "csv"

  override def next(): Boolean = {
    reader.next()
  }

  override def get(): InternalRow = {
    if (isCSVFormat) {
      val row = reader.get()
      val values = (0 until row.numFields).map(index => row.getString(index))
      converter(values.toArray)
    } else {
      reader.get()
    }
  }

  override def close(): Unit = {
    reader.close()
  }
}
