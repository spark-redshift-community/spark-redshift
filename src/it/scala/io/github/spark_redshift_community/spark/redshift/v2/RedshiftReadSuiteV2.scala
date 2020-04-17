/*
 * Copyright 2016 Databricks
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

package io.github.spark_redshift_community.spark.redshift.v2

import io.github.spark_redshift_community.spark.redshift.RedshiftReadSuite
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}

/**
 * End-to-end tests of functionality which only impacts the read path (e.g. filter pushdown).
 */
class RedshiftReadSuiteV2 extends RedshiftReadSuite {
  /**
   * Create a new DataFrameReader using common options for reading from Redshift.
   */
  override protected def read: DataFrameReader = {
    val reader = super.read
    reader.format("redshift")
    reader.option("unloadformat", "parquet")
  }

  override def write(df: DataFrame): DataFrameWriter[Row] = {
    throw new  UnsupportedOperationException
  }

  override def getUnloadFormats: Seq[String] = {
    super.getUnloadFormats :+ "parquet"
  }
}