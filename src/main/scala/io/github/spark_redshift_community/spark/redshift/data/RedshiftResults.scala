/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package io.github.spark_redshift_community.spark.redshift.data

import com.amazonaws.services.redshiftdataapi.model.{Field, GetStatementResultResult}

import java.sql.ResultSet
import scala.collection.JavaConverters._

private[redshift] abstract class RedshiftResults() {
  def next(): Boolean

  // 1-based indexing per JDBC convention
  def getInt(columnIndex: Int): Int
  def getLong(columnIndex: Int): Long
  def getString(columnIndex: Int): String
  def getBinary(columnIndex: Int): Array[Byte]

  def getInt(columnLabel: String): Int
  def getLong(columnLabel: String): Long
  def getString(columnLabel: String): String
  def getBinary(columnLabel: String): Array[Byte]

}

private[redshift] case class DataApiResults(results: GetStatementResultResult)
  extends RedshiftResults {

  private val iter = results.getRecords.asScala.iterator
  private var curr : java.util.List[Field] = null

  override def next(): Boolean = {
    if (!iter.hasNext) {
      false
    } else {
      curr = iter.next
      true
    }
  }

  override def getInt(columnIndex: Int): Int = {
    curr.get(columnIndex - 1).getLongValue.asInstanceOf[Int]
  }

  override def getLong(columnIndex: Int): Long = {
    curr.get(columnIndex - 1).getLongValue
  }

  override def getString(columnIndex: Int): String = {
    curr.get(columnIndex - 1).getStringValue
  }

  override def getBinary(columnIndex: Int): Array[Byte] = {
    curr.get(columnIndex - 1).getBlobValue.array()
  }

  override def getInt(columnLabel: String): Int = {
    curr.get(getIndex(columnLabel)).getLongValue.asInstanceOf[Int]
  }

  override def getLong(columnLabel: String): Long = {
    curr.get(getIndex(columnLabel)).getLongValue
  }

  override def getString(columnLabel: String): String = {
    curr.get(getIndex(columnLabel)).getStringValue
  }

  def getBinary(columnLabel: String): Array[Byte] = {
    curr.get(getIndex(columnLabel)).getBlobValue.array()
  }

  private def getIndex(columnLabel: String): Int = {
    results.getColumnMetadata.asScala.indexWhere(col => col.getLabel == columnLabel)
  }
}

private[redshift] case class JDBCResults(results: ResultSet) extends RedshiftResults {
  override def next(): Boolean = {
    results.next()
  }

  override def getInt(columnIndex: Int): Int = {
    results.getInt(columnIndex)
  }

  override def getLong(columnIndex: Int): Long = {
    results.getLong(columnIndex)
  }

  override def getString(columnIndex: Int): String = {
    results.getString(columnIndex)
  }

  override def getBinary(columnIndex: Int): Array[Byte] = {
    results.getBytes(columnIndex)
  }

  override def getInt(columnLabel: String): Int = {
    results.getInt(columnLabel)
  }

  override def getLong(columnLabel: String): Long = {
    results.getLong(columnLabel)
  }

  override def getString(columnLabel: String): String = {
    results.getString(columnLabel)
  }

  override def getBinary(columnLabel: String): Array[Byte] = {
    results.getBytes(columnLabel)
  }
}
