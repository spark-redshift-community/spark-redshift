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

import io.github.spark_redshift_community.spark.redshift.Parameters.MergedParameters
import io.github.spark_redshift_community.spark.redshift.pushdown.{ConstantString, RedshiftSQLStatement}
import org.apache.spark.sql.types._

private[redshift] case class QueryParameter[T](
  name: String, value: Option[T], nullType: Int
)

private[redshift] class RedshiftWrapper extends Serializable {

  protected val MASTER_LOG_PREFIX = "Spark Connector Master"
  protected val WORKER_LOG_PREFIX = "Spark Connector Worker"

  /**
   * Compute the SQL schema string for the given Spark SQL Schema.
   */
  def schemaString(schema: StructType,
                   params: Option[MergedParameters] = None): String = {
    val sb = new StringBuilder()
    schema.fields.foreach { field => {
      val name = field.name
      val typ: String = if (field.metadata.contains("redshift_type")) {
        field.metadata.getString("redshift_type")
      } else {
        field.dataType match {
          case IntegerType => "INTEGER"
          case LongType => "BIGINT"
          case DoubleType => "DOUBLE PRECISION"
          case FloatType => "REAL"
          case ShortType => if (params.exists(_.legacyMappingShortToInt)) { "INTEGER" }
                            else { "SMALLINT" }
          case ByteType => "SMALLINT" // Redshift does not support the BYTE type.
          case BooleanType => "BOOLEAN"
          case StringType =>
            if (field.metadata.contains("maxlength")) {
              s"VARCHAR(${field.metadata.getLong("maxlength")})"
            } else {
              s"VARCHAR(MAX)"
            }
          case TimestampType => "TIMESTAMP"
          case DateType => "DATE"
          case t: DecimalType => s"DECIMAL(${t.precision},${t.scale})"
          case _: ArrayType | _: MapType | _: StructType => "SUPER"
          case _ => throw new IllegalArgumentException(s"Don't know how to save $field to JDBC")
        }
      }

      val nullable = if (field.nullable) "" else "NOT NULL"
      val encoding = if (field.metadata.contains("encoding")) {
        s"ENCODE ${field.metadata.getString("encoding")}"
      } else {
        ""
      }
      sb.append(s""", "${name.replace("\"", "\\\"")}" $typ $nullable $encoding""".trim)
    }
    }
    if (sb.length < 2) "" else sb.substring(2)
  }

  def setAutoCommit(conn: RedshiftConnection, autoCommit: Boolean): Unit
    = throw new NotImplementedError()

  def commit(conn: RedshiftConnection): Unit
    = throw new NotImplementedError()

  def rollback(conn: RedshiftConnection): Unit
  = throw new NotImplementedError()

  def executeInterruptibly(conn: RedshiftConnection, sql: String): Boolean
    = throw new NotImplementedError()

  def executeQueryInterruptibly(conn: RedshiftConnection, sql: String): RedshiftResults
    = throw new NotImplementedError()

  def executeUpdateInterruptibly(conn: RedshiftConnection, sql: String): Long
    = throw new NotImplementedError()

  def executeUpdate(conn: RedshiftConnection, sql: String): Long = throw new NotImplementedError()

  def getConnector(params: MergedParameters): RedshiftConnection = throw new NotImplementedError()

  def getConnectorWithQueryGroup(
    params: MergedParameters,
    queryGroup: String): RedshiftConnection = throw new NotImplementedError()

  def resolveTable(conn: RedshiftConnection,
                   table: String,
                   params: Option[MergedParameters] = None): StructType
    = throw new NotImplementedError()

  def tableExists(conn: RedshiftConnection, table: String): Boolean
    = throw new NotImplementedError()

  def tableSchema(conn: RedshiftConnection,
                  statement: RedshiftSQLStatement,
                  params: MergedParameters): StructType = throw new NotImplementedError()
}
