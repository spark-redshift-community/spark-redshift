/*
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


import java.sql.Timestamp
import java.text.{DecimalFormat, DecimalFormatSymbols, SimpleDateFormat}
import java.time.{DateTimeException, LocalDateTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.Locale
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

/**
 * Data type conversions for Redshift unloaded data
 */
private[redshift] object Conversions {

  /**
    * From the DateTimeFormatter docs (Java 8):
    * "A formatter created from a pattern can be used as many times as necessary,
    * it is immutable and is thread-safe."
    */
  private val formatter = DateTimeFormatter.ofPattern(
    "yyyy-MM-dd HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S][X]")

  /**
   * Parse a boolean using Redshift's UNLOAD bool syntax
   */
  private def parseBoolean(s: String): Boolean = {
    if (s == "t") true
    else if (s == "f") false
    else throw new IllegalArgumentException(s"Expected 't' or 'f' but got '$s'")
  }

  /**
   * Formatter for writing decimals unloaded from Redshift.
   *
   * Note that Java Formatters are NOT thread-safe, so you should not re-use instances of this
   * DecimalFormat across threads.
   */
  def createRedshiftDecimalFormat(): DecimalFormat = {
    val format = new DecimalFormat()
    format.setParseBigDecimal(true)
    format.setDecimalFormatSymbols(new DecimalFormatSymbols(Locale.US))
    format
  }

  /**
   * Formatter for parsing strings exported from Redshift DATE columns.
   *
   * Note that Java Formatters are NOT thread-safe, so you should not re-use instances of this
   * SimpleDateFormat across threads.
   */
  def createRedshiftDateFormat(): SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  /**
   * Formatter for formatting timestamps for insertion into Redshift TIMESTAMP columns.
   *
   * This formatter should not be used to parse timestamps returned from Redshift UNLOAD commands;
   * instead, use [[Timestamp.valueOf()]].
   *
   * Note that Java Formatters are NOT thread-safe, so you should not re-use instances of this
   * SimpleDateFormat across threads.
   */
  def createRedshiftTimestampFormat(): DateTimeFormatter = {
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSS")
  }

  def parseRedshiftTimestamp(s: String): Timestamp = {
    val temporalAccessor = formatter.parse(s)

    try {
      // timestamptz
      Timestamp.from(ZonedDateTime.from(temporalAccessor).toInstant)
    }
    catch {
      // Case timestamp without timezone
      case e: DateTimeException =>
        Timestamp.valueOf(LocalDateTime.from(temporalAccessor))
    }
  }

  /**
   * Return a function that will convert arrays of strings conforming to the given schema to Rows.
   *
   * Note that instances of this function are NOT thread-safe.
   */
  def createRowConverter(schema: StructType,
                         nullString: String,
                         overrideNullable: Boolean): Array[String] => InternalRow = {
    val dateFormat = createRedshiftDateFormat()
    val decimalFormat = createRedshiftDecimalFormat()
    val conversionFunctions: Array[String => Any] = schema.fields.map { field =>
      field.dataType match {
        case ByteType => (data: String) => java.lang.Byte.parseByte(data)
        case BooleanType => (data: String) => parseBoolean(data)
        case DateType => (data: String) => new java.sql.Date(dateFormat.parse(data).getTime)
        case DoubleType => (data: String) => data match {
          case "nan" => Double.NaN
          case "inf" => Double.PositiveInfinity
          case "-inf" => Double.NegativeInfinity
          case _ => java.lang.Double.parseDouble(data)
        }
        case FloatType => (data: String) => data match {
          case "nan" => Float.NaN
          case "inf" => Float.PositiveInfinity
          case "-inf" => Float.NegativeInfinity
          case _ => java.lang.Float.parseFloat(data)
        }
        case dt: DecimalType =>
          (data: String) => decimalFormat.parse(data).asInstanceOf[java.math.BigDecimal]
        case IntegerType => (data: String) => java.lang.Integer.parseInt(data)
        case LongType => (data: String) => java.lang.Long.parseLong(data)
        case ShortType => (data: String) => java.lang.Short.parseShort(data)
        case StringType => (data: String) => data
        case TimestampType => (data: String) => parseRedshiftTimestamp(data)
        case _ => (data: String) => data
      }
    }
    // As a performance optimization, re-use the same mutable row / array:
    val converted: Array[Any] = Array.fill(schema.length)(null)
    val externalRow = new GenericRow(converted)
    val toRow = RowEncoderUtils.expressionEncoderForSchema(schema).createSerializer()
    (inputRow: Array[String]) => {
      var i = 0
      while (i < schema.length) {
        val data = inputRow(i)
        if (overrideNullable) {
          converted(i) = if (data == null || data.isEmpty) null else conversionFunctions(i)(data)
        } else {
          converted(i) = if ((data == null || data == nullString) ||
            (data.isEmpty && schema.fields(i).dataType != StringType)) {
            null
          }
          else if (data.isEmpty) {
            ""
          }
          else {
            conversionFunctions(i)(data)
          }
        }

        i += 1
      }
      toRow.apply(externalRow)
    }
  }

  def parquetDataTypeConvert(from: Any,
                             dataType: DataType,
                             redshiftType: String,
                             overrideNullable: Boolean): Any = {
    dataType match {
      case _ if overrideNullable && from!= null && from.toString.isEmpty => null
      // Redshift does not have a cast for single byte so it will be received as a string
      case ByteType if from != null => java.lang.Byte.parseByte(from.toString)
      case DoubleType if from != null => from.asInstanceOf[Number].doubleValue
      case FloatType if from != null => from.asInstanceOf[Number].floatValue
      case IntegerType if from != null => from.asInstanceOf[Number].intValue
      case LongType if from != null => from.asInstanceOf[Number].longValue
      case ShortType if from != null => from.asInstanceOf[Number].shortValue
      case _: DecimalType if from != null && from.isInstanceOf[Double] =>
        org.apache.spark.sql.types.Decimal(from.asInstanceOf[Double])
      case _: DecimalType if from != null & from.isInstanceOf[org.apache.spark.sql.types.Decimal] =>
        // needed when the schema decimals differ by precision and scale
        org.apache.spark.sql.types.Decimal(
          from.asInstanceOf[org.apache.spark.sql.types.Decimal].toBigDecimal)
      case StringType =>
        // Redshift returns null values in Parquet format as null values. This is fine
        // except for super data types which are supposed to be returned as valid JSON which
        // is the null literal instead of the null value (i.e., "null" versus null).
        // Therefore, we do the conversion here to workaround the Redshift limitation.
        if (from == null && redshiftType == "super") {
          org.apache.spark.unsafe.types.UTF8String.fromString("null")
        } else if (from != null && redshiftType == "bpchar") {
          from.asInstanceOf[org.apache.spark.unsafe.types.UTF8String].trimRight()
        } else {
          from
        }

      // When Redshift TIMESTAMPTZ fields are unloaded from Redshift in Parquet, Redshift
      // will always convert the timestamps to UTC prior to unloading as Parquet. This is
      // correct behavior by Redshift as Parquet only supports UTC timezones. For this case,
      // we can load these unloaded values directly into Java Timestamps as the constructor
      // also assumes UTC-relative timestamp values. In contrast, when Redshift TIMESTAMP fields
      // (i.e., no timezone) are unloaded from Redshift, Redshift will not do any conversion and the
      // times will be non-instant times and relative. Unfortunately, Spark does not have a
      // corresponding relative timestamp type. Instead, it supports the TimestampType which is an
      // instant type with a timezone. Therefore, the connector must make an assumption here as
      // there is simply not enough information in the data. For handling this case, the connector
      // assumes any relative timestamps are relative to the local timezone where the connector is
      // running. Note that if this is not correct, the user can set the spark local timezone to
      // match the data since the connector always uses the local timezone to convert any relative
      // timestamp data.
      case DateType if from!=null && from.isInstanceOf[Long] =>
        DateTimeUtils.microsToDays(
          from.asInstanceOf[Long], ZoneId.of("UTC"))
      case TimestampType if from!=null =>
        if (redshiftType == "timestamptz") {
          from
        } else DateTimeUtils.toUTCTime(from.asInstanceOf[Long], ZoneId.systemDefault().getId)
      case _ => from
    }
  }
}
