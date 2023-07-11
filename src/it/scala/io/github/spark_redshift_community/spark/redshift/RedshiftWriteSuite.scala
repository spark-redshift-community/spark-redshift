/*
 * Copyright 2015 Databricks
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

import java.sql.{Date, SQLException, Timestamp}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
 * End-to-end tests of functionality which involves writing to Redshift via the connector.
 */
abstract class BaseRedshiftWriteSuite extends IntegrationSuiteBase {

  protected val tempformat: String

  override protected def write(df: DataFrame): DataFrameWriter[Row] =
    super.write(df).option("tempformat", tempformat)

  test("roundtrip save and load") {
    // This test can be simplified once #98 is fixed.
    val tableName = s"roundtrip_save_and_load_$randomSuffix"
    try {
      write(
        sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData), TestUtils.testSchema))
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      checkAnswer(read.option("dbtable", tableName).load(), TestUtils.expectedData)
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
    }
  }

  test("roundtrip save and load with uppercase column names") {
    testRoundtripSaveAndLoad(
      s"roundtrip_write_and_read_with_uppercase_column_names_$randomSuffix",
      sqlContext.createDataFrame(
        sc.parallelize(Seq(Row(1))), StructType(StructField("SomeColumn", IntegerType) :: Nil)
      ),
      expectedSchemaAfterLoad = Some(StructType(StructField("somecolumn", IntegerType, true,
        new MetadataBuilder().putString("redshift_type", "int4").build()) :: Nil))
    )
  }

  test("save with column names that are reserved words") {
    testRoundtripSaveAndLoad(
      s"save_with_column_names_that_are_reserved_words_$randomSuffix",
      sqlContext.createDataFrame(
        sc.parallelize(Seq(Row(1))),
        StructType(StructField("table", IntegerType, true,
          new MetadataBuilder().putString("redshift_type", "int4").build()) :: Nil)
      )
    )
  }

  test("save with one empty partition (regression test for #96)") {
    val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1)), 2),
      StructType(StructField("foo", IntegerType, true,
        new MetadataBuilder().putString("redshift_type", "int4").build()) :: Nil))
    assert(df.rdd.glom.collect() === Array(Array.empty[Row], Array(Row(1))))
    testRoundtripSaveAndLoad(s"save_with_one_empty_partition_$randomSuffix", df)
  }

  test("save with all empty partitions (regression test for #96)") {
    val df = sqlContext.createDataFrame(sc.parallelize(Seq.empty[Row], 2),
      StructType(StructField("foo", IntegerType, true,
        new MetadataBuilder().putString("redshift_type", "int4").build()) :: Nil))
    assert(df.rdd.glom.collect() === Array(Array.empty[Row], Array.empty[Row]))
    testRoundtripSaveAndLoad(s"save_with_all_empty_partitions_$randomSuffix", df)
    // Now try overwriting that table. Although the new table is empty, it should still overwrite
    // the existing table.
    val df2 = df.withColumnRenamed("foo", "bar")
    testRoundtripSaveAndLoad(
      s"save_with_all_empty_partitions_$randomSuffix", df2, saveMode = SaveMode.Overwrite)
  }

  test("informative error message when saving a table with string that is longer than max length") {
    val tableName = s"error_message_when_string_too_long_$randomSuffix"
    try {
      val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row("a" * 65536))),
        StructType(StructField("A", StringType) :: Nil))
      val e = intercept[SQLException] {
        write(df)
          .option("dbtable", tableName)
          .mode(SaveMode.ErrorIfExists)
          .save()
      }
      assert(e.getMessage.contains("while loading data into Redshift"))
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
    }
  }

  test("full timestamp precision is preserved in loads (regression test for #214)") {
    val timestamps = Seq(
      TestUtils.toTimestamp(1970, 0, 1, 0, 0, 0, millis = 1),
      TestUtils.toTimestamp(1970, 0, 1, 0, 0, 0, millis = 10),
      TestUtils.toTimestamp(1970, 0, 1, 0, 0, 0, millis = 100),
      TestUtils.toTimestamp(1970, 0, 1, 0, 0, 0, millis = 1000),
      TestUtils.toNanosTimestamp(1970, 0, 1, 0, 0, 0, 100000),
      TestUtils.toNanosTimestamp(1970, 0, 1, 0, 0, 0, 10000),
      TestUtils.toNanosTimestamp(1970, 0, 1, 0, 0, 0, 1000))
    testRoundtripSaveAndLoad(
      s"full_timestamp_precision_is_preserved$randomSuffix",
      sqlContext.createDataFrame(sc.parallelize(timestamps.map(Row(_))),
        StructType(StructField("ts", TimestampType, true,
          new MetadataBuilder().putString("redshift_type", "timestamp").build()) :: Nil))
    )
  }
}

class AvroRedshiftWriteSuite extends BaseRedshiftWriteSuite {
  override protected val tempformat: String = "AVRO"

  test("informative error message when saving with column names that contain spaces (#84)") {
    intercept[IllegalArgumentException] {
      testRoundtripSaveAndLoad(
        s"error_when_saving_column_name_with_spaces_$randomSuffix",
        sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
          StructType(StructField("column name with spaces", IntegerType, true,
            new MetadataBuilder().putString("redshift_type", "int4").build()) :: Nil)))
    }
  }
}

class CSVRedshiftWriteSuite extends BaseRedshiftWriteSuite {
  override protected val tempformat: String = "CSV"

  test("save with column names that contain spaces (#84)") {
    testRoundtripSaveAndLoad(
      s"save_with_column_names_that_contain_spaces_$randomSuffix",
      sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(StructField("column name with spaces", IntegerType, true,
          new MetadataBuilder().putString("redshift_type", "int4").build()) :: Nil)))
  }

  test("roundtrip save and load struct type") {
    // does not use testRoundtripSaveAndLoad since that function cannot take a schema to load with
    withTempRedshiftTable("roundtrip_struct_save_load") { tableName =>
      val superSchema = StructType(
        StructField("string", StringType) ::
          StructField("byte", ByteType) ::
          StructField("short", ShortType) ::
          StructField("int", IntegerType) ::
          StructField("long", LongType) ::
          StructField("float", FloatType) ::
          StructField("double", DoubleType) ::
          StructField("decimal", DecimalType(2, 1)) ::
          StructField("bool", BooleanType) ::
          StructField("date", DateType) ::
          StructField("timestamp", TimestampType) ::
          StructField("map", MapType(StringType, StringType)) ::
          StructField("array", ArrayType(IntegerType)) ::
          StructField("struct", StructType(StructField("hello", StringType) :: Nil)) ::
          Nil
      )

      val dataframeSchema = StructType(StructField("a", superSchema) :: Nil)
      val byte: Byte = 1
      val short: Short = 1
      val date: Date = Date.valueOf("1970-01-01")
      val decimal: java.math.BigDecimal = new java.math.BigDecimal("2.2")
      val timestamp = Timestamp.valueOf("2019-07-01 12:01:19.000")
      val map = Map("key" -> "value")
      val array = mutable.WrappedArray.make(Array(1, 2, 3))
      val struct = Row("world")

      val expectedRow = Row(
        new GenericRowWithSchema(
          Array(
            "a", byte, short, 1, 1L, 1.1f, 2.2, decimal, true, date, timestamp, map, array, struct),
          superSchema))

      val data = sc.parallelize(Seq(expectedRow))
      val df = sqlContext.createDataFrame(data, dataframeSchema)

      write(df).option("dbtable", tableName).mode(SaveMode.Append).save()

      val actualDf = read.option("dbtable", tableName).schema(dataframeSchema).load

      checkAnswer(actualDf, Seq(expectedRow))
    }
  }

  test("roundtrip save and load array type") {
    withTempRedshiftTable("roundtrip_array_save_load") { tableName =>
      val superSchema = ArrayType(IntegerType)
      val dataframeSchema = StructType(StructField("a", superSchema) :: Nil)

      val expectedRow = Row(mutable.WrappedArray.make(Array(1, 2, 3)))
      val data = sc.parallelize(Seq(expectedRow))
      val df = sqlContext.createDataFrame(data, dataframeSchema)

      write(df).option("dbtable", tableName).mode(SaveMode.Append).save()

      val actualDf = read.option("dbtable", tableName).schema(dataframeSchema).load

      checkAnswer(actualDf, Seq(expectedRow))
    }
  }

  test("roundtrip save and load nested array type") {
    withTempRedshiftTable("roundtrip_nested_array_save_load") { tableName =>
      val superSchema = ArrayType(ArrayType(IntegerType))
      val dataframeSchema = StructType(StructField("a", superSchema) :: Nil)

      val expectedRow = Row(mutable.WrappedArray.make(Array(mutable.WrappedArray.make(Array(1)))))
      val data = sc.parallelize(Seq(expectedRow))
      val df = sqlContext.createDataFrame(data, dataframeSchema)

      write(df).option("dbtable", tableName).mode(SaveMode.Append).save()

      val actualDf = read.option("dbtable", tableName).schema(dataframeSchema).load()

      checkAnswer(actualDf, Seq(expectedRow))
    }
  }

  test("roundtrip save and load map type") {
    withTempRedshiftTable("roundtrip_map_save_load") { tableName =>
      val superSchema = MapType(StringType, StringType)
      val dataframeSchema = StructType(StructField("a", superSchema) :: Nil)

      val expectedRow = Row(Map("hello" -> "world", "key" -> "value"))
      val data = sc.parallelize(Seq(expectedRow))
      val df = sqlContext.createDataFrame(data, dataframeSchema)

      write(df).option("dbtable", tableName).mode(SaveMode.Append).save()

      val actualDf = read.option("dbtable", tableName).schema(dataframeSchema).load

      checkAnswer(actualDf, Seq(expectedRow))
    }
  }

  test("roundtrip save and load nested map type") {
    withTempRedshiftTable("roundtrip_nested_map_save_load") { tableName =>
      val superSchema = MapType(StringType, MapType(StringType, IntegerType))
      val dataframeSchema = StructType(StructField("a", superSchema) :: Nil)

      val expectedRow = Row(Map("hello" -> Map("hi" -> 1)))
      val data = sc.parallelize(Seq(expectedRow))
      val df = sqlContext.createDataFrame(data, dataframeSchema)

      write(df).option("dbtable", tableName).mode(SaveMode.Append).save()

      val actualDf = read.option("dbtable", tableName).schema(dataframeSchema).load()

      checkAnswer(actualDf, Seq(expectedRow))
    }
  }
}

class CSVGZIPRedshiftWriteSuite extends IntegrationSuiteBase {
  // Note: we purposely don't inherit from BaseRedshiftWriteSuite because we're only interested in
  // testing basic functionality of the GZIP code; the rest of the write path should be unaffected
  // by compression here.

  override protected def write(df: DataFrame): DataFrameWriter[Row] =
    super.write(df).option("tempformat", "CSV GZIP")

  test("roundtrip save and load") {
    // This test can be simplified once #98 is fixed.
    val tableName = s"roundtrip_save_and_load_$randomSuffix"
    try {
      write(
        sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData), TestUtils.testSchema))
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      checkAnswer(read.option("dbtable", tableName).load(), TestUtils.expectedData)
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
    }
  }
}

class ParquetRedshiftWriteSuite extends BaseRedshiftWriteSuite {
  override val tempformat: String = "PARQUET"

  protected val AWS_S3_CROSS_REGION_SCRATCH_SPACE: String =
    loadConfigFromEnv("AWS_S3_CROSS_REGION_SCRATCH_SPACE")

  test("save fails on cross-region copy") {
    val tableName = s"cross_region_copy_parquet_$randomSuffix"
    val caught = intercept[Exception]({
      write(
        sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData), TestUtils.testSchema))
        .option("dbtable", tableName)
        .option("tempdir", AWS_S3_CROSS_REGION_SCRATCH_SPACE)
        .mode(SaveMode.ErrorIfExists)
        .save()
    })
    assert(caught.getMessage == "Redshift cluster and S3 bucket are in different regions " +
      "when tempformat is set to parquet")

  }
}