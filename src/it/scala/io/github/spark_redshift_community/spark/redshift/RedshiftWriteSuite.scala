/*
 * Copyright 2015 Databricks
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

import java.sql.SQLException
import org.apache.spark.sql._
import org.apache.spark.sql.types._

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

      assert(redshiftWrapper.tableExists(conn, tableName))
      checkAnswer(read.option("dbtable", tableName).load(), TestUtils.expectedData)
    } finally {
      redshiftWrapper.executeUpdate(conn, s"drop table if exists $tableName")
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

  test("error when saving a table with string that is longer than max length") {
    val tableName = s"error_when_string_too_long_$randomSuffix"
    try {
      val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row("a" * 65536))),
        StructType(StructField("A", StringType) :: Nil))
      val e = intercept[Exception] {
        write(df)
          .option("dbtable", tableName)
          .mode(SaveMode.ErrorIfExists)
          .save()
      }
    } finally {
      redshiftWrapper.executeUpdate(conn, s"drop table if exists $tableName")
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

  test("save with column values that contain spaces are preserved by default") {
    // This test can be simplified once #98 is fixed.
    val tableName = s"save_with_column_values_with_spaces_$randomSuffix"
    try {
      val df = Seq(Row("  test    "))
      write(
        sqlContext.createDataFrame(
          sc.parallelize(df),
          StructType(Seq(StructField("teststring", StringType)))))
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(redshiftWrapper.tableExists(conn, tableName))
      checkAnswer(read.option("dbtable", tableName).load(), df)
    } finally {
      redshiftWrapper.executeUpdate(conn, s"drop table if exists $tableName")
    }
  }

  test ("Check ShortType Mapping to SMALLINT Test") {
    withTempRedshiftTable("myAllTypeTable") { tableName =>
      // val tableName: String = "myAllTypeTable"
      val testSchema: StructType = {
        StructType(Seq(
          StructField("col_byte", ByteType, nullable = true),
          StructField("col_short", ShortType, nullable = true),
          StructField("col_integer", IntegerType, nullable = true),
          StructField("col_long", LongType, nullable = true)
        )
        )
      }

      val dataRow = Seq(Row(Byte.MaxValue, Short.MaxValue, Integer.MAX_VALUE, Long.MaxValue))
      val data = sc.parallelize((dataRow))
      val df = sqlContext.createDataFrame(data, testSchema)

      write(df).option("dbtable", tableName)
        .option(Parameters.PARAM_LEGACY_MAPPING_SHORT_TO_INT, value = false)
        .save()
      if (!redshiftWrapper.tableExists(conn, tableName)) {
        Thread.sleep(1000)
        assert(redshiftWrapper.tableExists(conn, tableName))
      }
      val loadedDf = read.option("dbtable", tableName)
        .option(Parameters.PARAM_LEGACY_MAPPING_SHORT_TO_INT, value = false)
        .load()

      // loadedDf.schema.fields.foreach(elem => println(elem))
      assert(loadedDf.schema match {
        case StructType(Array(StructField(_, ShortType, _, _),
        StructField(_, ShortType, _, _),
        StructField(_, IntegerType, _, _),
        StructField(_, LongType, _, _))) => true
        case _ => false
      })
    }
  }
  test ("Check ShortType Mapping to INTEGER Test") {
    withTempRedshiftTable("myAllTypeTable") { tableName =>
      // val tableName: String = "myAllTypeTable"
      val testSchema: StructType = {
        StructType(Seq(
          StructField("col_byte", ByteType, nullable = true),
          StructField("col_short", ShortType, nullable = true),
          StructField("col_integer", IntegerType, nullable = true),
          StructField("col_long", LongType, nullable = true)
        )
        )
      }

      val dataRow = Seq(Row(Byte.MaxValue, Short.MaxValue, Integer.MAX_VALUE, Long.MaxValue))
      val data = sc.parallelize((dataRow))
      val df = sqlContext.createDataFrame(data, testSchema)

      write(df).option("dbtable", tableName)
        .option(Parameters.PARAM_LEGACY_MAPPING_SHORT_TO_INT, value = true)
        .save()
      if (!redshiftWrapper.tableExists(conn, tableName)) {
        Thread.sleep(1000)
        assert(redshiftWrapper.tableExists(conn, tableName))
      }
      val loadedDf = read.option("dbtable", tableName)
        .option(Parameters.PARAM_LEGACY_MAPPING_SHORT_TO_INT, value = true)
        .load()

      // loadedDf.schema.fields.foreach(elem => println(elem))
      assert(loadedDf.schema match {
        case StructType(Array(StructField(_, IntegerType, _, _),
        StructField(_, IntegerType, _, _),
        StructField(_, IntegerType, _, _),
        StructField(_, LongType, _, _))) => true
        case _ => false
      })
    }
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

trait NonAvroRedshiftWriteSuite extends IntegrationSuiteBase {
  test("save with column names that contain spaces (#84)") {
    testRoundtripSaveAndLoad(
      s"save_with_column_names_that_contain_spaces_$randomSuffix",
      sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(StructField("column name with spaces", IntegerType, true,
          new MetadataBuilder().putString("redshift_type", "int4").build()) :: Nil)))
  }
}

class CSVRedshiftWriteSuite extends BaseRedshiftWriteSuite
  with ComplexWriteSuite with NonAvroRedshiftWriteSuite{
  override protected val tempformat: String = "CSV"

  test("do not trim column values when legacy_trim_csv_writes option is false") {
    val tableName = s"save_with_column_values_with_spaces_$randomSuffix"
    try {
      val df = Seq(Row("  test    "))
      write(
        sqlContext.createDataFrame(
          sc.parallelize(df),
          StructType(Seq(StructField("teststring", StringType)))))
        .option("dbtable", tableName)
        .option(Parameters.PARAM_LEGACY_TRIM_CSV_WRITES, value = false)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(redshiftWrapper.tableExists(conn, tableName))
      checkAnswer(read.option("dbtable", tableName).load(), df)
    } finally {
      redshiftWrapper.executeUpdate(conn, s"drop table if exists $tableName")
    }
  }

  test("trim column values when legacy_trim_csv_writes option is true") {
    val tableName = s"save_with_column_values_with_spaces_$randomSuffix"
    try {
      val df = Seq(Row("  test    "))
      val trimmed_df = Seq(Row("test"))
      write(
        sqlContext.createDataFrame(
          sc.parallelize(df),
          StructType(Seq(StructField("teststring", StringType)))))
        .option("dbtable", tableName)
        .option(Parameters.PARAM_LEGACY_TRIM_CSV_WRITES, value = true)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(redshiftWrapper.tableExists(conn, tableName))
      checkAnswer(read.option("dbtable", tableName).load(), trimmed_df)
    } finally {
      redshiftWrapper.executeUpdate(conn, s"drop table if exists $tableName")
    }
  }
}

class CSVGZIPRedshiftWriteSuite extends CSVRedshiftWriteSuite with NonAvroRedshiftWriteSuite{
  override protected def write(df: DataFrame): DataFrameWriter[Row] =
    super.write(df).option("tempformat", "CSV GZIP")
}

class ParquetRedshiftWriteSuite extends BaseRedshiftWriteSuite
  with ComplexWriteSuite with NonAvroRedshiftWriteSuite{
  override val tempformat: String = "PARQUET"

  protected val AWS_S3_CROSS_REGION_SCRATCH_SPACE: String =
    loadConfigFromEnv("AWS_S3_CROSS_REGION_SCRATCH_SPACE")

  protected val AWS_S3_CROSS_REGION_SCRATCH_SPACE_REGION: String =
    loadConfigFromEnv("AWS_S3_CROSS_REGION_SCRATCH_SPACE_REGION")


  test("save fails on cross-region copy") {
    val tableName = s"cross_region_copy_parquet_$randomSuffix"
    val caught = intercept[Exception]({
      write(
        sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData), TestUtils.testSchema))
        .option("dbtable", tableName)
        .option("tempdir", AWS_S3_CROSS_REGION_SCRATCH_SPACE)
        .option("tempdir_region", AWS_S3_CROSS_REGION_SCRATCH_SPACE_REGION)
        .mode(SaveMode.ErrorIfExists)
        .save()
    })
    assert(caught.getMessage == "Redshift cluster and S3 bucket are in different regions " +
      "when tempformat is set to parquet")
  }
}
