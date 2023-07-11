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
package io.github.spark_redshift_community.spark.redshift

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}

import java.sql.{Date, Timestamp}
import scala.collection.mutable

trait ComplexWriteSuite extends IntegrationSuiteBase {
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