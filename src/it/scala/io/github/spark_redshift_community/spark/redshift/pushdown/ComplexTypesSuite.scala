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
package io.github.spark_redshift_community.spark.redshift.pushdown

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.col

import java.sql.{Date, Timestamp}
import scala.collection.mutable

abstract class ComplexTypesSuite extends IntegrationPushdownSuiteBase {
  test("Unload super as StructType") {
    withTempRedshiftTable("superStruct") { tableName =>
      val superSchema = StructType(
        StructField("string", StringType)::
          StructField("byte", ByteType)::
          StructField("short", ShortType)::
          StructField("int", IntegerType)::
          StructField("long", LongType)::
          StructField("float", FloatType)::
          StructField("double", DoubleType)::
          StructField("decimal", DecimalType(2, 1))::
          StructField("bool", BooleanType)::
          StructField("date", DateType)::
          StructField("timestamp", TimestampType)::
          StructField("map", MapType(StringType, StringType))::
          StructField("array", ArrayType(IntegerType))::
          StructField("struct", StructType(StructField("hello", StringType)::Nil))::
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


      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (a super)"
      )
      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values (
           |json_parse('{
           | "string": "a",
           | "byte": 1,
           | "short": 1,
           | "int": 1,
           | "long": 1,
           | "float": 1.1,
           | "double":2.2,
           | "decimal": "2.2",
           | "bool": true,
           | "date":"$date",
           | "timestamp":"2019-07-01 12:01:19.000",
           | "map": {"key":"value"},
           | "array": [1,2,3],
           | "struct": {"hello":"world"}}'))""".stripMargin
      )

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load(),
        Seq(expectedRow)
      )
    }
  }

  test("Unload super as ArrayType") {
    withTempRedshiftTable("superArray") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (a super);"
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values (json_parse('[1, 2, 3]'));"""
      )

      val superSchema = ArrayType(IntegerType)
      val dataframeSchema = StructType(StructField("a", superSchema):: Nil)

      val expectedRow = Row(mutable.WrappedArray.make(Array(1, 2, 3)))

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load(),
        Seq(expectedRow)
      )
    }
  }

  test("Unload super as MapType") {
    withTempRedshiftTable("superMap") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (a super)"
      )
      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values (json_parse('{"hello":"world", "foo":"bar"}'))"""
      )

      val superSchema = MapType(StringType, StringType)
      val dataframeSchema = StructType(StructField("a", superSchema) ::Nil)

      val expectedRow = Row(Map("hello"->"world", "foo"->"bar"))

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load(),
        Seq(expectedRow)
      )
    }
  }

  test("GetStructField on string") {
    withTempRedshiftTable("superStructWithString") { tableName =>
      val firstString = "world"
      val secondString = """{"nested":"value"}"""
      val expectedRows = Seq(Row(firstString), Row(secondString))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values
           | (json_parse('{"hello": "$firstString"}')),
           | (json_parse('{"hello": $secondString}'))""".stripMargin
      )

      val superSchema = StructType(StructField("hello", StringType)::Nil)
      val dataframeSchema = StructType(StructField("a", superSchema)::Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().select("a.hello"),
        expectedRows
      )
    }
  }

  test("GetStructField on string in where") {
    withTempRedshiftTable("superStructWithStringWhere") { tableName =>
      val firstString = "world"
      val secondString = """{"nested":"value"}"""
      val expectedRows = Seq(Row(secondString))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values
           | (json_parse('{"hello": "$firstString"}')),
           | (json_parse('{"hello": $secondString}'))""".stripMargin
      )

      val superSchema = StructType(StructField("hello", StringType) :: Nil)
      val dataframeSchema = StructType(StructField("a", superSchema) :: Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().
          select("a.hello").where(col("a.hello") === secondString),
        expectedRows
      )
    }
  }

  test("GetStructField on byte") {
    withTempRedshiftTable("superStructWithByte") { tableName =>
      val byte: Byte = 1
      val expectedRow = Seq(Row(byte))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values (json_parse('{"byte":1}'))"""
      )

      val superSchema = StructType(StructField("byte", ByteType)::Nil)
      val dataframeSchema = StructType(StructField("a", superSchema)::Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().select("a.byte"),
        expectedRow
      )
    }
  }

  test("GetStructField on byte in where") {
    withTempRedshiftTable("superStructWithByte") { tableName =>
      val byte: Byte = 1
      val expectedRow = Seq(Row(byte))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values
           | (json_parse('{"byte":1}')),
           | (json_parse('{"byte":2}'))""".stripMargin
      )

      val superSchema = StructType(StructField("byte", ByteType) :: Nil)
      val dataframeSchema = StructType(StructField("a", superSchema) :: Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().
          select("a.byte").where(col("a.byte") === byte),
        expectedRow
      )
    }
  }

  test("GetStructField on short") {
    withTempRedshiftTable("superStructWithShort") { tableName =>
      val short: Short = 1
      val expectedRow = Seq(Row(short))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values (json_parse('{"short": 1}'))"""
      )

      val superSchema = StructType(StructField("short", ShortType)::Nil)
      val dataframeSchema = StructType(StructField("a", superSchema)::Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().select("a.short"),
        expectedRow
      )
    }
  }

  test("GetStructField on short in where") {
    withTempRedshiftTable("superStructWithShort") { tableName =>
      val short: Short = 1
      val expectedRow = Seq(Row(short))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values
           |(json_parse('{"short": 1}')),
           |(json_parse('{"short": 2}'))""".stripMargin
      )

      val superSchema = StructType(StructField("short", ShortType) :: Nil)
      val dataframeSchema = StructType(StructField("a", superSchema) :: Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().
          select("a.short").where(col("a.short") === short),
        expectedRow
      )
    }
  }

  test("GetStructField on int") {
    withTempRedshiftTable("superStructWithInt") { tableName =>
      val expectedRow = Seq(Row(1))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values (json_parse('{"int": 1}'))"""
      )

      val superSchema = StructType(StructField("int", IntegerType)::Nil)
      val dataframeSchema = StructType(StructField("a", superSchema)::Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().select("a.int"),
        expectedRow
      )
    }
  }

  test("GetStructField on int in where") {
    withTempRedshiftTable("superStructWithInt") { tableName =>
      val int = 1
      val expectedRow = Seq(Row(int))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values
           |(json_parse('{"int": 1}')),
           |(json_parse('{"int": 2}'))""".stripMargin
      )

      val superSchema = StructType(StructField("int", IntegerType) :: Nil)
      val dataframeSchema = StructType(StructField("a", superSchema) :: Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().
          select("a.int").where(col("a.int") === int),
        expectedRow
      )
    }
  }

  test("GetStructField on long") {
    withTempRedshiftTable("superStructWithLong") { tableName =>
      val expectedRow = Seq(Row(1L))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values (json_parse('{"long": 1}'))"""
      )

      val superSchema = StructType(StructField("long", LongType)::Nil)
      val dataframeSchema = StructType(StructField("a", superSchema)::Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().select("a.long"),
        expectedRow
      )
    }
  }

  test("GetStructField on long in where") {
    withTempRedshiftTable("superStructWithLong") { tableName =>
      val expectedRow = Seq(Row(1L))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values
           |(json_parse('{"long": 1}')),
           |(json_parse('{"long": 2}'))""".stripMargin
      )

      val superSchema = StructType(StructField("long", LongType) :: Nil)
      val dataframeSchema = StructType(StructField("a", superSchema) :: Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().
          select("a.long").where(col("a.long") === 1L),
        expectedRow
      )
    }
  }

  test("GetStructField on float") {
    withTempRedshiftTable("superStructWithFloat") { tableName =>
      val expectedRow = Seq(Row(2.2f))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values (json_parse('{"float": 2.2}'))"""
      )

      val superSchema = StructType(StructField("float", FloatType)::Nil)
      val dataframeSchema = StructType(StructField("a", superSchema)::Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().select("a.float"),
        expectedRow
      )
    }
  }

  test("GetStructField on float in where") {
    // uses a.float < 2.3f instead of equal because of [Redshift-9668]
    withTempRedshiftTable("superStructWithFloat") { tableName =>
      val expectedRow = Seq(Row(2.2f))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values
           |(json_parse('{"float": 2.2}')),
           |(json_parse('{"float": 3.3}'))""".stripMargin
      )

      val superSchema = StructType(StructField("float", FloatType) :: Nil)
      val dataframeSchema = StructType(StructField("a", superSchema) :: Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().
          select("a.float").where(col("a.float") < 2.3f),
        expectedRow
      )
    }
  }

  test("GetStructField on double") {
    withTempRedshiftTable("superStructWithDouble") { tableName =>
      val expectedRow = Seq(Row(2.2))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values (json_parse('{"double":2.2}'))"""
      )

      val superSchema = StructType(StructField("double", DoubleType)::Nil)
      val dataframeSchema = StructType(StructField("a", superSchema)::Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().select("a.double"),
        expectedRow
      )
    }
  }

  test("GetStructField on double in where") {
    withTempRedshiftTable("superStructWithDouble") { tableName =>
      val expectedRow = Seq(Row(2.2))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values
           |(json_parse('{"double":2.2}')),
           |(json_parse('{"double":3.3}'))""".stripMargin
      )

      val superSchema = StructType(StructField("double", DoubleType) :: Nil)
      val dataframeSchema = StructType(StructField("a", superSchema) :: Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().
          select("a.double").where(col("a.double") === 2.2),
        expectedRow
      )
    }
  }

  test("GetStructField on decimal") {
    withTempRedshiftTable("superStructWithDecimal") { tableName =>
      val expectedRow = Seq(Row(new java.math.BigDecimal("2.2")))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values (json_parse('{"decimal": "2.2"}'))"""
      )

      val superSchema = StructType(StructField("decimal", DecimalType(2, 1))::Nil)
      val dataframeSchema = StructType(StructField("a", superSchema)::Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().select("a.decimal"),
        expectedRow
      )
    }
  }

  test("GetStructField on decimal in where") {
    withTempRedshiftTable("superStructWithDecimal") { tableName =>
      val expectedDecimal = new java.math.BigDecimal("2.2")
      val expectedRow = Seq(Row(expectedDecimal))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values
           |(json_parse('{"decimal": "2.2"}')),
           |(json_parse('{"decimal": "3.3"}'))""".stripMargin
      )

      val superSchema = StructType(StructField("decimal", DecimalType(2, 1)) :: Nil)
      val dataframeSchema = StructType(StructField("a", superSchema) :: Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().
          select("a.decimal").where(col("a.decimal") === expectedDecimal),
        expectedRow
      )
    }
  }

  test("GetStructField on boolean") {
    withTempRedshiftTable("superStructWithBoolean") { tableName =>
      val expectedRow = Seq(Row(true))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values (json_parse('{"boolean": true}'))"""
      )

      val superSchema = StructType(StructField("boolean", BooleanType)::Nil)
      val dataframeSchema = StructType(StructField("a", superSchema)::Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().select("a.boolean"),
        expectedRow
      )
    }
  }

  test("GetStructField on boolean in where") {
    withTempRedshiftTable("superStructWithBoolean") { tableName =>
      val expectedRow = Seq(Row(true))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values
           |(json_parse('{"boolean": true}')),
           |(json_parse('{"boolean": false}'))""".stripMargin
      )

      val superSchema = StructType(StructField("boolean", BooleanType) :: Nil)
      val dataframeSchema = StructType(StructField("a", superSchema) :: Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().
          select("a.boolean").where(col("a.boolean")),
        expectedRow
      )
    }
  }

  test("GetStructField on date") {
    withTempRedshiftTable("superStructWithDate") { tableName =>
      val date: Date = Date.valueOf("1970-01-01")
      val expectedRow = Seq(Row(date))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values (json_parse('{"date": "$date"}'))"""
      )

      val superSchema = StructType(StructField("date", DateType)::Nil)
      val dataframeSchema = StructType(StructField("a", superSchema)::Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().select("a.date"),
        expectedRow
      )
    }
  }

  test("GetStructField on date in where") {
    withTempRedshiftTable("superStructWithDate") { tableName =>
      val date: Date = Date.valueOf("1970-01-01")
      val otherDate: Date = Date.valueOf("1975-01-01")
      val expectedRow = Seq(Row(date))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values
           |(json_parse('{"date": "$date"}')),
           |(json_parse('{"date": "$otherDate"}'))""".stripMargin
      )

      val superSchema = StructType(StructField("date", DateType) :: Nil)
      val dataframeSchema = StructType(StructField("a", superSchema) :: Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().
          select("a.date").where(col("a.date") === date),
        expectedRow
      )
    }
  }

  test("GetStructField on timestamp") {
    withTempRedshiftTable("superStructWithTimestamp") { tableName =>
      val timestamp = Timestamp.valueOf("2019-07-01 12:01:19.000")
      val expectedRow = Seq(Row(timestamp))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values (json_parse('{"timestamp":"2019-07-01 12:01:19.000"}'))"""
      )

      val superSchema = StructType(StructField("timestamp", TimestampType)::Nil)
      val dataframeSchema = StructType(StructField("a", superSchema) :: Nil)

      checkAnswer(
        read.
        schema(dataframeSchema).option("dbtable", tableName).load().select("a.timestamp"),
        expectedRow
      )
    }
  }

  test("GetStructField on timestamp in where") {
    withTempRedshiftTable("superStructWithTimestamp") { tableName =>
      val timestamp = Timestamp.valueOf("2019-07-01 12:01:19.000")
      val expectedRow = Seq(Row(timestamp))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values
           |(json_parse('{"timestamp":"2019-07-01 12:01:19.000"}')),
           |(json_parse('{"timestamp":"2020-07-01 12:01:19.000"}'))""".stripMargin
      )

      val superSchema = StructType(StructField("timestamp", TimestampType) :: Nil)
      val dataframeSchema = StructType(StructField("a", superSchema) :: Nil)

      checkAnswer(
        read.
          schema(dataframeSchema).option("dbtable", tableName).load().
          select("a.timestamp").where(col("a.timestamp") === timestamp),
        expectedRow
      )
    }
  }

  test("GetStructField on map") {
    withTempRedshiftTable("superStructWithMap") { tableName =>
      val expectedRow = Seq(Row(Map("key" -> "value")))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values (json_parse('{"map":{"key":"value"}}'))"""
      )

      val superSchema = StructType(StructField("map", MapType(StringType, StringType))::Nil)
      val dataframeSchema = StructType(StructField("a", superSchema)::Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().select("a.map"),
        expectedRow
      )
    }
  }

  test("GetStructField on array") {
    withTempRedshiftTable("superStructWithArray") { tableName =>
      val expectedRow = Seq(Row(mutable.WrappedArray.make(Array(1, 2, 3))))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values (json_parse('{"array": [1,2,3]}'))"""
      )

      val superSchema = StructType(StructField("array", ArrayType(IntegerType))::Nil)
      val dataframeSchema = StructType(StructField("a", superSchema)::Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().select("a.array"),
        expectedRow
      )
    }
  }

  test("GetStructField on array in where") {
    // this is not able to be pushed down as array literals are unhandled in BasicStatement.scala
    withTempRedshiftTable("superStructWithArray") { tableName =>
      val expectedRow = Seq(Row(mutable.WrappedArray.make(Array(1, 2, 3))))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values (json_parse('{"array": [1,2,3]}'))"""
      )

      val superSchema = StructType(StructField("array", ArrayType(IntegerType)) :: Nil)
      val dataframeSchema = StructType(StructField("a", superSchema) :: Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load()
          .select("a.array").where(col("a.array") === mutable.WrappedArray.make(Array(1, 2, 3))),
        expectedRow
      )
    }
  }

  test("GetStructField on struct") {
    withTempRedshiftTable("superStructWithStruct") { tableName =>
      val expectedRow = Seq(Row(Row("one", 2)))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values
           |(json_parse('{"struct": {"first":"one", "second":2}}'))""".stripMargin
      )

      val superSchema = StructType(StructField("struct", StructType(
        StructField("first", StringType)::StructField("second", IntegerType)::Nil))::Nil)
      val dataframeSchema = StructType(StructField("a", superSchema)::Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().select("a.struct"),
        expectedRow
      )
    }
  }

  test("GetStructField on struct in where") {
    // this is not able to be pushed down as struct literals are unhandled in BasicStatement.scala
    withTempRedshiftTable("superStructWithStructInWhere") { tableName =>
      val expectedRow = Seq(Row(Row("one", 2)))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values
           |(json_parse('{"struct": {"first":"one", "second":2}}')),
           |(json_parse('{"struct": {"first":"notone", "second":2}}'))""".stripMargin
      )

      val superSchema = StructType(StructField("struct", StructType(
        StructField("first", StringType)::StructField("second", IntegerType)::Nil))::Nil)
      val dataframeSchema = StructType(StructField("a", superSchema)::Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().select("a.struct")
          .where("a.struct = named_struct('first', 'one', 'second', 2)"),
        expectedRow
      )
    }
  }

  test("GetStructField on nested struct field") {
    withTempRedshiftTable("superStructWithNestedStructField") { tableName =>
      val expectedRow = Seq(Row("one"))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (a super)"""
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values
           |(json_parse('{"struct": {"first":"one"}}'))""".stripMargin
      )

      val superSchema = StructType(StructField("struct", StructType(
        StructField("first", StringType)::Nil))::Nil)
      val dataframeSchema = StructType(StructField("a", superSchema)::Nil)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().select("a.struct.first"),
        expectedRow
      )
    }
  }

  test("GetArrayItem with literal") {
    withTempRedshiftTable("getArrayItemWithLiteral") { tableName =>
      val byteIndex: Byte = 1;
      val shortIndex: Short = 1;
      val intIndex: Int = 1;

      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (a super);"
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values (json_parse('[1, 2, 3]'));"""
      )

      val superSchema = ArrayType(IntegerType)
      val dataframeSchema = StructType(StructField("a", superSchema) :: Nil)

      val expectedRow = Row(2)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).
          load().select(col("a").getItem(byteIndex)),
        Seq(expectedRow)
      )

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).
          load().select(col("a").getItem(shortIndex)),
        Seq(expectedRow)
      )

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).
          load().select(col("a").getItem(intIndex)),
        Seq(expectedRow)
      )
    }
  }

  test("GetArrayItem with integer expression") {
    withTempRedshiftTable("getArrayItemWithExpression") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (a super);"
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values (json_parse('[1, 2, 3]'));"""
      )

      val superSchema = ArrayType(IntegerType)
      val dataframeSchema = StructType(StructField("a", superSchema) :: Nil)

      val expectedRow = Row(3)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).
          load().selectExpr("a[a[1]]"),
        Seq(expectedRow)
      )
    }
  }

  test("GetArrayItem in where"){
    withTempRedshiftTable("getArrayItemWithExpression") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (a super);"
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values (json_parse('[1, 2, 3]'));"""
      )

      val superSchema = ArrayType(IntegerType)
      val dataframeSchema = StructType(StructField("a", superSchema) :: Nil)

      val expectedRow = Row(1)

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).
          load().selectExpr("a[0]").where("a[0] = 1"),
        Seq(expectedRow)
      )
    }
  }

  test("GetMapValue with literal") {
    withTempRedshiftTable("getMapValueLiteral") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (a super);"
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values (json_parse('{"hello": "world"}'));"""
      )

      val superSchema = MapType(StringType, StringType)
      val dataframeSchema = StructType(StructField("a", superSchema)::Nil)

      val expectedRow = Seq(Row("world"))

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).
          load().selectExpr("a['hello']"),
        expectedRow
      )
    }
  }

  test("GetMapValue in where") {
    withTempRedshiftTable("getMapValueInWhere") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (a super);"
      )

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values (json_parse('{"hello": "world"}'));"""
      )

      val superSchema = MapType(StringType, StringType)
      val dataframeSchema = StructType(StructField("a", superSchema)::Nil)

      val expectedRow = Seq(Row("world"))

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).
          load().selectExpr("a['hello']").where("a['hello'] = 'world'"),
        expectedRow
      )
    }
  }
}

class TextComplexTypesSuite extends ComplexTypesSuite {
  override protected val s3format: String = "TEXT"
}

class NoPushdownTextComplexTypesSuite extends TextComplexTypesSuite {
  override protected val auto_pushdown: String = "false"
}

class ParquetComplexTypesSuite extends ComplexTypesSuite {
  override protected val s3format: String = "PARQUET"
}

class NoPushdownParquetComplexTypesSuite extends ParquetComplexTypesSuite {
  override protected val auto_pushdown: String = "false"
}
