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
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{BooleanType, DateType, IntegerType, ShortType, StringType, StructField, StructType, TimestampType}

import java.sql.{Date, Timestamp}

abstract class PushdownMiscSuite extends IntegrationPushdownSuiteBase {

  test("Boolean subquery cast to String") {
    checkAnswer(
      sqlContext.sql(
        s"""SELECT CAST((SELECT testbool from test_table WHERE testbool = true) AS STRING)""".stripMargin),
      Seq(Row("true")))

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTBOOL" ) AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM (
         | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
         | ( ( "SUBQUERY_0"."TESTBOOL" IS NOT NULL ) AND "SUBQUERY_0"."TESTBOOL" ) ) AS
         | "SUBQUERY_1"""".stripMargin
    )
  }

  test("Casting true boolean value to string") {
    checkAnswer(
      sqlContext.sql(
        s"""SELECT CAST(testbool AS STRING) FROM test_table WHERE testbool = true limit 1""".stripMargin),
      Seq(Row("true")))

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT ( CASE "SUBQUERY_1"."TESTBOOL" WHEN TRUE THEN \\'true\\'
         | WHEN FALSE THEN \\'false\\' ELSE null END ) AS "SUBQUERY_2_COL_0" FROM
         | ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTBOOL" IS NOT NULL ) AND
         | "SUBQUERY_0"."TESTBOOL" ) ) AS "SUBQUERY_1" ) AS "SUBQUERY_2" LIMIT 1""".stripMargin
    )
  }

  test("Casting false boolean value to string") {
    checkAnswer(
      sqlContext.sql(
        s"""SELECT CAST(testbool AS STRING) FROM test_table WHERE testbool = false limit 1""".stripMargin),
      Seq(Row("false")))

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT ( CASE "SUBQUERY_1"."TESTBOOL" WHEN TRUE THEN \\'true\\'
         | WHEN FALSE THEN \\'false\\' ELSE null END ) AS "SUBQUERY_2_COL_0" FROM
         | ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTBOOL" IS NOT NULL ) AND NOT
         | ( "SUBQUERY_0"."TESTBOOL" ) ) ) AS "SUBQUERY_1" ) AS "SUBQUERY_2" LIMIT 1""".stripMargin
    )
  }

  test("Casting null boolean value to string") {
    checkAnswer(
      sqlContext.sql(
        s"""SELECT CAST(testbool AS STRING) FROM test_table WHERE testbool is null limit 1""".stripMargin),
      Seq(Row(null)))

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT ( CASE "SUBQUERY_1"."TESTBOOL" WHEN TRUE THEN \\'true\\' WHEN FALSE
        | THEN \\'false\\' ELSE null END ) AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
        | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE ( "SUBQUERY_0"."TESTBOOL"
        | IS NULL ) ) AS "SUBQUERY_1" ) AS "SUBQUERY_2" LIMIT 1""".stripMargin
    )
  }

  test("Boolean expression cast to String") {
    checkAnswer(
      sqlContext.sql(
        s"""SELECT CAST(testint > testfloat AS STRING) FROM test_table WHERE testbool = true""".stripMargin),
      Seq(Row("true")))

    checkSqlStatement(
      s"""SELECT ( CASE ( CAST ( "SUBQUERY_1"."TESTINT" AS FLOAT4 ) > "SUBQUERY_1"."TESTFLOAT" )
         | WHEN TRUE THEN \\'true\\' WHEN FALSE THEN \\'false\\' ELSE null END ) AS "SUBQUERY_2_COL_0"
         | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         | WHERE ( ( "SUBQUERY_0"."TESTBOOL" IS NOT NULL ) AND "SUBQUERY_0"."TESTBOOL" ) ) AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("Non-boolean type casts to Strings") {
    // (id, column, result)
    val paramTuples = List(
      ("testbyte", "1"),
      ("testdate", "2015-07-01"),
      ("testint", "42"),
      ("testlong", "1239012341823719"),
      ("testshort", "23"),
      ("testtimestamp", "2015-07-01 00:00:00.001")
    )

    paramTuples.par.foreach(paramTuple => {
      val column = paramTuple._1
      val result = paramTuple._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT CAST($column AS STRING) FROM test_table WHERE testbool = true""".stripMargin),
        Seq(Row(result)))

      checkSqlStatement(
        s"""SELECT ( CAST ( "SUBQUERY_1"."${column.toUpperCase}" AS VARCHAR ) )
           | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table
           | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE ( (
           | "SUBQUERY_0"."TESTBOOL" IS NOT NULL ) AND "SUBQUERY_0"."TESTBOOL" ) )
           | AS "SUBQUERY_1"""".stripMargin
      )
    })
  }

  // According to OptimizeIn rule,
  // If the size of IN values list is larger than the predefined threshold,
  // Spark Optimizer replaces IN operator with INSET operator
  test("Test Integer type with InSet Operator") {

    checkAnswer(sqlContext.sql(
      s"""SELECT testint FROM test_table WHERE testint
         | IN (42, 4141214, 1, 2, 2, 4, 5, 5, 7, 8, 9, 10, 11, 12, 13)"""
        .stripMargin), Seq(Row(4141214), Row(42), Row(42))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT
         | * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |  AS "SUBQUERY_0" WHERE "SUBQUERY_0"."TESTINT" IN
         | ( 4 , 13 , 8 , 9 , 10 , 7 , 2 , 42 , 11 , 1 , 4141214 , 12 , 5 ) )
         |  AS "SUBQUERY_1"""".stripMargin)
  }

  test("Test basic CreateNamedStruct") {
    val innerSchema = StructType(StructField("testString", StringType) ::
      StructField("testInt", IntegerType) :: Nil)
    val outerSchema = StructType(
      StructField("named_struct(testString, testString, testInt, testInt)",
        innerSchema, nullable = false) :: Nil)
    val expectedRow =
      Seq(new GenericRowWithSchema(
        Array(new GenericRowWithSchema(Array("f", 4141214), innerSchema)), outerSchema))

    checkAnswer(sqlContext.sql(
      s"""SELECT NAMED_STRUCT('a', teststring, 'b', testint)
         | FROM test_table WHERE testint = 4141214""".stripMargin),
      expectedRow)

    checkSqlStatement(
      s"""SELECT ( OBJECT( \\'a\\', "SUBQUERY_1" . "TESTSTRING",
         |                 \\'b\\', "SUBQUERY_1" . "TESTINT" ) )
         | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table
         | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0" .
         | "TESTINT" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTINT" = 4141214 ) ) )
         | AS "SUBQUERY_1"""".stripMargin)
  }

  test("Test basic CreateNamedStruct All types except DATE and TIMESTAMP") {
    val innerSchema = StructType(
      StructField("testbyte", ShortType) ::
        StructField("testbool", BooleanType) ::
        StructField("testdouble", StringType) ::
        StructField("testfloat", StringType) ::
        StructField("testint", StringType) ::
        StructField("testlong", StringType) ::
        StructField("testshort", IntegerType) ::
        StructField("testString", StringType) :: Nil)
    val outerSchema = StructType(
      StructField("named_struct(testbyte, testbyte, testbool, testbool, " +
        "testdouble, testdouble, testfloat, testfloat, " +
        "testint, testint, testlong, testlong, testshort, testshort," +
        "teststring, teststring)",
        innerSchema, nullable = false) :: Nil)
    val expectedRow =
      Seq(new GenericRowWithSchema(
        Array(new GenericRowWithSchema(Array(0, null, 0.0, -1.0, 4141214,
          1239012341823719L, null, "f"), innerSchema)), outerSchema))

    checkAnswer(sqlContext.sql(
      s"""select named_struct('a', testbyte, 'b', testbool, 'd', testdouble,
         | 'e', testfloat, 'f', testint, 'g', testlong, 'h', testshort,
         | 'i', teststring)
         | from test_table where testint = 4141214""".stripMargin),
      expectedRow)

    checkSqlStatement(
      s"""SELECT ( OBJECT( \\'a\\' , "SUBQUERY_1"."TESTBYTE" , \\'b\\' , "SUBQUERY_1"."TESTBOOL",
         | \\'d\\' , "SUBQUERY_1"."TESTDOUBLE" , \\'e\\' , "SUBQUERY_1"."TESTFLOAT" ,
         | \\'f\\' , "SUBQUERY_1"."TESTINT" , \\'g\\' , "SUBQUERY_1"."TESTLONG" ,
         | \\'h\\' , "SUBQUERY_1"."TESTSHORT" , \\'i\\' , "SUBQUERY_1"."TESTSTRING" ) )
         |  AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |  AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE (
         |  ( "SUBQUERY_0"."TESTINT" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTINT" = 4141214 ) ) )
         |   AS "SUBQUERY_1"""".stripMargin)
  }

  test("Negative Test CreateNamedStruct: DATE type not convertible to super") {
    val innerSchema = StructType(StructField("testdate", DateType) ::
      StructField("testint", IntegerType) ::Nil)
    val outerSchema = StructType(
      StructField("named_struct(testdate, testdate, testint, testint)",
        innerSchema, nullable = false) :: Nil)
    val expectedRow =
      Seq(new GenericRowWithSchema(
        Array(new GenericRowWithSchema(Array(Date.valueOf("2015-07-03"), 4141214), innerSchema)),
        outerSchema))

    checkAnswer(sqlContext.sql(
         s"""select named_struct('a', testdate, 'b', testint) from test_table
         |where testint = 4141214""".stripMargin),
      expectedRow)

    checkSqlStatement(
      s"""SELECT "testdate", "testint" FROM $test_table WHERE "testint" IS NOT NULL AND
         |"testint" = 4141214""".stripMargin)
  }

  test("Negative Test CreateNamedStruct: TIMESTAMP type not convertible to super") {
    val innerSchema = StructType(StructField("testtimestamp", TimestampType) :: Nil)
    val outerSchema = StructType(
      StructField("named_struct(testtimestamp, testtimestamp)",
        innerSchema, nullable = false) :: Nil)
    val expectedRow =
      Seq(new GenericRowWithSchema(
        Array(new GenericRowWithSchema(Array(Timestamp.valueOf("2015-07-03 12:34:56")),
          innerSchema)), outerSchema))

    checkAnswer( sqlContext.sql(
        s"""select named_struct('a', testtimestamp) from test_table
           | where testint = 4141214""".stripMargin), expectedRow)

    checkSqlStatement(
      s"""SELECT "testtimestamp" FROM $test_table WHERE "testint"
         | IS NOT NULL AND "testint" = 4141214""".stripMargin)
  }

  test("Test CreateNamedStruct with inner select") {
    val innerSchema = StructType(StructField("testString", StringType) ::
      StructField("testShort", ShortType) :: Nil)
    val outerSchema = StructType(
      StructField("named_struct(testString, testString, testShort, testShort)",
        innerSchema, nullable = false) :: Nil)
    val expectedData = Seq(
      ("Unicode's樂趣", 24),
      ("___|_123", 24),
      ("asdf", 24),
      ("f", 24),
      (null, 24)
    )
    val expectedRows = expectedData.map { case (str, num) =>
      new GenericRowWithSchema(
        Array(new GenericRowWithSchema(Array(str, num), innerSchema)),
        outerSchema
      )
    }

    checkAnswer(sqlContext.sql(
      s"""SELECT NAMED_STRUCT('a', teststring, 'b', (SELECT MAX(testshort)
         | FROM test_table)) FROM test_table""".stripMargin),
      expectedRows)

    checkSqlStatement(
      s"""SELECT ( OBJECT( \\'a\\' , "SUBQUERY_0"."TESTSTRING" ,
         |                 \\'b\\' , ( SELECT ( MAX ( "SUBQUERY_1"."SUBQUERY_1_COL_0" )
         | ) AS "SUBQUERY_2_COL_0" FROM ( SELECT ( "SUBQUERY_0"."TESTSHORT" )
         | AS "SUBQUERY_1_COL_0" FROM ( SELECT * FROM $test_table AS
         | "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1" LIMIT 1 ) ) )
         | AS "SUBQUERY_1_COL_0" FROM ( SELECT * FROM $test_table AS
         | "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"""".stripMargin)
  }

  test("Test nested CreateNamedStructs") {
    val innerSchema2 = StructType(StructField("testInt", IntegerType) :: Nil)
    val innerSchema = StructType(StructField("named_struct(testInt, testInt)",
        innerSchema2, nullable = false) :: Nil)
    val outerSchema = StructType(
      StructField("named_struct(testString, testString, " +
        "named_struct(testint, testint), named_struct(testint, testint))",
        innerSchema, nullable = false) :: Nil)

    val expectedRow =
      Seq(new GenericRowWithSchema(
        Array(new GenericRowWithSchema(Array("asdf",
          new GenericRowWithSchema(Array(4141214), innerSchema2)), innerSchema)), outerSchema))

    checkAnswer(sqlContext.sql(
      s"""SELECT NAMED_STRUCT(
            'a', teststring,
            'b', (SELECT NAMED_STRUCT('c', testint) FROM test_table WHERE testint = 4141214)
           ) FROM test_table WHERE testshort = -13
         """.stripMargin),
      expectedRow)

    checkSqlStatement(
      s"""SELECT ( OBJECT( \\'a\\' , "SUBQUERY_1"."TESTSTRING" ,
         |                 \\'b\\' , ( SELECT ( OBJECT( \\'c\\' , "SUBQUERY_1"."TESTINT" ) )
         | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
         | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE ( (
         | "SUBQUERY_0"."TESTINT" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTINT" = 4141214 ) ) )
         | AS "SUBQUERY_1" ) ) ) AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
         | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE ( (
         | "SUBQUERY_0"."TESTSHORT" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTSHORT" = -13 ) ) )
         | AS "SUBQUERY_1"""".stripMargin)
  }
}

class TextPushdownMiscSuite extends PushdownMiscSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "true"
}

class ParquetPushdownMiscSuite extends PushdownMiscSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "true"
}

class TextNoPushdownMiscSuite extends PushdownMiscSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "false"
}

class ParquetNoPushdownMiscSuite extends PushdownMiscSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}

class TextNoCachePushdownMiscSuite extends TextPushdownMiscSuite {
  override protected val s3_result_cache = "false"
}

class ParquetNoCachePushdownMiscSuite extends ParquetPushdownMiscSuite {
  override protected val s3_result_cache = "false"
}
