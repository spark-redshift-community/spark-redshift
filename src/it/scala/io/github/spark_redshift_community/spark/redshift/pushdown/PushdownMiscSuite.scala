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

  test("Boolean type cast to String") {
    checkAnswer(
      sqlContext.sql(
        s"""SELECT CAST(testbool AS STRING) FROM test_table WHERE testbool = true""".stripMargin),
      Seq(Row("true")))

    checkSqlStatement(
      s"""SELECT ( CASE "SUBQUERY_1"."TESTBOOL" WHEN TRUE THEN \\'true\\'
         | WHEN FALSE THEN \\'false\\' ELSE \\'null\\' END )
         |  AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT *
         |  FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         | WHERE ( ( "SUBQUERY_0"."TESTBOOL" IS NOT NULL ) AND "SUBQUERY_0"."TESTBOOL" ) )
         | AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("Boolean expression cast to String") {
    checkAnswer(
      sqlContext.sql(
        s"""SELECT CAST(testint > testfloat AS STRING) FROM test_table WHERE testbool = true""".stripMargin),
      Seq(Row("true")))

    checkSqlStatement(
      s"""SELECT ( CASE ( CAST ( "SUBQUERY_1"."TESTINT" AS FLOAT4 ) > "SUBQUERY_1"."TESTFLOAT" )
         | WHEN TRUE THEN \\'true\\' WHEN FALSE THEN \\'false\\' ELSE \\'null\\' END ) AS "SUBQUERY_2_COL_0"
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
