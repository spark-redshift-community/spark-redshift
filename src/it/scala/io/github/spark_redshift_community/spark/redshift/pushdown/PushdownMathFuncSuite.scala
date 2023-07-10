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

abstract class PushdownMathFuncSuite extends IntegrationPushdownSuiteBase {
  test("Abs pushdown") {
    checkAnswer(
      sqlContext.sql("""SELECT ABS(testfloat) FROM test_table WHERE testfloat < 0 """),
      Seq(Row(1.0))
    )

    checkSqlStatement(
      s"""SELECT ( ABS ( "SUBQUERY_1"."TESTFLOAT" ) ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTFLOAT" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTFLOAT" < 0.0 ) ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("Cos, Sin, Acos, Asin, Atan, Tan pushdown") {

    checkAnswer(
      sqlContext.sql(
        """SELECT
          |COS(testfloat),
          |SIN(testfloat),
          |ACOS(testfloat),
          |ASIN(testfloat),
          |ATAN(testfloat),
          |TAN(testfloat)
          |FROM test_table WHERE testfloat < 0
          |""".stripMargin),
      Seq(Row(0.5403023058681398,
        -0.8414709848078965,
        3.141592653589793,
        -1.5707963267948966,
        -0.7853981633974483,
        -1.5574077246549023))
    )

    checkSqlStatement(
      s"""SELECT ( COS ( CAST ( "SUBQUERY_1"."TESTFLOAT" AS FLOAT8 ) ) ) AS "SUBQUERY_2_COL_0" ,
         |( SIN ( CAST ( "SUBQUERY_1"."TESTFLOAT" AS FLOAT8 ) ) ) AS "SUBQUERY_2_COL_1" ,
         |( ACOS ( CAST ( "SUBQUERY_1"."TESTFLOAT" AS FLOAT8 ) ) ) AS "SUBQUERY_2_COL_2" ,
         |( ASIN ( CAST ( "SUBQUERY_1"."TESTFLOAT" AS FLOAT8 ) ) ) AS "SUBQUERY_2_COL_3" ,
         |( ATAN ( CAST ( "SUBQUERY_1"."TESTFLOAT" AS FLOAT8 ) ) ) AS "SUBQUERY_2_COL_4" ,
         |( TAN ( CAST ( "SUBQUERY_1"."TESTFLOAT" AS FLOAT8 ) ) ) AS "SUBQUERY_2_COL_5"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTFLOAT" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTFLOAT" < 0.0 ) ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("Sqrt pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT SQRT(testfloat) FROM test_table WHERE testfloat > 0
          |AND testfloat < 5""".stripMargin),
      Seq(Row(1))
    )

    checkSqlStatement(
      s"""SELECT ( SQRT ( CAST ( "SUBQUERY_1"."TESTFLOAT" AS FLOAT8 ) ) )
         |AS "SUBQUERY_2_COL_0" FROM ( SELECT *
         |FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTFLOAT" IS NOT NULL ) AND ( ( "SUBQUERY_0"."TESTFLOAT" > 0.0 )
         |AND ( "SUBQUERY_0"."TESTFLOAT" < 5.0 ) ) ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("Log10 pushdown") {

    checkAnswer(
      sqlContext.sql(
        """SELECT LOG10(testfloat*100) FROM test_table WHERE testfloat > 0
          |AND testfloat < 5""".stripMargin),
      Seq(Row(2))
    )

    checkSqlStatement(
      s"""SELECT ( LOG ( CAST ( ( "SUBQUERY_1"."TESTFLOAT" * 100.0 ) AS FLOAT8 ) ) )
         |AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT *
         |FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
         |( ( "SUBQUERY_0"."TESTFLOAT" IS NOT NULL ) AND ( ( "SUBQUERY_0"."TESTFLOAT" > 0.0 )
         |AND ( "SUBQUERY_0"."TESTFLOAT" < 5.0 ) ) ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("Ceil pushdown") {
    checkAnswer(
      sqlContext.sql("""SELECT CEIL(testdouble) FROM test_table WHERE testdouble < 0 """),
      Seq(Row(-1234152))
    )

    checkSqlStatement(
      s"""SELECT ( CEIL ( "SUBQUERY_1"."TESTDOUBLE" ) ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTDOUBLE" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTDOUBLE" < 0.0 ) ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("Floor pushdown") {
    checkAnswer(
      sqlContext.sql("""SELECT FLOOR(testdouble) FROM test_table WHERE testdouble < 0 """),
      Seq(Row(-1234153))
    )

    checkSqlStatement(
      s"""SELECT ( FLOOR ( "SUBQUERY_1"."TESTDOUBLE" ) ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTDOUBLE" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTDOUBLE" < 0.0 ) ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("Round pushdown") {
    checkAnswer(
      sqlContext.sql("""SELECT ROUND(testdouble) FROM test_table WHERE testdouble < 0 """),
      Seq(Row(-1234152))
    )

    checkSqlStatement(
      s"""SELECT ( ROUND ( "SUBQUERY_1"."TESTDOUBLE", 0 ) ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTDOUBLE" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTDOUBLE" < 0.0 ) ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("Greatest pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT GREATEST(testdouble, testint)
          |FROM test_table WHERE testbool = true """.stripMargin),
      Seq(Row(1234152.12312498))
    )

    checkSqlStatement(
      s"""SELECT ( GREATEST ( "SUBQUERY_1"."TESTDOUBLE" ,
         |CAST ( "SUBQUERY_1"."TESTINT" AS FLOAT8 ) ) ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTBOOL" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTBOOL" = true ) ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("Least pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT LEAST(teststring, CONCAT('A',CAST(testshort as String)))
          |FROM test_table WHERE testbool = true """.stripMargin),
      Seq(Row("A23"))
    )

    checkSqlStatement(
      s"""SELECT ( LEAST ( "SUBQUERY_1"."TESTSTRING" ,
         |CONCAT ( \\'A\\' , CAST ( "SUBQUERY_1"."TESTSHORT" AS VARCHAR ) ) ) )
         |AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM
         |( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTBOOL" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTBOOL" = true ) ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("Exp pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT EXP(testbyte) FROM test_table WHERE testbool = true """.stripMargin),
      Seq(Row(2.718281828459045))
    )

    checkSqlStatement(
      s"""SELECT ( EXP ( CAST ( "SUBQUERY_1"."TESTBYTE" AS FLOAT8 ) ) ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTBOOL" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTBOOL" = true ) ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("Arithmetic add pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT testint + 23 FROM test_table WHERE testbool = true """.stripMargin),
      Seq(Row(65))
    )

    checkSqlStatement(
      s"""SELECT ( (  "SUBQUERY_1"."TESTINT" + 23 ) ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTBOOL" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTBOOL" = true ) ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("Arithmetic substract pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT testint - 5 FROM test_table WHERE testbool = true """.stripMargin),
      Seq(Row(37))
    )

    checkSqlStatement(
      s"""SELECT ( (  "SUBQUERY_1"."TESTINT" - 5 ) ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTBOOL" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTBOOL" = true ) ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("Arithmetic multiply pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT testint*5 FROM test_table WHERE testbool = true """.stripMargin),
      Seq(Row(210))
    )

    checkSqlStatement(
      s"""SELECT ( ( "SUBQUERY_1"."TESTINT" * 5) ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTBOOL" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTBOOL" = true ) ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("Arithmetic divide pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT testint/2 FROM test_table WHERE testbool = true """.stripMargin),
      Seq(Row(21f))
    )

    checkSqlStatement(
      s"""SELECT ( ( CAST ( "SUBQUERY_1"."TESTINT" AS FLOAT8 ) / 2.0 ) ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTBOOL" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTBOOL" = true ) ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }
}

class DefaultPushdownMathFuncSuite extends PushdownMathFuncSuite {
  override protected val s3format: String = "DEFAULT"
}

class ParquetPushdownMathFuncSuite extends PushdownMathFuncSuite {
  override protected val s3format: String = "PARQUET"
}