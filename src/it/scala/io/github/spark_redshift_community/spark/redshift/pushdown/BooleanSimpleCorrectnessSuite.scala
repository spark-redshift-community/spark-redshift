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
package io.github.spark_redshift_community.spark.redshift.pushdown.test

import org.apache.spark.sql.Row

/*
  Test against built-in table
 */
abstract class BooleanSimpleCorrectnessSuite extends IntegrationPushdownSuiteBase {

  val string2000Char =
    """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
      |""".stripMargin

  test("child in list pushdown -v2") {
    // "Column name", list in spark query, result size, list in pushdown
    val input = List(
      ("testbyte", "(0,1)", 4, "(0,1)"),
      ("testbool", "(true, false)", 3, "(true, false)"),
      ("testdouble", "(0.0, 0.2)", 2, "(0.0, 0.2)"),
      ("testfloat", "(0.0, 0.5)", 1, "(0.0::float4, 0.5::float4)"),
      ("testint", "(42, 43)", 2, "(42, 43)"),
      ("testlong", "(1239012341823719, 1)", 4, "(1239012341823719, 1)"),
      ("testshort", "(23, 24)", 2, "(23, 24)")
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2
      val result_size = test_case._3
      val pushdown_list = test_case._4
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name in $expected_res"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
           |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0" WHERE "SQ_0"."$column_name" IN $pushdown_list )
           |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child in list pushdown", P1Test) {
    checkAnswer(
      sqlContext.sql("""SELECT count(*) FROM test_table where testdouble in (0.0, 2.0)"""),
      Seq(Row(2)))

    checkSqlStatement(
      s"""SELECT (COUNT(1)) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RCQ_ALIAS" ) AS "SQ_0"
         |WHERE "SQ_0"."TESTDOUBLE" IN (0.0,2.0))
         |AS "SQ_1" LIMIT 1""".stripMargin)
  }

  test("child in list pushdown (bool type)") {
    checkAnswer(
      sqlContext.sql("""SELECT count(*) FROM test_table where testbool in (true, false)"""),
      Seq(Row(3)))

    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS
         |"RCQ_ALIAS" ) AS "SQ_0"
         |WHERE "SQ_0"."TESTBOOL" IN ( true , false ) )
         |AS "SQ_1" LIMIT 1""".stripMargin)
  }

  test("child in list pushdown (date type)") {
    checkAnswer(
      sqlContext.sql(
        """SELECT count(*) FROM test_table where testdate
          |in ('2015-07-01', '2015-07-02')""".stripMargin),
      Seq(Row(2)))

    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
         |AS "SQ_0" WHERE CAST ( "SQ_0"."TESTDATE" AS VARCHAR )
         |IN ( \\'2015-07-01\\' , \\'2015-07-02\\' ) ) AS "SQ_1" LIMIT 1""".stripMargin)
  }

  test("child in list pushdown (string type)") {
    checkAnswer(
      sqlContext.sql(
        """SELECT count(*) FROM test_table where teststring
          |in ('asdf', 'ldf')""".stripMargin),
      Seq(Row(1)))

    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
         |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0"
         |WHERE "SQ_0"."TESTSTRING" IN ( \\'asdf\\' , \\'ldf\\' ) )
         |AS "SQ_1" LIMIT 1""".stripMargin)
  }

  test("child IS NULL pushdown", P1Test) {
    // "Column name" and result size
    val input = List(
      ("testbyte", 1),
      ("testbool", 2),
      ("testdate", 2),
      ("testdouble", 1),
      ("testfloat", 1),
      ("testint", 2),
      ("testlong", 1),
      ("testshort", 2),
      ("teststring", 1),
      ("testtimestamp", 2)
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val result_size = test_case._2
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name is NULL"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM
           |( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0" WHERE ( "SQ_0"."$column_name" IS NULL ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child IS NOT NULL pushdown", P1Test) {
    // "Column name" and result size
    val input = List(
      ("testbyte", 4),
      ("testbool", 3),
      ("testdate", 3),
      ("testdouble", 4),
      ("testfloat", 4),
      ("testint", 3),
      ("testlong", 4),
      ("testshort", 3),
      ("teststring", 4),
      ("testtimestamp", 3)
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val result_size = test_case._2
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name is NOT NULL"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
           |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( "SQ_0"."$column_name" IS NOT NULL ) ) AS "SQ_1"
           |LIMIT 1""".stripMargin)
    })
  }

  test("child EqualTo pushdown", P1Test) {
    // "Column name",match value, result size, needed cast
    val input = List(
      ("testbyte", 0, 2, ""),
      ("testdouble", 1234152.12312498, 1, ""),
      ("testfloat", 1.0, 1, "::float4"),
      ("testint", 42, 2, ""),
      ("testlong", 1239012341823719L, 4, ""),
      ("testshort", 23, 1, "")
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val match_value = test_case._2
      val result_size = test_case._3
      val cast = test_case._4
        checkAnswer(
          sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name = $match_value """),
          Seq(Row(result_size)))
        checkSqlStatement(
          s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
             |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
             |AS "SQ_0"
             |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
             |AND ( "SQ_0"."$column_name" = $match_value $cast ) ) )
             |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child EqualTo pushdown (boolean type)") {
    // "Column name",match value and result size
    val input = List(
      ("testbool", true, 1),
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val match_value = test_case._2
      val result_size = test_case._3
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name = $match_value """),
        Seq(Row(result_size)))
      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
                                    |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
                                    |AS "SQ_0"
                                    |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
                                    |AND ( "SQ_0"."$column_name" = $match_value ) ) )
                                    |AS "SQ_1" LIMIT 1""".stripMargin,
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
                                    |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
                                    |AS "SQ_0"
                                    |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
                                    |AND "SQ_0"."$column_name" ) )
                                    |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  //    val input = List(
  //      ("testbyte", 0.1, 2),
  //      ("testbool", 2.3, 0),
  //      ("testdouble", 1234152, 0),
  //      ("testfloat", 1, 1),
  //      ("testint", 42.0, 2),
  //      ("testlong", 1239012341823719.0, 4),
  //      ("testshort", 23.0, 1)
  test("child EqualTo pushdown (double different type)") {
    checkAnswer(
      sqlContext.sql(s"""SELECT count(*) FROM test_table where testdouble = 1234152 """),
      Seq(Row(0)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
         |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
         |AS "SQ_0" WHERE ( ( "SQ_0"."TESTDOUBLE" IS NOT NULL ) AND
         |( "SQ_0"."TESTDOUBLE" = 1234152.0 ) ) ) AS "SQ_1" LIMIT 1""".stripMargin)
  }

  test("child EqualTo pushdown (float different type)") {
    checkAnswer(
      sqlContext.sql(s"""SELECT count(*) FROM test_table where testfloat = 1 """),
      Seq(Row(1)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
         |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0"
         |WHERE ( ( "SQ_0"."TESTFLOAT" IS NOT NULL )
         |AND ( "SQ_0"."TESTFLOAT" = 1.0::float4 ) ) ) AS "SQ_1" LIMIT 1""".stripMargin)
  }

  test("child EqualTo pushdown (int different type)") {
    checkAnswer(
      sqlContext.sql(s"""SELECT count(*) FROM test_table where testint = 42.0 """),
      Seq(Row(2)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM
         |( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
         |AS "SQ_0" WHERE ( ( "SQ_0"."TESTINT" IS NOT NULL )
         |AND ( "SQ_0"."TESTINT" = 42 ) ) ) AS "SQ_1" LIMIT 1""".stripMargin)
  }

  test("child EqualTo pushdown (long different type)") {
    checkAnswer(
      sqlContext.sql(s"""SELECT count(*) FROM test_table where testlong = 1239012341823719.0 """),
      Seq(Row(4)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM
         |( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
         |AS "SQ_0" WHERE ( ( "SQ_0"."TESTLONG" IS NOT NULL )
         |AND ( "SQ_0"."TESTLONG" = 1239012341823719 ) ) )
         |AS "SQ_1" LIMIT 1""".stripMargin)
  }

  test("child EqualTo pushdown (date type)") {
    checkAnswer(
      sqlContext.sql(s"""SELECT count(*) FROM test_table where testdate = '2015-07-01' """),
      Seq(Row(1)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         AS "RCQ_ALIAS" ) AS "SQ_0"
         |WHERE ( ( "SQ_0"."TESTDATE" IS NOT NULL )
         |AND ( "SQ_0"."TESTDATE" = DATEADD(day, 16617 ,
         |TO_DATE(\\'1970-01-01\\', \\'YYYY-MM-DD\\')) ) ) ) AS "SQ_1"
         |LIMIT 1""".stripMargin)
  }

  test("child EqualTo pushdown (string type)") {
    checkAnswer(
      sqlContext.sql(s"""SELECT count(*) FROM test_table where teststring = 'asdf' """),
      Seq(Row(1)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
         |AS "SQ_0" WHERE ( ( "SQ_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SQ_0"."TESTSTRING" = \\'asdf\\' ) ) ) AS "SQ_1"
         |LIMIT 1""".stripMargin)
  }

  test("child EqualTo pushdown (timestamp type)") {
    checkAnswer(
      sqlContext.sql(
        s"""SELECT count(*) FROM test_table
           |where testtimestamp = '2015-07-02 00:00:00.000' """.stripMargin),
      Seq(Row(1)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
         |AS "SQ_0" WHERE ( ( "SQ_0"."TESTTIMESTAMP" IS NOT NULL )
         |AND ( "SQ_0"."TESTTIMESTAMP" = \\'2015-07-02 00:00:00\\' ::TIMESTAMP ) ) )
         |AS "SQ_1" LIMIT 1""".stripMargin)
  }

  test("child NOT EqualTo pushdown", P1Test) {
    // "Column name",match value, result size, needed cast
    val input = List(
      ("testbyte", 0, 2, ""),
      ("testdouble", 1234152.12312498, 3, ""),
      ("testfloat", 1.0, 3, "::float4"),
      ("testint", 42, 1, ""),
      ("testlong", 1239012341823719L, 0, ""),
      ("testshort", 23, 2, "")
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val match_value = test_case._2
      val result_size = test_case._3
      val cast = test_case._4
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name != $match_value """),
        Seq(Row(result_size)))
      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
           |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" != $match_value $cast ) ) ) AS "SQ_1" LIMIT 1
           |""".stripMargin)
    })
  }

  test("child NOT EqualTo pushdown (boolean type)") {
    // "Column name",match value and result size
    val input = List(
      ("testbool", true, 2),
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val match_value = test_case._2
      val result_size = test_case._3
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name != $match_value """),
        Seq(Row(result_size)))
      checkSqlStatement(
          s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
             |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
             |AS "SQ_0"
             |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
             |AND ( "SQ_0"."$column_name" >= $match_value ) ) )
             |AS "SQ_1" LIMIT 1""".stripMargin,
          s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
             |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
             |AS "SQ_0"
             |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
             |AND NOT ( "SQ_0"."$column_name" ) ) )
             |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child NOT EqualTo pushdown (date type)") {
    checkAnswer(
      sqlContext.sql(s"""SELECT count(*) FROM test_table where testdate != '2015-07-01' """),
      Seq(Row(2)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         AS "RCQ_ALIAS" ) AS "SQ_0"
         |WHERE ( ( "SQ_0"."TESTDATE" IS NOT NULL )
         |AND ( "SQ_0"."TESTDATE" != DATEADD(day, 16617 ,
         |TO_DATE(\\'1970-01-01\\', \\'YYYY-MM-DD\\')) ) ) ) AS "SQ_1"
         |LIMIT 1""".stripMargin)
  }

  test("child NOT EqualTo pushdown (string type)") {
    checkAnswer(
      sqlContext.sql(s"""SELECT count(*) FROM test_table where teststring != 'asdf' """),
      Seq(Row(3)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
         |AS "SQ_0" WHERE ( ( "SQ_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SQ_0"."TESTSTRING" != \\'asdf\\' ) ) ) AS "SQ_1"
         |LIMIT 1""".stripMargin)
  }

  test("child NOT EqualTo pushdown (timestamp type)") {
    checkAnswer(
      sqlContext.sql(
        s"""SELECT count(*) FROM test_table
           |where testtimestamp != '2015-07-02 00:00:00.000' """.stripMargin),
      Seq(Row(2)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
         |AS "SQ_0" WHERE ( ( "SQ_0"."TESTTIMESTAMP" IS NOT NULL )
         |AND ( "SQ_0"."TESTTIMESTAMP" != \\'2015-07-02 00:00:00\\' ::TIMESTAMP ) ) )
         |AS "SQ_1" LIMIT 1""".stripMargin)
  }

  test("child GreaterThanOrEqual pushdown", P1Test) {
    // "Column name",match value, result size, needed cast
    val input = List(
      ("testbyte", 0, 4, ""),
      ("testbool", true, 1, ""),
      ("testdouble", 1234152.12312498, 1, ""),
      ("testfloat", 1.0, 2, "::float4"),
      ("testint", 42, 3, ""),
      ("testlong", 1239012341823719L, 4, ""),
      ("testshort", 23, 2, "")
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val match_value = test_case._2
      val result_size = test_case._3
      val cast = test_case._4
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name >= $match_value """),
        Seq(Row(result_size)))
      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
           |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" >= $match_value $cast ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child GreaterThanOrEqual pushdown (date type)") {
    checkAnswer(
      sqlContext.sql(s"""SELECT count(*) FROM test_table where testdate >= '2015-07-01' """),
      Seq(Row(3)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         AS "RCQ_ALIAS" ) AS "SQ_0"
         |WHERE ( ( "SQ_0"."TESTDATE" IS NOT NULL )
         |AND ( "SQ_0"."TESTDATE" >= DATEADD(day, 16617 ,
         |TO_DATE(\\'1970-01-01\\', \\'YYYY-MM-DD\\')) ) ) ) AS "SQ_1"
         |LIMIT 1""".stripMargin)
  }

  test("child GreaterThanOrEqual pushdown (string type)") {
    checkAnswer(
      sqlContext.sql(s"""SELECT count(*) FROM test_table where teststring >= 'asdf' """),
      Seq(Row(2)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
         |AS "SQ_0" WHERE ( ( "SQ_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SQ_0"."TESTSTRING" >= \\'asdf\\' ) ) ) AS "SQ_1"
         |LIMIT 1""".stripMargin)
  }

  test("child LessThanOrEqual pushdown", P1Test) {
    // "Column name",match value, result size, needed cast
    val input = List(
      ("testbyte", 0, 2, ""),
      ("testbool", true, 3, ""),
      ("testdouble", 1234152.12312498, 4, ""),
      ("testfloat", 1.0, 3, "::float4"),
      ("testint", 42, 2, ""),
      ("testlong", 1239012341823719L, 4, ""),
      ("testshort", 23, 2, "")
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val match_value = test_case._2
      val result_size = test_case._3
      val cast = test_case._4
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name <= $match_value """),
        Seq(Row(result_size)))
      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
           |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name"<= $match_value $cast ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }


  test("child LessThanOrEqual pushdown (date type)") {
    checkAnswer(
      sqlContext.sql(s"""SELECT count(*) FROM test_table where testdate <= '2015-07-01' """),
      Seq(Row(1)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         AS "RCQ_ALIAS" ) AS "SQ_0"
         |WHERE ( ( "SQ_0"."TESTDATE" IS NOT NULL )
         |AND ( "SQ_0"."TESTDATE" <= DATEADD(day, 16617 ,
         |TO_DATE(\\'1970-01-01\\', \\'YYYY-MM-DD\\')) ) ) ) AS "SQ_1"
         |LIMIT 1""".stripMargin)
  }

  test("child LessThanOrEqual pushdown (string type)") {
    checkAnswer(
      sqlContext.sql(s"""SELECT count(*) FROM test_table where teststring <= 'asdf' """),
      Seq(Row(3)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
         |AS "SQ_0" WHERE ( ( "SQ_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SQ_0"."TESTSTRING" <= \\'asdf\\' ) ) ) AS "SQ_1"
         |LIMIT 1""".stripMargin)
  }

  test("child GreaterThan pushdown", P1Test) {
    // "Column name",match value, result size, needed cast
    val input = List(
      ("testbyte", 0, 2, ""),
      ("testbool", true, 0, ""),
      ("testdouble", 1234152.12312498, 0, ""),
      ("testfloat", 1.0, 1, "::float4"),
      ("testint", 42, 1, ""),
      ("testlong", 1239012341823719L, 0, ""),
      ("testshort", 23, 1, "")
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val match_value = test_case._2
      val result_size = test_case._3
      val cast = test_case._4
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name > $match_value """),
        Seq(Row(result_size)))
      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
           |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" > $match_value $cast) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child GreaterThan pushdown (date type)") {
    checkAnswer(
      sqlContext.sql(s"""SELECT count(*) FROM test_table where testdate > '2015-07-01' """),
      Seq(Row(2)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         AS "RCQ_ALIAS" ) AS "SQ_0"
         |WHERE ( ( "SQ_0"."TESTDATE" IS NOT NULL )
         |AND ( "SQ_0"."TESTDATE" > DATEADD(day, 16617 ,
         |TO_DATE(\\'1970-01-01\\', \\'YYYY-MM-DD\\')) ) ) ) AS "SQ_1"
         |LIMIT 1""".stripMargin)
  }

  test("child GreaterThan pushdown (string type)") {
    checkAnswer(
      sqlContext.sql(s"""SELECT count(*) FROM test_table where teststring > 'asdf' """),
      Seq(Row(1)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
         |AS "SQ_0" WHERE ( ( "SQ_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SQ_0"."TESTSTRING" > \\'asdf\\' ) ) ) AS "SQ_1"
         |LIMIT 1""".stripMargin)
  }

  test("child LessThan pushdown", P1Test) {
    // "Column name",match value, result size, needed cast
    val input = List(
      ("testbyte", 0, 0, ""),
      ("testbool", true, 2, ""),
      ("testdouble", 1234152.12312498, 3, ""),
      ("testfloat", 1.0, 2, "::float4"),
      ("testint", 42, 0, ""),
      ("testlong", 1239012341823719L, 0, ""),
      ("testshort", 23, 1, "")
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val match_value = test_case._2
      val result_size = test_case._3
      val cast = test_case._4
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name < $match_value """),
        Seq(Row(result_size)))
      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
           |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" < $match_value $cast ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child LessThan pushdown (date type)") {
    checkAnswer(
      sqlContext.sql(
        s"""SELECT count(*) FROM test_table
           |where testdate < '2015-07-01'""".stripMargin),
      Seq(Row(0)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         AS "RCQ_ALIAS" ) AS "SQ_0"
         |WHERE ( ( "SQ_0"."TESTDATE" IS NOT NULL )
         |AND ( "SQ_0"."TESTDATE" < DATEADD(day, 16617 ,
         |TO_DATE(\\'1970-01-01\\', \\'YYYY-MM-DD\\')) ) ) ) AS "SQ_1"
         |LIMIT 1""".stripMargin)
  }

  test("child LessThan pushdown (string type)") {
    checkAnswer(
      sqlContext.sql(s"""SELECT count(*) FROM test_table where teststring < 'asdf' """),
      Seq(Row(2)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
         |AS "SQ_0" WHERE ( ( "SQ_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SQ_0"."TESTSTRING" < \\'asdf\\' ) ) ) AS "SQ_1"
         |LIMIT 1""".stripMargin)
  }

  test("child contains pushdown", P1Test) {
    checkAnswer(
      sqlContext.sql(s"""SELECT count(*) FROM test_table where teststring LIKE '%asdf%' """),
      Seq(Row(1)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
         |AS "SQ_0" WHERE ( ( "SQ_0"."TESTSTRING" IS NOT NULL )
         |AND ( CAST( "SQ_0"."TESTSTRING" AS VARCHAR )
         |LIKE CONCAT(\\'%\\', CONCAT(\\'asdf\\', \\'%\\') ) ) ) ) AS "SQ_1"
         |LIMIT 1""".stripMargin)
  }

  test("child contains pushdown - long string") {
    checkAnswer(
      sqlContext.sql(
        s"""SELECT count(*) FROM test_table where teststring
           |LIKE '%$string2000Char%' """.stripMargin),
      Seq(Row(0)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
         |AS "SQ_0" WHERE ( ( "SQ_0"."TESTSTRING" IS NOT NULL )
         |AND ( CAST ( "SQ_0"."TESTSTRING" AS VARCHAR ) LIKE
         |CONCAT(\\'%\\', CONCAT(\\'$string2000Char\\', \\'%\\') ) ) ) ) AS "SQ_1"
         |LIMIT 1""".stripMargin)
  }

  test("child startswith pushdown", P1Test) {
    checkAnswer(
      sqlContext.sql(s"""SELECT count(*) FROM test_table where teststring LIKE 'asdf%' """),
      Seq(Row(1)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
         |AS "SQ_0" WHERE ( ( "SQ_0"."TESTSTRING" IS NOT NULL )
         |AND ( CAST ( "SQ_0"."TESTSTRING" AS VARCHAR ) LIKE
         |CONCAT(\\'asdf\\', \\'%\\' ) ) ) ) AS "SQ_1"
         |LIMIT 1""".stripMargin)
  }

  test("child endswith pushdown - with quote") {
    checkAnswer(
      sqlContext.sql(s"""SELECT count(*) FROM test_table where teststring LIKE '%\\'s樂趣' """),
      Seq(Row(1)))
    checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
           |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0" WHERE ( ( "SQ_0"."TESTSTRING" IS NOT NULL )
           |AND ( CAST ( "SQ_0"."TESTSTRING" AS VARCHAR ) LIKE
           |CONCAT(\\'%\\', \\'\\'\\'s樂趣\\') ) ) ) AS "SQ_1"
           |LIMIT 1""".stripMargin)
  }

  test("child startswith pushdown - with quote") {
    checkAnswer(
      sqlContext.sql(s"""SELECT count(*) FROM test_table where teststring LIKE 'Unicode\\'%' """),
      Seq(Row(1)))
    checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
           |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS")
           |AS "SQ_0" WHERE ( ( "SQ_0"."TESTSTRING" IS NOT NULL )
           |AND ( CAST ( "SQ_0"."TESTSTRING" AS VARCHAR ) LIKE
           |CONCAT(\\'Unicode\\'\\'\\', \\'%\\') ) ) ) AS "SQ_1"
           |LIMIT 1""".stripMargin)
  }

  test("child contains pushdown - with quote") {
    checkAnswer(
      sqlContext.sql(s"""SELECT count(*) FROM test_table where teststring LIKE '%\\'%' """),
      Seq(Row(1)))
    checkSqlStatement(
    s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
       |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS")
       |AS "SQ_0" WHERE ( ( "SQ_0"."TESTSTRING" IS NOT NULL )
       |AND ( CAST ( "SQ_0"."TESTSTRING" AS VARCHAR ) LIKE
       |CONCAT( \\'%\\' , CONCAT(\\'\\'\\'\\', \\'%\\' ) ) ) ) ) AS "SQ_1"
       |LIMIT 1""".stripMargin
    )
  }

  test("child startswith pushdown - long string") {
    checkAnswer(
      sqlContext.sql(
        s"""SELECT count(*) FROM test_table where teststring
           |LIKE '$string2000Char%' """.stripMargin),
      Seq(Row(0)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
         |AS "SQ_0" WHERE ( ( "SQ_0"."TESTSTRING" IS NOT NULL )
         |AND ( CAST ( "SQ_0"."TESTSTRING" AS VARCHAR ) LIKE
         |CONCAT( \\'$string2000Char\\', \\'%\\' ) ) ) ) AS "SQ_1"
         |LIMIT 1""".stripMargin)
  }

  test("child endswith pushdown", P1Test) {
    checkAnswer(
      sqlContext.sql(s"""SELECT count(*) FROM test_table where teststring LIKE '%asdf' """),
      Seq(Row(1)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
         |AS "SQ_0" WHERE ( ( "SQ_0"."TESTSTRING" IS NOT NULL )
         |AND ( CAST ( "SQ_0"."TESTSTRING" AS VARCHAR ) LIKE CONCAT(\\'%\\', \\'asdf\\') ) ) ) AS "SQ_1"
         |LIMIT 1""".stripMargin)
  }

  test("child endswith pushdown - long string") {
    checkAnswer(
      sqlContext.sql(
        s"""SELECT count(*) FROM test_table where teststring
           |LIKE '%$string2000Char' """.stripMargin),
      Seq(Row(0)))
    checkSqlStatement(
      s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
         |AS "SQ_0" WHERE ( ( "SQ_0"."TESTSTRING" IS NOT NULL )
         |AND ( CAST ( "SQ_0"."TESTSTRING" AS VARCHAR ) LIKE
         |CONCAT(\\'%\\', \\'$string2000Char\\' ) ) ) ) AS "SQ_1"
         |LIMIT 1""".stripMargin)
  }
}

class TextBooleanSimpleCorrectnessSuite extends BooleanSimpleCorrectnessSuite {
  override protected val s3format: String = "TEXT"
}

class ParquetBooleanSimpleCorrectnessSuite extends BooleanSimpleCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
}

class TextNoPushdownBooleanSimpleCorrectnessSuite extends BooleanSimpleCorrectnessSuite {
  override protected val auto_pushdown: String = "false"
  override protected val s3format: String = "TEXT"
}

class ParquetNoPushdownBooleanSimpleCorrectnessSuite extends BooleanSimpleCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}

class TextPushdownNoCacheBooleanSimpleCorrectnessSuite
  extends TextBooleanSimpleCorrectnessSuite {
  override protected val s3_result_cache = "false"
}

class ParquetPushdownNoCacheBooleanSimpleCorrectnessSuite
extends ParquetBooleanSimpleCorrectnessSuite {
  override protected val s3_result_cache = "false"
}
