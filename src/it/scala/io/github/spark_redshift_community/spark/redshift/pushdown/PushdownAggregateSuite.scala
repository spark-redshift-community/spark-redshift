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

abstract class PushdownAggregateSuite extends IntegrationPushdownSuiteBase {

  val testCount: TestCase = TestCase (
    """SELECT COUNT(1) FROM test_table""",
    Seq(Row(5)),
    s"""SELECT ( COUNT ( 1 ) ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" LIMIT 1""".stripMargin
  )

  val testCountDistinct: TestCase = TestCase (
    """SELECT COUNT(DISTINCT testdouble) FROM test_table""",
    Seq(Row(3)),
    s"""SELECT ( COUNT ( DISTINCT "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."TESTDOUBLE" ) AS "SUBQUERY_1_COL_0" FROM
       | ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin
  )

  val testCountGroupBy1: TestCase = TestCase (
    """SELECT COUNT(1), testbyte FROM test_table GROUP BY testbyte""",
    Seq(Row(1, null), Row(2, 0), Row(2, 1)),
    s"""SELECT ( COUNT ( 1 ) ) AS "SUBQUERY_2_COL_0" ,
       | ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_1"
       | FROM ( SELECT ( "SUBQUERY_0"."TESTBYTE" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0"""".stripMargin
  )

  val testCountGroupBy2: TestCase = TestCase (
    """SELECT COUNT(testbyte), testbyte FROM test_table GROUP BY testbyte""",
    Seq(Row(0, null), Row(2, 0), Row(2, 1)),
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0" ,
       | ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_1"
       | FROM ( SELECT ( "SUBQUERY_0"."TESTBYTE" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0"""".stripMargin
  )

  test("Test COUNT aggregation statements") {
    doTest(sqlContext, testCount)
    doTest(sqlContext, testCountDistinct)
    doTest(sqlContext, testCountGroupBy1)
    doTest(sqlContext, testCountGroupBy2)
  }

  val testMax: TestCase = TestCase (
    """SELECT MAX(testdouble) FROM test_table""",
    Seq(Row(1234152.12312498)),
    s"""SELECT ( MAX ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."TESTDOUBLE" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS"SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin
  )

  val testMaxGroupBy: TestCase = TestCase (
    """SELECT MAX(testfloat), testbyte FROM test_table GROUP BY testbyte""",
    Seq(Row(null, null), Row(100000f, 0), Row(1f, 1)),
    s"""SELECT ( MAX ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) ) AS "SUBQUERY_2_COL_0" ,
       | ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_1"
       | FROM ( SELECT ( "SUBQUERY_0"."TESTBYTE" ) AS "SUBQUERY_1_COL_0" ,
       | ( "SUBQUERY_0"."TESTFLOAT" ) AS "SUBQUERY_1_COL_1"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0"""".stripMargin
  )

  test("Test Max aggregation statements") {
    doTest(sqlContext, testMax)
    doTest(sqlContext, testMaxGroupBy)
  }

  val testMin: TestCase = TestCase(
    """SELECT MIN(testfloat) FROM test_table""",
    Seq(Row(-1.0)),
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."TESTFLOAT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS"RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin
  )

  val testMinGroupBy: TestCase = TestCase (
    """SELECT MIN(testfloat), testbyte FROM test_table GROUP BY testbyte""",
    Seq(Row(null, null), Row(-1f, 0), Row(0f, 1)),
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) ) AS "SUBQUERY_2_COL_0" ,
       | ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_1"
       | FROM ( SELECT ( "SUBQUERY_0"."TESTBYTE" ) AS "SUBQUERY_1_COL_0" ,
       | ( "SUBQUERY_0"."TESTFLOAT" ) AS "SUBQUERY_1_COL_1"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0"""".stripMargin
  )

  test("Test Min aggregation statements") {
    doTest(sqlContext, testMin)
    doTest(sqlContext, testMinGroupBy)
  }

  val testAvg: TestCase = TestCase(
    """SELECT AVG(testdouble) FROM test_table""",
    Seq(Row(0.0)),
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."TESTDOUBLE" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin
  )

  val testAvgGroupBy: TestCase = TestCase (
    """SELECT AVG(testint), testbyte FROM test_table GROUP BY testbyte""",
    Seq(Row(null, null), Row(4141214f, 0), Row(42f, 1)),
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ::FLOAT ) ) AS "SUBQUERY_2_COL_0" ,
       | ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_1"
       | FROM ( SELECT ( "SUBQUERY_0"."TESTBYTE" ) AS "SUBQUERY_1_COL_0" ,
       | ( "SUBQUERY_0"."TESTINT" ) AS "SUBQUERY_1_COL_1"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0"""".stripMargin
  )

  test("Test Avg aggregation statements") {
    doTest(sqlContext, testAvg)
    doTest(sqlContext, testAvgGroupBy)
  }

  val testSum: TestCase = TestCase(
    """SELECT SUM(testfloat) FROM test_table""",
    Seq(Row(100000.0)),
    s"""SELECT ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."TESTFLOAT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin
  )

  val testSumGroupBy: TestCase = TestCase (
    """SELECT SUM(testint), testbyte FROM test_table GROUP BY testbyte""",
    Seq(Row(null, null), Row(4141214, 0), Row(84, 1)),
    s"""SELECT ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) ) AS "SUBQUERY_2_COL_0" ,
       | ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_1"
       | FROM ( SELECT ( "SUBQUERY_0"."TESTBYTE" ) AS "SUBQUERY_1_COL_0" ,
       | ( "SUBQUERY_0"."TESTINT" ) AS "SUBQUERY_1_COL_1"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0"""".stripMargin
  )

  test("Test Sum aggregation statements") {
    doTest(sqlContext, testSum)
    doTest(sqlContext, testSumGroupBy)
  }

  test("test MakeDecimal function in aggregation statement") {
    checkAnswer(
      sqlContext.sql(
        """SELECT SUM(cast(testint AS DECIMAL(5, 2))),
          |testbyte FROM test_table WHERE testbyte>0
          |GROUP BY testbyte""".stripMargin),
      Seq(Row(84f, 1)))

    checkSqlStatement(
      s"""SELECT ( CAST ( ( SUM ( (
         |CAST ( "SUBQUERY_2"."SUBQUERY_2_COL_1" AS DECIMAL(5, 2) ) * POW(10, 2 ) ) ) / POW(10, 2 ) )
         |AS DECIMAL( 15 , 2 ) ) ) AS "SUBQUERY_3_COL_0" ,
         |( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) AS "SUBQUERY_3_COL_1"
         |FROM ( SELECT ( "SUBQUERY_1"."TESTBYTE" ) AS "SUBQUERY_2_COL_0" ,
         |( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_1" FROM ( SELECT * FROM
         |( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTBYTE" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTBYTE" > 0 ) ) ) AS "SUBQUERY_1" )
         |AS "SUBQUERY_2" GROUP BY "SUBQUERY_2"."SUBQUERY_2_COL_0"
         |""".stripMargin
    )
  }
}

class DefaultPushdownAggregateSuite extends PushdownAggregateSuite {
  override protected val s3format: String = "DEFAULT"
  override protected val auto_pushdown: String = "true"
}

class ParquetPushdownAggregateSuite extends PushdownAggregateSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "true"
}

class DefaultNoPushdownAggregateSuite extends PushdownAggregateSuite {
  override protected val s3format: String = "DEFAULT"
  override protected val auto_pushdown: String = "false"
}

class ParquetNoPushdownAggregateSuite extends PushdownAggregateSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}