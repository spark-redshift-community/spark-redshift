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
    s"""SELECT ( COUNT ( 1 ) ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" LIMIT 1""".stripMargin
  )

  val testCountDistinct: TestCase = TestCase (
    """SELECT COUNT(DISTINCT testdouble) FROM test_table""",
    Seq(Row(3)),
    s"""SELECT ( COUNT ( DISTINCT "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."TESTDOUBLE" ) AS "SQ_1_COL_0" FROM
       | ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin
  )

  val testCountGroupBy1: TestCase = TestCase (
    """SELECT COUNT(1), testbyte FROM test_table GROUP BY testbyte""",
    Seq(Row(1, null), Row(2, 0), Row(2, 1)),
    s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" ,
       | ( "SQ_1"."SQ_1_COL_0" ) AS "SQ_2_COL_1"
       | FROM ( SELECT ( "SQ_0"."TESTBYTE" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" GROUP BY "SQ_1"."SQ_1_COL_0"""".stripMargin
  )

  val testCountGroupBy2: TestCase = TestCase (
    """SELECT COUNT(testbyte), testbyte FROM test_table GROUP BY testbyte""",
    Seq(Row(0, null), Row(2, 0), Row(2, 1)),
    s"""SELECT ( COUNT ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0" ,
       | ( "SQ_1"."SQ_1_COL_0" ) AS "SQ_2_COL_1"
       | FROM ( SELECT ( "SQ_0"."TESTBYTE" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" GROUP BY "SQ_1"."SQ_1_COL_0"""".stripMargin
  )

  test("Test COUNT aggregation statements", P0Test, P1Test) {
    doTest(sqlContext, testCount)
    doTest(sqlContext, testCountDistinct)
    doTest(sqlContext, testCountGroupBy1)
    doTest(sqlContext, testCountGroupBy2)
  }

  val testMax: TestCase = TestCase (
    """SELECT MAX(testdouble) FROM test_table""",
    Seq(Row(1234152.12312498)),
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."TESTDOUBLE" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS"SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin
  )

  val testMaxGroupBy: TestCase = TestCase (
    """SELECT MAX(testfloat), testbyte FROM test_table GROUP BY testbyte""",
    Seq(Row(null, null), Row(100000f, 0), Row(1f, 1)),
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_1" ) ) AS "SQ_2_COL_0" ,
       | ( "SQ_1"."SQ_1_COL_0" ) AS "SQ_2_COL_1"
       | FROM ( SELECT ( "SQ_0"."TESTBYTE" ) AS "SQ_1_COL_0" ,
       | ( "SQ_0"."TESTFLOAT" ) AS "SQ_1_COL_1"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" GROUP BY "SQ_1"."SQ_1_COL_0"""".stripMargin
  )

  test("Test Max aggregation statements", P0Test, P1Test) {
    doTest(sqlContext, testMax)
    doTest(sqlContext, testMaxGroupBy)
  }

  val testMin: TestCase = TestCase(
    """SELECT MIN(testfloat) FROM test_table""",
    Seq(Row(-1.0)),
    s"""SELECT ( MIN ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."TESTFLOAT" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS"RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin
  )

  val testMinGroupBy: TestCase = TestCase (
    """SELECT MIN(testfloat), testbyte FROM test_table GROUP BY testbyte""",
    Seq(Row(null, null), Row(-1f, 0), Row(0f, 1)),
    s"""SELECT ( MIN ( "SQ_1"."SQ_1_COL_1" ) ) AS "SQ_2_COL_0" ,
       | ( "SQ_1"."SQ_1_COL_0" ) AS "SQ_2_COL_1"
       | FROM ( SELECT ( "SQ_0"."TESTBYTE" ) AS "SQ_1_COL_0" ,
       | ( "SQ_0"."TESTFLOAT" ) AS "SQ_1_COL_1"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" GROUP BY "SQ_1"."SQ_1_COL_0"""".stripMargin
  )

  test("Test Min aggregation statements", P0Test, P1Test) {
    doTest(sqlContext, testMin)
    doTest(sqlContext, testMinGroupBy)
  }

  val testAvg: TestCase = TestCase(
    """SELECT AVG(testdouble) FROM test_table""",
    Seq(Row(0.0)),
    s"""SELECT ( AVG ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."TESTDOUBLE" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin
  )

  val testAvgGroupBy: TestCase = TestCase (
    """SELECT AVG(testint), testbyte FROM test_table GROUP BY testbyte""",
    Seq(Row(null, null), Row(4141214f, 0), Row(42f, 1)),
    s"""SELECT ( AVG ( "SQ_1"."SQ_1_COL_1" ::FLOAT ) ) AS "SQ_2_COL_0" ,
       | ( "SQ_1"."SQ_1_COL_0" ) AS "SQ_2_COL_1"
       | FROM ( SELECT ( "SQ_0"."TESTBYTE" ) AS "SQ_1_COL_0" ,
       | ( "SQ_0"."TESTINT" ) AS "SQ_1_COL_1"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" GROUP BY "SQ_1"."SQ_1_COL_0"""".stripMargin
  )

  test("Test Avg aggregation statements", P0Test, P1Test) {
    doTest(sqlContext, testAvg)
    doTest(sqlContext, testAvgGroupBy)
  }

  val testSum: TestCase = TestCase(
    """SELECT SUM(testfloat) FROM test_table""",
    Seq(Row(100000.0)),
    s"""SELECT ( SUM ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."TESTFLOAT" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin
  )

  val testSumGroupBy: TestCase = TestCase (
    """SELECT SUM(testint), testbyte FROM test_table GROUP BY testbyte""",
    Seq(Row(null, null), Row(4141214, 0), Row(84, 1)),
    s"""SELECT ( SUM ( "SQ_1"."SQ_1_COL_1" ) ) AS "SQ_2_COL_0" ,
       | ( "SQ_1"."SQ_1_COL_0" ) AS "SQ_2_COL_1"
       | FROM ( SELECT ( "SQ_0"."TESTBYTE" ) AS "SQ_1_COL_0" ,
       | ( "SQ_0"."TESTINT" ) AS "SQ_1_COL_1"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" GROUP BY "SQ_1"."SQ_1_COL_0"""".stripMargin
  )

  test("Test Sum aggregation statements", P0Test, P1Test) {
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
         |CAST ( "SQ_2"."SQ_2_COL_1" AS DECIMAL(5, 2) ) * POW(10, 2 ) ) ) / POW(10, 2 ) )
         |AS DECIMAL( 15 , 2 ) ) ) AS "SQ_3_COL_0" ,
         |( "SQ_2"."SQ_2_COL_0" ) AS "SQ_3_COL_1"
         |FROM ( SELECT ( "SQ_1"."TESTBYTE" ) AS "SQ_2_COL_0" ,
         |( "SQ_1"."TESTINT" ) AS "SQ_2_COL_1" FROM ( SELECT * FROM
         |( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0"
         |WHERE ( ( "SQ_0"."TESTBYTE" IS NOT NULL )
         |AND ( "SQ_0"."TESTBYTE" > 0 ) ) ) AS "SQ_1" )
         |AS "SQ_2" GROUP BY "SQ_2"."SQ_2_COL_0"
         |""".stripMargin
    )
  }
}

class TextPushdownAggregateSuite extends PushdownAggregateSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "true"
}

class ParquetPushdownAggregateSuite extends PushdownAggregateSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "true"
}

class TextNoPushdownAggregateSuite extends PushdownAggregateSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "false"
}

class ParquetNoPushdownAggregateSuite extends PushdownAggregateSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}

class TextNoCachePushdownAggregateSuite
  extends TextPushdownAggregateSuite {
  override protected val s3_result_cache = "false"
}

class ParquetNoCachePushdownAggregateSuite
  extends ParquetPushdownAggregateSuite {
  override protected val s3_result_cache = "false"
}
