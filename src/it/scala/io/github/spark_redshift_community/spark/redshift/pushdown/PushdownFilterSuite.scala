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

import io.github.spark_redshift_community.spark.redshift.TestUtils
import org.apache.spark.sql.Row

abstract class PushdownFilterSuite extends IntegrationPushdownSuiteBase {

  test("String equal filter pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql("""SELECT * FROM test_table WHERE teststring='asdf'"""),
      Seq(Row(1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42,
        1239012341823719L, -13.toShort, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0)),
      ))

    checkSqlStatement(s"""SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
         |( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'asdf\\' ) )""".stripMargin
    )
  }

  test("String LIKE start with filter pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql("""SELECT * FROM test_table WHERE teststring LIKE 'asd%'"""),
      Seq(Row(1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42,
        1239012341823719L, -13.toShort, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0)),
      ))

    checkSqlStatement(s"""SELECT * FROM ( SELECT * FROM $test_table
                         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
                         |( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
                         |AND ( CAST ( "SUBQUERY_0"."TESTSTRING" AS VARCHAR )
                         |LIKE CONCAT(\\'asd\\', \\'%\\') ) )""".stripMargin
    )
  }

  test("String LIKE contain filter pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql("""SELECT * FROM test_table WHERE teststring LIKE '%sd%'"""),
      Seq(Row(1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42,
        1239012341823719L, -13.toShort, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0)),
      ))

    checkSqlStatement(s"""SELECT * FROM ( SELECT * FROM $test_table
                         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
                         |( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
                         |AND ( CAST ( "SUBQUERY_0"."TESTSTRING" AS VARCHAR )
                         |LIKE CONCAT(\\'%\\', CONCAT(\\'sd\\', \\'%\\') ) ) )""".stripMargin
    )
  }

  test("String LIKE end with filter pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql("""SELECT * FROM test_table WHERE teststring LIKE '%sdf'"""),
      Seq(Row(1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42,
        1239012341823719L, -13.toShort, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0)),
      ))

    checkSqlStatement(s"""SELECT * FROM ( SELECT * FROM $test_table
                         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
                         |( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
                         |AND ( CAST ( "SUBQUERY_0"."TESTSTRING" AS VARCHAR )
                         |LIKE CONCAT(\\'%\\', \\'sdf\\' ) ) )""".stripMargin
    )
  }

  test("String LIKE % in the middle filter pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql("""SELECT * FROM test_table WHERE teststring LIKE 'as%f'"""),
      Seq(Row(1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42,
        1239012341823719L, -13.toShort, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0)),
      ))

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( ( LENGTH ( "SUBQUERY_0"."TESTSTRING" ) >= 3 )
         |AND ( ( CAST( "SUBQUERY_0"."TESTSTRING" AS VARCHAR ) LIKE CONCAT(\\'as\\', \\'%\\' ) )
         |AND ( CAST( "SUBQUERY_0"."TESTSTRING" AS VARCHAR ) LIKE CONCAT(\\'%\\', \\'f\\') )
         |) ) )""".stripMargin
    )
  }

  test("Integer equal filter pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql("""SELECT testint FROM test_table WHERE testint=4141214"""),
      Seq(Row(4141214))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTINT" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTINT" = 4141214 ) ) ) AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("GreaterThan filter pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql("""SELECT testint FROM test_table WHERE testint>45"""),
      Seq(Row(4141214))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTINT" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTINT" > 45 ) ) ) AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("GreaterThanOrEqual filter pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql("""SELECT testint FROM test_table WHERE testint>=45"""),
      Seq(Row(4141214))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTINT" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTINT" >= 45 ) ) ) AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("LessThan filter pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql("""SELECT testshort FROM test_table WHERE testshort<0"""),
      Seq(Row(-13))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTSHORT" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSHORT" < 0 ) ) ) AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("LessThanOrEqual filter pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql("""SELECT testshort FROM test_table WHERE testshort<=0"""),
      Seq(Row(-13))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTSHORT" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSHORT" <= 0 ) ) ) AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("Between filter pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql("""SELECT testshort FROM test_table WHERE testshort BETWEEN -15 AND 0"""),
      Seq(Row(-13))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTSHORT" IS NOT NULL )
         |AND ( ( "SUBQUERY_0"."TESTSHORT" >= -15 )
         |AND ( "SUBQUERY_0"."TESTSHORT" <= 0 ) ) ) ) AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("IN filter pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql("""SELECT testshort FROM test_table WHERE testshort IN (-15, -13, 0)"""),
      Seq(Row(-13))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE "SUBQUERY_0"."TESTSHORT" IN (-15,-13,0) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("AND filter pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT testshort FROM test_table
          |WHERE testshort > -15 AND testshort < 0""".stripMargin),
      Seq(Row(-13))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTSHORT" IS NOT NULL )
         |AND ( ( "SUBQUERY_0"."TESTSHORT" > -15 )
         |AND ( "SUBQUERY_0"."TESTSHORT" < 0 ) ) ) ) AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("OR filter pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT testshort, testint FROM test_table
          |WHERE testshort < 0 OR testint > 45""".stripMargin),
      Seq(Row(null, 4141214), Row(-13, 42))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_0" ,
         |( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_1"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTSHORT" < 0 )
         |OR ( "SUBQUERY_0"."TESTINT" > 45 ) ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("IS NULL filter pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT testshort, testint FROM test_table WHERE testshort IS NULL""".stripMargin),
      Seq(Row(null, null), Row(null, 4141214))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_0" ,
         |( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_1"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( "SUBQUERY_0"."TESTSHORT" IS NULL ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("IS NOT NULL filter pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT testshort FROM test_table WHERE testshort IS NOT NULL""".stripMargin),
      Seq(Row(24), Row(-13), Row(23))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( "SUBQUERY_0"."TESTSHORT" IS NOT NULL ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("NOT filter pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT testlong FROM test_table WHERE NOT testlong = 1239012341823719""".stripMargin),
      Seq()
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTLONG" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTLONG" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTLONG" != 1239012341823719 ) ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("Combined condition NOT filter optimized", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT teststring, testfloat FROM test_table
          |WHERE NOT (teststring = 'asdf'
          |OR testfloat > 0)""".stripMargin),
      Seq(Row("f", -1))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTSTRING" ) AS "SUBQUERY_2_COL_0" ,
         |( "SUBQUERY_1"."TESTFLOAT" ) AS "SUBQUERY_2_COL_1"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTFLOAT" IS NOT NULL ) )
         |AND ( ( "SUBQUERY_0"."TESTSTRING" != \\'asdf\\' )
         |AND ( "SUBQUERY_0"."TESTFLOAT" <= 0.0::float4 ) ) ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("Filter NOT LIKE filter pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT testlong FROM test_table WHERE NOT teststring LIKE 'asd%'""".stripMargin),
      Seq(Row(1239012341823719L), Row(1239012341823719L), Row(1239012341823719L))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTLONG" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM (
         |SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND NOT ( ( CAST ( "SUBQUERY_0"."TESTSTRING" AS VARCHAR )
         |LIKE CONCAT(\\'asd\\', \\'%\\') ) ) ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("Filter EqualNullSafe not supported: operator <=>", P0Test, P1Test) {

    conn.createStatement().executeUpdate(
      s""" create table $test_table_safe_null(c1 varchar, c2 varchar)""")
    conn.createStatement().executeUpdate(
      s"insert into $test_table_safe_null values(null, null), ('a', null), ('a', 'a')")

    read
      .option("dbtable", test_table_safe_null)
      .load()
      .createOrReplaceTempView("test_table_safenull")

    checkAnswer(
      sqlContext.sql("""select * from test_table_safenull where c1 <=> c2"""),
      Seq(Row(null, null), Row("a", "a"))
    )
    checkSqlStatement(
      s"""SELECT "c1", "c2" FROM $test_table_safe_null"""
    )

  }

  val testCountGroupBy2: TestCase = TestCase (
    """SELECT COUNT(testbyte), testbyte FROM test_table GROUP BY testbyte""",
    Seq(Row(0, null), Row(2, 0), Row(2, 1)),
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0" ,
       |( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_1"
       |FROM ( SELECT ( "SUBQUERY_0"."TESTBYTE" ) AS "SUBQUERY_1_COL_0"
       |FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       |AS "SUBQUERY_1" GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0"
       |""".stripMargin
  )

  val testAnd0: TestCase = TestCase(
    """SELECT testbyte, testint FROM test_table WHERE testbyte = 1 AND testint = 42""",
    Seq(Row(1, 42), Row(1, 42)),
    s"""SELECT("SUBQUERY_1"."TESTBYTE")AS"SUBQUERY_2_COL_0",
       |("SUBQUERY_1"."TESTINT")AS"SUBQUERY_2_COL_1"
       |FROM(SELECT*FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"WHERE(
       |(("SUBQUERY_0"."TESTBYTE"ISNOTNULL)AND
       |("SUBQUERY_0"."TESTINT"ISNOTNULL))AND
       |(("SUBQUERY_0"."TESTBYTE"=1)AND
       |("SUBQUERY_0"."TESTINT"=42))))AS"SUBQUERY_1"""".stripMargin
       )

  val testAnd1: TestCase = TestCase(
  """SELECT testdate, testfloat FROM test_table WHERE testdate = DATE '2015-07-03'
      |and testfloat = -1.0""".stripMargin,
    Seq(Row(TestUtils.toDate(2015, 6, 3), -1.0)),
    s"""SELECT("SUBQUERY_1"."TESTDATE")AS"SUBQUERY_2_COL_0",
       |("SUBQUERY_1"."TESTFLOAT")AS"SUBQUERY_2_COL_1"
       |FROM(SELECT*FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"
       |WHERE((("SUBQUERY_0"."TESTDATE"ISNOTNULL)AND
       |("SUBQUERY_0"."TESTFLOAT"ISNOTNULL))AND
       |(("SUBQUERY_0"."TESTDATE"=DATEADD(day,16619,TO_DATE(\\'1970-01-01\\',\\'YYYY-MM-DD\\')))
       |AND("SUBQUERY_0"."TESTFLOAT"=-1.0::float4))))AS"SUBQUERY_1"""".stripMargin
  )

  val testAnd2: TestCase = TestCase(
    sparkStatement =
      """SELECT testbool, testdouble FROM test_table WHERE testbool = true
        |AND testdouble = 1234152.12312498""".stripMargin,
    expectedResult = Seq(Row(true, 1234152.12312498)),
      s"""SELECT("SUBQUERY_1"."TESTBOOL")AS"SUBQUERY_2_COL_0",
       |("SUBQUERY_1"."TESTDOUBLE")AS"SUBQUERY_2_COL_1"FROM
       |(SELECT*FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"WHERE
       |((("SUBQUERY_0"."TESTBOOL"ISNOTNULL)AND
       |("SUBQUERY_0"."TESTDOUBLE"ISNOTNULL))AND
       |(("SUBQUERY_0"."TESTBOOL"=true)AND
       |("SUBQUERY_0"."TESTDOUBLE"=1234152.12312498))))AS"SUBQUERY_1"""".stripMargin,
      s"""SELECT("SUBQUERY_1"."TESTBOOL")AS"SUBQUERY_2_COL_0",
       |("SUBQUERY_1"."TESTDOUBLE")AS"SUBQUERY_2_COL_1"FROM
       |(SELECT*FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"WHERE
       |((("SUBQUERY_0"."TESTBOOL"ISNOTNULL)AND
       |("SUBQUERY_0"."TESTDOUBLE"ISNOTNULL))AND
       |("SUBQUERY_0"."TESTBOOL"AND
       |("SUBQUERY_0"."TESTDOUBLE"=1234152.12312498))))AS"SUBQUERY_1"""".stripMargin,
  )

  val testAnd3: TestCase = TestCase(
    """SELECT testshort, teststring FROM test_table WHERE testshort = 24 AND
      |teststring = '___|_123'""".stripMargin,
    Seq(Row(24, "___|_123")),
    s"""SELECT("SUBQUERY_1"."TESTSHORT")AS"SUBQUERY_2_COL_0",
       |("SUBQUERY_1"."TESTSTRING")AS"SUBQUERY_2_COL_1"FROM
       |(SELECT*FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"
       |WHERE((("SUBQUERY_0"."TESTSHORT"ISNOTNULL)AND
       |("SUBQUERY_0"."TESTSTRING"ISNOTNULL))AND
       |(("SUBQUERY_0"."TESTSHORT"=24)AND
       |("SUBQUERY_0"."TESTSTRING"=\\'___|_123\\'))))AS"SUBQUERY_1"""".stripMargin
  )

  val testAnd4: TestCase = TestCase(
    s"""SELECT testshort FROM test_table WHERE testbyte IS NULL and testbool IS NULL""",
    Seq(Row(null)),
    s"""SELECT("SUBQUERY_1"."TESTSHORT")AS"SUBQUERY_2_COL_0"FROM
       |(SELECT*FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"WHERE
       |(("SUBQUERY_0"."TESTBYTE"ISNULL)AND
       |("SUBQUERY_0"."TESTBOOL"ISNULL)))AS"SUBQUERY_1"""".stripMargin
  )

  val testAnd5: TestCase = TestCase(
    s"""SELECT COUNT(*) FROM test_table WHERE testbyte IS NOT NULL and testbool IS NULL""",
    Seq(Row(1)),
    s"""SELECT(COUNT(1))AS"SUBQUERY_2_COL_0"FROM(SELECT*FROM(
       |SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"WHERE
       |(("SUBQUERY_0"."TESTBYTE"ISNOTNULL)AND
       |("SUBQUERY_0"."TESTBOOL"ISNULL)))AS"SUBQUERY_1"LIMIT1""".stripMargin
  )

  val testAnd6: TestCase = TestCase(
    s"""SELECT COUNT(*) FROM test_table WHERE testbyte < testint AND testint < testlong""",
    Seq(Row(3)),
    s"""SELECT(COUNT(1))AS"SUBQUERY_2_COL_0"FROM(SELECT*FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"WHERE(
       |((("SUBQUERY_0"."TESTBYTE"ISNOTNULL)AND
       |("SUBQUERY_0"."TESTINT"ISNOTNULL))AND
       |("SUBQUERY_0"."TESTLONG"ISNOTNULL))AND
       |(("SUBQUERY_0"."TESTBYTE"<"SUBQUERY_0"."TESTINT")AND
       |(CAST("SUBQUERY_0"."TESTINT"ASBIGINT)<"SUBQUERY_0"."TESTLONG"))))
       |AS"SUBQUERY_1"LIMIT1""".stripMargin
       )

  val testAnd7: TestCase = TestCase(
    s"""SELECT COUNT(*) FROM test_table WHERE teststring LIKE 'Unicode%' AND testint > 22""",
    Seq(Row(1)),
    s"""SELECT ( COUNT ( 1 ) ) AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTINT" IS NOT NULL ) ) AND
       | ( ( CAST ( "SUBQUERY_0"."TESTSTRING" AS VARCHAR ) LIKE CONCAT (\\'Unicode\\', \\'%\\') ) AND
       | ("SUBQUERY_0"."TESTINT" > 22 ) ) ) ) AS "SUBQUERY_1" LIMIT 1""".stripMargin
  )

  val testAnd8: TestCase = TestCase(
    s"""SELECT COUNT(*) FROM test_table WHERE 1=1 AND 2=2""",
    Seq(Row(5)),
    s"""SELECT(COUNT(1))AS"SUBQUERY_1_COL_0"FROM(
       |SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"LIMIT1""".stripMargin
  )

  val testOr0: TestCase = TestCase(
    """SELECT testbyte, testtimestamp FROM test_table WHERE
      |testtimestamp = '2015-07-01 00:00:00.001' OR
      |testbyte IS NOT NULL ORDER BY testbyte""".stripMargin,
    Seq(
      Row(0, TestUtils.toTimestamp(year = 2015, zeroBasedMonth = 6, date = 3, hour = 12,
        minutes = 34, seconds = 56, millis = 0)),
      Row(0, null),
      Row(1, TestUtils.toTimestamp(year = 2015, zeroBasedMonth = 6, date = 2, hour = 0,
        minutes = 0, seconds = 0, millis = 0)),
      Row(1, TestUtils.toTimestamp(year = 2015, zeroBasedMonth = 6, date = 1, hour = 0,
        minutes = 0, seconds = 0, millis = 1)),
    ),
    s"""SELECT*FROM
       |(SELECT("SUBQUERY_1"."TESTBYTE")AS"SUBQUERY_2_COL_0",
       |("SUBQUERY_1"."TESTTIMESTAMP")AS"SUBQUERY_2_COL_1"FROM
       |(SELECT*FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"WHERE
       |(("SUBQUERY_0"."TESTTIMESTAMP"=\\'2015-07-0100:00:00.001\\'::TIMESTAMP)OR
       |("SUBQUERY_0"."TESTBYTE"ISNOTNULL)))AS"SUBQUERY_1")
       |AS"SUBQUERY_2"ORDERBY("SUBQUERY_2"."SUBQUERY_2_COL_0")ASCNULLSFIRST""".stripMargin
  )

  val testOr1: TestCase = TestCase(
    s"""SELECT testdate FROM test_table WHERE false OR testdate < DATE '2022-11-11'""",
    Seq(Row(TestUtils.toDate(2015, 6, 1)),
      Row(TestUtils.toDate(2015, 6, 2)),
      Row(TestUtils.toDate(2015, 6, 3))
    ),
    s"""SELECT("SUBQUERY_1"."TESTDATE")AS"SUBQUERY_2_COL_0"FROM
       |(SELECT*FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"WHERE
       |(("SUBQUERY_0"."TESTDATE"ISNOTNULL)AND
       |("SUBQUERY_0"."TESTDATE"<
       |DATEADD(day,19307,TO_DATE(\\'1970-01-01\\',\\'YYYY-MM-DD\\')))))
       |AS"SUBQUERY_1"""".stripMargin
  )

  val testOr2: TestCase = TestCase(
    s"""SELECT COUNT(*) FROM test_table WHERE true OR teststring = 'DNE'""",
    Seq(Row(5)),
    s"""SELECT(COUNT(1))AS"SUBQUERY_1_COL_0"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS
       |"SUBQUERY_0"LIMIT1""".stripMargin
  )

  val testOr3: TestCase = TestCase(
    s"""SELECT COUNT(*) FROM test_table WHERE testbyte IS NOT NULL OR teststring IS NOT NULL""",
    Seq(Row(4)),
    s"""SELECT(COUNT(1))AS"SUBQUERY_2_COL_0"FROM
       |(SELECT*FROM(SELECT*FROM$test_table
       |AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"WHERE
       |(("SUBQUERY_0"."TESTBYTE"ISNOTNULL)OR
       |("SUBQUERY_0"."TESTSTRING"ISNOTNULL)))
       |AS"SUBQUERY_1"LIMIT1""".stripMargin
  )

  val testOr4: TestCase = TestCase(
    s"""SELECT COUNT(*) FROM test_table WHERE 1=0 OR 1=1""",
    Seq(Row(5)),
    s"""SELECT(COUNT(1))AS"SUBQUERY_1_COL_0"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"LIMIT1""".stripMargin
  )


  val testBinaryPlus0: TestCase = TestCase(
    s"""SELECT 1 + testbyte from test_table ORDER BY testbyte ASC NULLS LAST""",
    Seq(Row(1), Row(1), Row(2), Row(2), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((1+"SUBQUERY_0"."TESTBYTE"))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTBYTE")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryPlus1: TestCase = TestCase(
    s"""SELECT 1.0 + testdouble FROM test_table""",
    Seq(Row(-1234151.12312498), Row(1.0), Row(1.0), Row(1234153.12312498), Row(null)),
    s"""SELECT((1.0+"SUBQUERY_0"."TESTDOUBLE"))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinaryPlus2: TestCase = TestCase(
    s"""SELECT 1.0 + testfloat FROM test_table""",
    Seq(Row(0.0), Row(1.0), Row(100001.0), Row(2.0), Row(null)),
    s"""SELECT((1.0+CAST("SUBQUERY_0"."TESTFLOAT"ASFLOAT8)))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinaryPlus3: TestCase = TestCase(
    s"""SELECT 1 + testint from test_table ORDER BY testint ASC NULLS LAST""",
    Seq(Row(43), Row(43), Row(4141215), Row(null), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((1+"SUBQUERY_0"."TESTINT"))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTINT")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryPlus4: TestCase = TestCase(
    s"""SELECT 1 + testlong from test_table ORDER BY testlong ASC NULLS LAST""",
    Seq(
      Row(1239012341823720L),
      Row(1239012341823720L),
      Row(1239012341823720L),
      Row(1239012341823720L),
      Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((1+"SUBQUERY_0"."TESTLONG"))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTLONG")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryPlus5: TestCase = TestCase(
    s"""SELECT 1 + testdate FROM test_table ORDER BY testdate ASC NULLS LAST""",
    Seq(
      Row(TestUtils.toDate(2015, 6, 2)),
      Row(TestUtils.toDate(2015, 6, 3)),
      Row(TestUtils.toDate(2015, 6, 4)),
      Row(null),
      Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(DATEADD(day,1,"SUBQUERY_0"."TESTDATE"))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTDATE")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS
       |"SUBQUERY_2"""".stripMargin
  )

  val testBinaryPlus6: TestCase = TestCase( // Redshift-6994
    s"""SELECT 'hello' + teststring FROM test_table ORDER BY teststring ASC NULLS LAST""",
    Seq(Row("Failed")),
    s"""Expect failure"""
  )

  val testBinaryPlus7: TestCase = TestCase( // Redshift-6996
    s"""SELECT 1 + testtimestamp FROM test_table ORDER BY testtimestamp ASC NULLS LAST""",
    Seq(Row("Failed")),
    s"""Expect failure"""
  )

  val testBinaryPlus8: TestCase = TestCase(
    s"""select 32767 + testbyte from test_table order by testbyte ASC NULLS LAST""",
    Seq(Row(32767), Row(32767), Row(32768), Row(32768), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((32767+"SUBQUERY_0"."TESTBYTE"))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTBYTE")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryPlus9: TestCase = TestCase(
    s"""select -32767 + testbyte from test_table order by testbyte ASC NULLS LAST""",
    Seq(Row(-32767), Row(-32767), Row(-32766), Row(-32766), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((-32767+"SUBQUERY_0"."TESTBYTE"))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTBYTE")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS
       |"SUBQUERY_2"""".stripMargin
  )

  val testBinaryPlus10: TestCase = TestCase(
    s"""SELECT 1.0 + testdouble FROM test_table""",
    Seq(Row(-1234151.12312498), Row(1.0), Row(1.0), Row(1234153.12312498), Row(null)),
    s"""SELECT((1.0+"SUBQUERY_0"."TESTDOUBLE"))AS"SUBQUERY_1_COL_0"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinaryPlus11: TestCase = TestCase(
    s"""SELECT -1 + testbyte from test_table ORDER BY testbyte ASC NULLS LAST""",
    Seq(Row(-1), Row(-1), Row(0), Row(0), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((-1+"SUBQUERY_0"."TESTBYTE"))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTBYTE")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryPlus12: TestCase = TestCase(
    s"""SELECT -999.0 + testdouble FROM test_table""",
    Seq(Row(-1235151.12312498), Row(-999.0), Row(-999.0), Row(1233153.12312498), Row(null)),
    s"""SELECT((-999.0+"SUBQUERY_0"."TESTDOUBLE"))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinaryPlus13: TestCase = TestCase(
    s"""SELECT -1.0 + testfloat FROM test_table""",
    Seq(Row(-1.0), Row(-2.0), Row(0.0), Row(99999.0), Row(null)),
    s"""SELECT((-1.0+CAST("SUBQUERY_0"."TESTFLOAT"ASFLOAT8)))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinaryPlus14: TestCase = TestCase(
    s"""SELECT -1 + testint from test_table ORDER BY testint ASC NULLS LAST""",
    Seq(Row(41), Row(41), Row(4141213), Row(null), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((-1+"SUBQUERY_0"."TESTINT"))AS"SUBQUERY_1_COL_0",
       |("SUBQUERY_0"."TESTINT")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryPlus15: TestCase = TestCase(
    s"""SELECT -1 + testlong from test_table ORDER BY testlong ASC NULLS LAST""",
    Seq(
      Row(1239012341823718L),
      Row(1239012341823718L),
      Row(1239012341823718L),
      Row(1239012341823718L),
      Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((-1+"SUBQUERY_0"."TESTLONG"))AS"SUBQUERY_1_COL_0",
       |("SUBQUERY_0"."TESTLONG")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryPlus16: TestCase = TestCase(
    s"""SELECT -1 + testdate FROM test_table ORDER BY testdate ASC NULLS LAST""",
    Seq(
      Row(TestUtils.toDate(2015, 5, 30)),
      Row(TestUtils.toDate(2015, 6, 1)),
      Row(TestUtils.toDate(2015, 6, 2)),
      Row(null),
      Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(DATEADD(day,-1,"SUBQUERY_0"."TESTDATE"))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTDATE")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryPlus17: TestCase = TestCase(
    s"""SELECT -900000000 + testlong from test_table ORDER BY testlong ASC NULLS LAST""",
    Seq(
      Row(1239011441823719L),
      Row(1239011441823719L),
      Row(1239011441823719L),
      Row(1239011441823719L),
      Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM(
       |SELECT*FROM(SELECT((-900000000+"SUBQUERY_0"."TESTLONG"))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTLONG")AS"SUBQUERY_1_COL_1"
       |FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryPlus18: TestCase = TestCase(
    s"""SELECT 1234567890123456789 + testlong from test_table ORDER BY testlong ASC NULLS LAST""",
    Seq(
      Row(1235806902465280508L),
      Row(1235806902465280508L),
      Row(1235806902465280508L),
      Row(1235806902465280508L),
      Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((1234567890123456789+"SUBQUERY_0"."TESTLONG"))
       |AS"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTLONG")AS"SUBQUERY_1_COL_1"
       |FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryPlus19: TestCase = TestCase(
    s"""select -32767 + testbyte from test_table order by testbyte ASC NULLS LAST""",
    Seq(Row(-32767), Row(-32767), Row(-32766), Row(-32766), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((-32767+"SUBQUERY_0"."TESTBYTE"))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTBYTE")AS"SUBQUERY_1_COL_1"
       |FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryPlus20: TestCase = TestCase(
    s"""select -32767 + testbyte from test_table order by testbyte ASC NULLS LAST""",
    Seq(Row(-32767), Row(-32767), Row(-32766), Row(-32766), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((-32767+"SUBQUERY_0"."TESTBYTE"))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTBYTE")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryPlus21: TestCase = TestCase(
    s"""SELECT -1.0 + testdouble FROM test_table""",
    Seq(Row(-1.0), Row(-1.0), Row(-1234153.12312498), Row(1234151.12312498), Row(null)),
    s"""SELECT((-1.0+"SUBQUERY_0"."TESTDOUBLE"))AS"SUBQUERY_1_COL_0"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinarySubtract0: TestCase = TestCase(
    s"""SELECT testbyte - 1 from test_table ORDER BY testbyte ASC NULLS LAST""",
    Seq(Row(-1), Row(-1), Row(0), Row(0), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTBYTE"-1))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTBYTE")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinarySubtract1: TestCase = TestCase(
    s"""SELECT testdate - 1 FROM test_table ORDER BY testdate ASC NULLS LAST""",
    Seq(
      Row(TestUtils.toDate(2015, 5, 30)),
      Row(TestUtils.toDate(2015, 6, 1)),
      Row(TestUtils.toDate(2015, 6, 2)),
      Row(null),
      Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(DATEADD(day,(0-(1)),"SUBQUERY_0"."TESTDATE"))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTDATE")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS
       |"SUBQUERY_2"""".stripMargin
  )

  val testBinarySubtract2: TestCase = TestCase( // Redshift-6996
    s"""SELECT testtimestamp - 1 FROM test_table ORDER BY testtimestamp ASC NULLS LAST""",
    Seq(Row("Failed")),
    s"""Expect failure"""
  )

  val testBinarySubtract3: TestCase = TestCase(
    s"""SELECT testdouble - 1.0 FROM test_table""",
    Seq(Row(-1.0), Row(-1.0), Row(-1234153.12312498), Row(1234151.12312498), Row(null)),
    s"""SELECT(("SUBQUERY_0"."TESTDOUBLE"-1.0))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinarySubtract4: TestCase = TestCase(
    s"""SELECT testfloat - 1.0 FROM test_table""",
    Seq(Row(-1.0), Row(-2.0), Row(0.0), Row(99999.0), Row(null)),
    s"""SELECT((CAST("SUBQUERY_0"."TESTFLOAT"ASFLOAT8)-1.0))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinarySubtract5: TestCase = TestCase(
    s"""SELECT testint - 1 from test_table ORDER BY testint ASC NULLS LAST""",
    Seq(Row(41), Row(41), Row(4141213), Row(null), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTINT"-1))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTINT")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinarySubtract6: TestCase = TestCase(
    s"""SELECT testlong - 1 from test_table ORDER BY testlong ASC NULLS LAST""",
    Seq(
      Row(1239012341823718L),
      Row(1239012341823718L),
      Row(1239012341823718L),
      Row(1239012341823718L),
      Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTLONG"-1))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTLONG")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinarySubtract7: TestCase = TestCase(
    s"""SELECT testbyte - 32767 from test_table ORDER BY testbyte ASC NULLS LAST""",
    Seq(Row(-32767), Row(-32767), Row(-32766), Row(-32766), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTBYTE"-32767))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTBYTE")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinarySubtract8: TestCase = TestCase(
    s"""SELECT testdouble - 1.5 FROM test_table""",
    Seq(Row(-1.5), Row(-1.5), Row(-1234153.62312498), Row(1234150.62312498), Row(null)),
    s"""SELECT(("SUBQUERY_0"."TESTDOUBLE"-1.5))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinarySubtract9: TestCase = TestCase(
    s"""SELECT testfloat - 1.5 FROM test_table""",
    Seq(Row(-0.5), Row(-1.5), Row(-2.5), Row(99998.5), Row(null)),
    s"""SELECT((CAST("SUBQUERY_0"."TESTFLOAT"ASFLOAT8)-1.5))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinarySubtract10: TestCase = TestCase(
    s"""SELECT testint - 2147483648 from test_table ORDER BY testint ASC NULLS LAST""",
    Seq(Row(-2147483606), Row(-2147483606), Row(-2143342434), Row(null), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((CAST("SUBQUERY_0"."TESTINT"ASBIGINT)-2147483648))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTINT")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinarySubtract11: TestCase = TestCase(
    sparkStatement =
      s"""SELECT testlong - 9223372036854775808 from test_table ORDER BY testlong
         |ASC NULLS LAST""".stripMargin,
    expectedResult = Seq(
      Row(-9222133024512952089L),
      Row(-9222133024512952089L),
      Row(-9222133024512952089L),
      Row(-9222133024512952089L),
      Row(null)),
        s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT
       |(CAST((CAST(CAST("SUBQUERY_0"."TESTLONG"ASDECIMAL(20,0))
       |ASDECIMAL(21,0))-9223372036854775808)ASDECIMAL(21,0)))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTLONG")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin,
        s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT
       |(CAST((CAST("SUBQUERY_0"."TESTLONG"ASDECIMAL(21,0))
       |-9223372036854775808)ASDECIMAL(21,0)))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTLONG")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin,
    s"""SELECT ( "SUBQUERY_2"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_3_COL_0" FROM
       |( SELECT * FROM ( SELECT
       |( ( CAST ( "SUBQUERY_0"."TESTLONG" AS DECIMAL(20, 0) )
       |- 9223372036854775808 ) ) AS "SUBQUERY_1_COL_0" ,
       |( "SUBQUERY_0"."TESTLONG" ) AS "SUBQUERY_1_COL_1"
       |FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       |AS "SUBQUERY_0" ) AS "SUBQUERY_1" ORDER BY
       |( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) ASC NULLS LAST ) AS "SUBQUERY_2"""".stripMargin
  )

  val testBinarySubtract12: TestCase = TestCase(
    s"""SELECT testbyte - -1 from test_table ORDER BY testbyte ASC NULLS LAST""",
    Seq(Row(1), Row(1), Row(2), Row(2), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTBYTE"--1))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTBYTE")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinarySubtract13: TestCase = TestCase(
    s"""SELECT testdate - -1 FROM test_table ORDER BY testdate ASC NULLS LAST""",
    Seq(
      Row(TestUtils.toDate(2015, 6, 2)),
      Row(TestUtils.toDate(2015, 6, 3)),
      Row(TestUtils.toDate(2015, 6, 4)),
      Row(null),
      Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(DATEADD(day,(0-(-1)),"SUBQUERY_0"."TESTDATE"))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTDATE")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS
       |"SUBQUERY_2"""".stripMargin
  )

  val testBinarySubtract14: TestCase = TestCase(
    s"""SELECT testdate - -90 FROM test_table ORDER BY testdate ASC NULLS LAST""",
    Seq(
      Row(TestUtils.toDate(2015, 8, 29)),
      Row(TestUtils.toDate(2015, 8, 30)),
      Row(TestUtils.toDate(2015, 9, 1)),
      Row(null),
      Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(DATEADD(day,(0-(-90)),"SUBQUERY_0"."TESTDATE"))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTDATE")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS
       |"SUBQUERY_2"""".stripMargin
  )

  val testBinarySubtract15: TestCase = TestCase(
    s"""SELECT testdouble - -1.0 FROM test_table""",
    Seq(Row(-1234151.12312498), Row(1.0), Row(1.0), Row(1234153.12312498), Row(null)),
    s"""SELECT(("SUBQUERY_0"."TESTDOUBLE"--1.0))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinarySubtract16: TestCase = TestCase(
    s"""SELECT testfloat - -1.0 FROM test_table""",
    Seq(Row(0.0), Row(1.0), Row(100001.0), Row(2.0), Row(null)),
    s"""SELECT((CAST("SUBQUERY_0"."TESTFLOAT"ASFLOAT8)--1.0))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinarySubtract17: TestCase = TestCase(
    s"""SELECT testint - -1 from test_table ORDER BY testint ASC NULLS LAST""",
    Seq(Row(43), Row(43), Row(4141215), Row(null), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTINT"--1))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTINT")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinarySubtract18: TestCase = TestCase(
    s"""SELECT testlong - -1 from test_table ORDER BY testlong ASC NULLS LAST""",
    Seq(
      Row(1239012341823720L),
      Row(1239012341823720L),
      Row(1239012341823720L),
      Row(1239012341823720L),
      Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTLONG"--1))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTLONG")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinarySubtract19: TestCase = TestCase(
    s"""SELECT testbyte - -32767 from test_table ORDER BY testbyte ASC NULLS LAST""",
    Seq(Row(32767), Row(32767), Row(32768), Row(32768), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTBYTE"--32767))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTBYTE")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinarySubtract20: TestCase = TestCase(
    s"""SELECT testdouble - -1.5 FROM test_table""",
    Seq(Row(-1234150.62312498), Row(1.5), Row(1.5), Row(1234153.62312498), Row(null)),
    s"""SELECT(("SUBQUERY_0"."TESTDOUBLE"--1.5))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinarySubtract21: TestCase = TestCase(
    s"""SELECT testfloat - -1.5 FROM test_table""",
    Seq(Row(0.5), Row(1.5), Row(100001.5), Row(2.5), Row(null)),
    s"""SELECT((CAST("SUBQUERY_0"."TESTFLOAT"ASFLOAT8)--1.5))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinarySubtract22: TestCase = TestCase(
    s"""SELECT testint - -2147483647 from test_table ORDER BY testint ASC NULLS LAST""",
    Seq(Row(null), Row(null), Row(-2147483607), Row(-2147483607), Row(-2143342435)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((CAST("SUBQUERY_0"."TESTINT"ASBIGINT)--2147483647))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTINT")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinarySubtract23: TestCase = TestCase(
    s"""SELECT testlong - -9223372036854775808 from test_table ORDER BY testlong ASC NULLS LAST""",
    Seq(
      Row(null),
      Row(-9222133024512952089L),
      Row(-9222133024512952089L),
      Row(-9222133024512952089L),
      Row(-9222133024512952089L)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT
       |(CAST((CAST(CAST("SUBQUERY_0"."TESTLONG"ASDECIMAL(20,0))
       |ASDECIMAL(21,0))--9223372036854775808)ASDECIMAL(21,0)))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTLONG")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )


  val testBinaryMultiply0: TestCase = TestCase(
    s"""SELECT testbyte * 2 from test_table ORDER BY testbyte ASC NULLS LAST""",
    Seq(Row(0), Row(0), Row(2), Row(2), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTBYTE"*2))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTBYTE")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryMultiply1: TestCase = TestCase(
    s"""SELECT testint * -9 from test_table ORDER BY testint ASC NULLS LAST""",
    Seq(
      Row(-378),
      Row(-378),
      Row(-37270926),
      Row(null),
      Row(null),
    ),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTINT"*-9))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTINT")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryMultiply2: TestCase = TestCase(
    s"""SELECT testlong * -5000 from test_table ORDER BY testlong ASC NULLS LAST""",
    Seq(
      Row(-6195061709118595000L),
      Row(-6195061709118595000L),
      Row(-6195061709118595000L),
      Row(-6195061709118595000L),
      Row(null)
    ),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTLONG"*-5000))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTLONG")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS
       |"SUBQUERY_2"""".stripMargin
  )

  val testBinaryMultiply3: TestCase = TestCase(
    s"""SELECT testdouble * 2.0 FROM test_table""",
    Seq(Row(-2468304.24624996), Row(0.0), Row(0.0), Row(2468304.24624996), Row(null)),
    s"""SELECT(("SUBQUERY_0"."TESTDOUBLE"*2.0))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinaryMultiply4: TestCase = TestCase(
    s"""SELECT testfloat * 2.0 FROM test_table""",
    Seq(Row(-2.0), Row(0.0), Row(2.0), Row(200000.0), Row(null)),
    s"""SELECT((CAST("SUBQUERY_0"."TESTFLOAT"ASFLOAT8)*2.0))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinaryMultiply5: TestCase = TestCase(
    s"""SELECT testint * 2 from test_table ORDER BY testint ASC NULLS LAST""",
    Seq(Row(84), Row(84), Row(8282428), Row(null), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTINT"*2))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTINT")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryMultiply6: TestCase = TestCase(
    s"""SELECT testlong * 2 from test_table ORDER BY testlong ASC NULLS LAST""",
    Seq(
      Row(2478024683647438L),
      Row(2478024683647438L),
      Row(2478024683647438L),
      Row(2478024683647438L),
      Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTLONG"*2))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTLONG")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryMultiply7: TestCase = TestCase(
    s"""SELECT testbyte * 32767 from test_table ORDER BY testbyte ASC NULLS LAST""",
    Seq(Row(0), Row(0), Row(32767), Row(32767), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTBYTE"*32767))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTBYTE")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryMultiply8: TestCase = TestCase(
    s"""SELECT testdouble * 212.123 FROM test_table""",
    Seq(Row(-2.617920508136401E8), Row(0.0), Row(0.0), Row(2.617920508136401E8), Row(null)),
    s"""SELECT(("SUBQUERY_0"."TESTDOUBLE"*212.123))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinaryMultiply9: TestCase = TestCase(
    s"""SELECT testfloat * 123.45678 FROM test_table""",
    Seq(Row(-123.45678), Row(0.0), Row(1.2345678E7), Row(123.45678), Row(null)),
    s"""SELECT((CAST("SUBQUERY_0"."TESTFLOAT"ASFLOAT8)*123.45678))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinaryMultiply10: TestCase = TestCase(
    s"""SELECT testint * 2147483648 from test_table ORDER BY testint ASC NULLS LAST""",
    Seq(Row(90194313216L), Row(90194313216L), Row(8893189347868672L), Row(null), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((CAST("SUBQUERY_0"."TESTINT"ASBIGINT)*2147483648))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTINT")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryMultiply11: TestCase = TestCase(
    s"""SELECT testlong * 9223372 from test_table ORDER BY testlong ASC NULLS LAST""",
    Seq(
      Row(null),
      Row(-9109584468603241452L),
      Row(-9109584468603241452L),
      Row(-9109584468603241452L),
      Row(-9109584468603241452L),
    ),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(CAST((CAST("SUBQUERY_0"."TESTLONG"ASDECIMAL(21,0))*9223372)ASDECIMAL(21,0)))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTLONG")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryMultiply12: TestCase = TestCase(
    s"""SELECT testint * -9000 from test_table ORDER BY testint ASC NULLS LAST""",
    Seq(
      Row(-378),
      Row(-378),
      Row(-37270926),
      Row(null),
      Row(null),
    ),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTINT"*-9000))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTINT")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryMultiply13: TestCase = TestCase(
    s"""SELECT testbyte * -2 from test_table ORDER BY testbyte ASC NULLS LAST""",
    Seq(Row(0), Row(0), Row(-2), Row(-2), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTBYTE"*-2))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTBYTE")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryMultiply14: TestCase = TestCase(
    s"""SELECT testint * -98 from test_table ORDER BY testint ASC NULLS LAST""",
    Seq(
      Row(-4116),
      Row(-4116),
      Row(-405838972),
      Row(null),
      Row(null),
    ),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTINT"*-98))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTINT")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryMultiply15: TestCase = TestCase(
    s"""SELECT testlong * -5928 from test_table ORDER BY testlong ASC NULLS LAST""",
    Seq(
      Row(-7344865162331006232L),
      Row(-7344865162331006232L),
      Row(-7344865162331006232L),
      Row(-7344865162331006232L),
      Row(null)
    ),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTLONG"*-5928))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTLONG")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS
       |"SUBQUERY_2"""".stripMargin
  )

  val testBinaryMultiply16: TestCase = TestCase(
    s"""SELECT testdouble * -2.0 FROM test_table""",
    Seq(Row(-0.0), Row(-0.0), Row(-2468304.24624996), Row(2468304.24624996), Row(null)),
    s"""SELECT(("SUBQUERY_0"."TESTDOUBLE"*-2.0))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinaryMultiply17: TestCase = TestCase(
    s"""SELECT testfloat * -2.0 FROM test_table""",
    Seq(Row(-0.0), Row(-2.0), Row(-200000.0), Row(2.0), Row(null)),
    s"""SELECT((CAST("SUBQUERY_0"."TESTFLOAT"ASFLOAT8)*-2.0))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinaryMultiply18: TestCase = TestCase(
    s"""SELECT testint * -2 from test_table ORDER BY testint ASC NULLS LAST""",
    Seq(Row(-84), Row(-84), Row(-8282428), Row(null), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTINT"*-2))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTINT")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryMultiply19: TestCase = TestCase(
    s"""SELECT testlong * -2 from test_table ORDER BY testlong ASC NULLS LAST""",
    Seq(
      Row(-2478024683647438L),
      Row(-2478024683647438L),
      Row(-2478024683647438L),
      Row(-2478024683647438L),
      Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTLONG"*-2))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTLONG")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryMultiply20: TestCase = TestCase(
    s"""SELECT testbyte * -32767 from test_table ORDER BY testbyte ASC NULLS LAST""",
    Seq(Row(0), Row(0), Row(-32767), Row(-32767), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTBYTE"*-32767))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTBYTE")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryMultiply21: TestCase = TestCase(
    s"""SELECT testdouble * -212.123 FROM test_table""",
    Seq(Row(-0.0), Row(-0.0), Row(-2.617920508136401E8), Row(2.617920508136401E8), Row(null)),
    s"""SELECT(("SUBQUERY_0"."TESTDOUBLE"*-212.123))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinaryMultiply22: TestCase = TestCase(
    s"""SELECT testfloat * -123.45678 FROM test_table""",
    Seq(Row(123.45678), Row(-0.0), Row(-1.2345678E7), Row(-123.45678), Row(null)),
    s"""SELECT((CAST("SUBQUERY_0"."TESTFLOAT"ASFLOAT8)*-123.45678))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinaryMultiply23: TestCase = TestCase(
    s"""SELECT testint * -214 from test_table ORDER BY testint ASC NULLS LAST""",
    Seq(Row(-8988), Row(-8988), Row(-886219796), Row(null), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS
       |"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTINT"*-214))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTINT")AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)
       |AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryMultiply24: TestCase = TestCase(
    s"""SELECT testlong * -922 from test_table ORDER BY testlong ASC NULLS LAST""",
    Seq(
      Row(-1142369379161468918L),
      Row(-1142369379161468918L),
      Row(-1142369379161468918L),
      Row(-1142369379161468918L),
      Row(null)
    ),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT(("SUBQUERY_0"."TESTLONG"*-922))AS"SUBQUERY_1_COL_0",
       |("SUBQUERY_0"."TESTLONG")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )


  val testBinaryDivide0: TestCase = TestCase(
    s"""SELECT testbyte / 2 from test_table ORDER BY testbyte ASC NULLS LAST""",
    Seq(Row(0.0), Row(0.0), Row(0.5), Row(0.5), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((CAST("SUBQUERY_0"."TESTBYTE"ASFLOAT8)/2.0))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTBYTE")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"
       |ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryDivide1: TestCase = TestCase(
    s"""SELECT testint / -9 from test_table ORDER BY testint ASC NULLS LAST""",
    Seq(
      Row(-4.666666666666667),
      Row(-4.666666666666667),
      Row(-460134.8888888889),
      Row(null),
      Row(null),
    ),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((CAST("SUBQUERY_0"."TESTINT"ASFLOAT8)/-9.0))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTINT")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS
       |"SUBQUERY_2"""".stripMargin
  )

  val testBinaryDivide2: TestCase = TestCase(
    s"""SELECT testlong / -5000 from test_table ORDER BY testlong ASC NULLS LAST""",
    Seq(
      Row(-2.478024683647438E11),
      Row(-2.478024683647438E11),
      Row(-2.478024683647438E11),
      Row(-2.478024683647438E11),
      Row(null)
    ),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((CAST("SUBQUERY_0"."TESTLONG"ASFLOAT8)/-5000.0))
       |AS"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTLONG")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryDivide3: TestCase = TestCase(
    s"""SELECT testdouble / 2.0 FROM test_table""",
    Seq(Row(-617076.06156249), Row(0.0), Row(0.0), Row(617076.06156249), Row(null)),
    s"""SELECT(("SUBQUERY_0"."TESTDOUBLE"/2.0))AS"SUBQUERY_1_COL_0"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinaryDivide4: TestCase = TestCase(
    s"""SELECT testfloat / 2.0 FROM test_table""",
    Seq(Row(-0.5), Row(0.0), Row(0.5), Row(50000.0), Row(null)),
    s"""SELECT((CAST("SUBQUERY_0"."TESTFLOAT"ASFLOAT8)/2.0))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinaryDivide5: TestCase = TestCase(
    s"""SELECT testint / 2 from test_table ORDER BY testint ASC NULLS LAST""",
    Seq(Row(21.0), Row(21.0), Row(2070607.0), Row(null), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((CAST("SUBQUERY_0"."TESTINT"ASFLOAT8)/2.0))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTINT")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryDivide6: TestCase = TestCase(
    s"""SELECT testlong / 2 from test_table ORDER BY testlong ASC NULLS LAST""",
    Seq(
      Row(6.195061709118595E14),
      Row(6.195061709118595E14),
      Row(6.195061709118595E14),
      Row(6.195061709118595E14),
      Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((CAST("SUBQUERY_0"."TESTLONG"ASFLOAT8)/2.0))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTLONG")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryDivide7: TestCase = TestCase(
    s"""SELECT testbyte / 32767 from test_table ORDER BY testbyte ASC NULLS LAST""",
    Seq(Row(0), Row(0), Row(3.051850947599719E-5), Row(3.051850947599719E-5), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((CAST("SUBQUERY_0"."TESTBYTE"ASFLOAT8)/32767.0))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTBYTE")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryDivide8: TestCase = TestCase(
    s"""SELECT testdouble / 212.123 FROM test_table""",
    Seq(Row(-5818.096685059989), Row(0.0), Row(0.0), Row(5818.096685059989), Row(null)),
    s"""SELECT(("SUBQUERY_0"."TESTDOUBLE"/212.123))AS"SUBQUERY_1_COL_0"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinaryDivide9: TestCase = TestCase(
    s"""SELECT testfloat / 123.45678 FROM test_table""",
    Seq(Row(-0.008100000664200056), Row(0.008100000664200056), Row(0.0), Row(810.0000664200055),
      Row(null)),
    s"""SELECT((CAST("SUBQUERY_0"."TESTFLOAT"ASFLOAT8)/123.45678))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinaryDivide10: TestCase = TestCase(
    s"""SELECT testint / 2147483648 from test_table ORDER BY testint ASC NULLS LAST""",
    Seq(Row(1.955777406692505E-8), Row(1.955777406692505E-8), Row(0.0019284030422568321),
      Row(null), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((CAST("SUBQUERY_0"."TESTINT"ASFLOAT8)/2.147483648E9))
       |AS"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTINT")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryDivide11: TestCase = TestCase(
    s"""SELECT testlong / 9223372 from test_table ORDER BY testlong ASC NULLS LAST""",
    Seq(
      Row(1.34333987810935E8),
      Row(1.34333987810935E8),
      Row(1.34333987810935E8),
      Row(1.34333987810935E8),
      Row(null)
    ),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((CAST("SUBQUERY_0"."TESTLONG"ASFLOAT8)/9223372.0))
       |AS"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTLONG")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       |AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryDivide12: TestCase = TestCase(
    s"""SELECT testint / -9000 from test_table ORDER BY testint ASC NULLS LAST""",
    Seq(
      Row(-0.004666666666666667),
      Row(-0.004666666666666667),
      Row(-460.1348888888889),
      Row(null),
      Row(null),
    ),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((CAST("SUBQUERY_0"."TESTINT"ASFLOAT8)/-9000.0))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTINT")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryDivide13: TestCase = TestCase(
    s"""SELECT testbyte / -2 from test_table ORDER BY testbyte ASC NULLS LAST""",
    Seq(Row(-0.0), Row(-0.0), Row(-0.5), Row(-0.5), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((CAST("SUBQUERY_0"."TESTBYTE"ASFLOAT8)/-2.0))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTBYTE")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"
       |ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryDivide14: TestCase = TestCase(
    s"""SELECT testint / -987 from test_table ORDER BY testint ASC NULLS LAST""",
    Seq(
      Row(-0.0425531914893617),
      Row(-0.0425531914893617),
      Row(-4195.758865248227),
      Row(null),
      Row(null),
    ),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((CAST("SUBQUERY_0"."TESTINT"ASFLOAT8)/-987.0))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTINT")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"ORDERBY("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS
       |"SUBQUERY_2"""".stripMargin
  )

  val testBinaryDivide15: TestCase = TestCase(
    s"""SELECT testlong / -5346 from test_table ORDER BY testlong ASC NULLS LAST""",
    Seq(
      Row(-2.3176437370439935E11),
      Row(-2.3176437370439935E11),
      Row(-2.3176437370439935E11),
      Row(-2.3176437370439935E11),
      Row(null)
    ),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((CAST("SUBQUERY_0"."TESTLONG"ASFLOAT8)/-5346.0))
       |AS"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTLONG")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryDivide16: TestCase = TestCase(
    s"""SELECT testdouble / -2.0 FROM test_table""",
    Seq(Row(617076.06156249), Row(-0.0), Row(-0.0), Row(-617076.06156249), Row(null)),
    s"""SELECT(("SUBQUERY_0"."TESTDOUBLE"/-2.0))AS"SUBQUERY_1_COL_0"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinaryDivide17: TestCase = TestCase(
    s"""SELECT testfloat / -2.0 FROM test_table""",
    Seq(Row(-0.0), Row(-0.5), Row(-50000.0), Row(0.5), Row(null)),
    s"""SELECT((CAST("SUBQUERY_0"."TESTFLOAT"ASFLOAT8)/-2.0))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinaryDivide18: TestCase = TestCase(
    s"""SELECT testint / -2 from test_table ORDER BY testint ASC NULLS LAST""",
    Seq(Row(-21.0), Row(-21.0), Row(-2070607.0), Row(null), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((CAST("SUBQUERY_0"."TESTINT"ASFLOAT8)/-2.0))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTINT")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryDivide19: TestCase = TestCase(
    s"""SELECT testlong / -2 from test_table ORDER BY testlong ASC NULLS LAST""",
    Seq(
      Row(-6.195061709118595E14),
      Row(-6.195061709118595E14),
      Row(-6.195061709118595E14),
      Row(-6.195061709118595E14),
      Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((CAST("SUBQUERY_0"."TESTLONG"ASFLOAT8)/-2.0))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTLONG")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryDivide20: TestCase = TestCase(
    s"""SELECT testbyte / -32767 from test_table ORDER BY testbyte ASC NULLS LAST""",
    Seq(Row(-0), Row(-0), Row(-3.051850947599719E-5), Row(-3.051850947599719E-5), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((CAST("SUBQUERY_0"."TESTBYTE"ASFLOAT8)/-32767.0))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTBYTE")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryDivide21: TestCase = TestCase(
    s"""SELECT testdouble / -212.123 FROM test_table""",
    Seq(Row(5818.096685059989), Row(-0.0), Row(-0.0), Row(-5818.096685059989), Row(null)),
    s"""SELECT(("SUBQUERY_0"."TESTDOUBLE"/-212.123))AS"SUBQUERY_1_COL_0"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinaryDivide22: TestCase = TestCase(
    s"""SELECT testfloat / -123.45678 FROM test_table""",
    Seq(Row(0.008100000664200056), Row(-0.008100000664200056), Row(-0.0), Row(-810.0000664200055),
      Row(null)),
    s"""SELECT((CAST("SUBQUERY_0"."TESTFLOAT"ASFLOAT8)/-123.45678))AS
       |"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0"""".stripMargin
  )

  val testBinaryDivide23: TestCase = TestCase(
    s"""SELECT testint / -2147483648 from test_table ORDER BY testint ASC NULLS LAST""",
    Seq(Row(-1.955777406692505E-8), Row(-1.955777406692505E-8), Row(-0.0019284030422568321),
      Row(null), Row(null)),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((CAST("SUBQUERY_0"."TESTINT"ASFLOAT8)/-2.147483648E9))
       |AS"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTINT")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryDivide24: TestCase = TestCase(
    s"""SELECT testlong / -9223372 from test_table ORDER BY testlong ASC NULLS LAST""",
    Seq(
      Row(-1.34333987810935E8),
      Row(-1.34333987810935E8),
      Row(-1.34333987810935E8),
      Row(-1.34333987810935E8),
      Row(null)
    ),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((CAST("SUBQUERY_0"."TESTLONG"ASFLOAT8)/-9223372.0))
       |AS"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTLONG")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       |AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )

  val testBinaryDivide25: TestCase = TestCase(
    s"""SELECT testint / -96523 from test_table ORDER BY testint ASC NULLS LAST""",
    Seq(
      Row(-4.3512945101167595E-4),
      Row(-4.3512945101167595E-4),
      Row(-42.903908912901585),
      Row(null),
      Row(null),
    ),
    s"""SELECT("SUBQUERY_2"."SUBQUERY_1_COL_0")AS"SUBQUERY_3_COL_0"FROM
       |(SELECT*FROM(SELECT((CAST("SUBQUERY_0"."TESTINT"ASFLOAT8)/-96523.0))AS
       |"SUBQUERY_1_COL_0",("SUBQUERY_0"."TESTINT")AS"SUBQUERY_1_COL_1"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"ORDERBY
       |("SUBQUERY_1"."SUBQUERY_1_COL_1")ASC NULLS LAST)AS"SUBQUERY_2"""".stripMargin
  )


  val testLiteral0: TestCase = TestCase( // byte
    s"""SELECT -128, -127, -1, 0, 1, 126, 127 from test_table LIMIT 1""".stripMargin,
    Seq(Row(-128, -127, -1, 0, 1, 126, 127)),
    s"""SELECT*FROM(SELECT(-128)AS"SUBQUERY_1_COL_0",(-127)AS
       |"SUBQUERY_1_COL_1",(-1)AS"SUBQUERY_1_COL_2",(0)AS
       |"SUBQUERY_1_COL_3",(1)AS"SUBQUERY_1_COL_4",(126)AS
       |"SUBQUERY_1_COL_5",(127)AS"SUBQUERY_1_COL_6"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"LIMIT1""".stripMargin
  )

  val testLiteral1: TestCase = TestCase( // smallint
    s"""SELECT -32768s, -32767s, -1s, 0s, 1s, 32766s, 32767s from test_table LIMIT 1""".stripMargin,
    Seq(Row(-32768, -32767, -1, 0, 1, 32766, 32767)),
    s"""SELECT*FROM(SELECT(-32768)AS"SUBQUERY_1_COL_0",(-32767)AS
       |"SUBQUERY_1_COL_1",(-1)AS"SUBQUERY_1_COL_2",(0)AS
       |"SUBQUERY_1_COL_3",(1)AS"SUBQUERY_1_COL_4",(32766)AS
       |"SUBQUERY_1_COL_5",(32767)AS"SUBQUERY_1_COL_6"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")
       |AS"SUBQUERY_1"LIMIT1""".stripMargin
  )

  val testLiteral2: TestCase = TestCase( // int
    s"""SELECT -2147483647, -2147483646, -1, 0, 1, 2147483646, 2147483647 from test_table
       |LIMIT 1""".stripMargin,
    Seq(Row(-2147483647, -2147483646, -1, 0, 1, 2147483646, 2147483647)),
    s"""SELECT*FROM(SELECT(-2147483647)AS"SUBQUERY_1_COL_0",(-2147483646)AS
       |"SUBQUERY_1_COL_1",(-1)AS"SUBQUERY_1_COL_2",(0)AS"SUBQUERY_1_COL_3",
       |(1)AS"SUBQUERY_1_COL_4",(2147483646)AS"SUBQUERY_1_COL_5",
       |(2147483647)AS"SUBQUERY_1_COL_6"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"LIMIT1""".stripMargin
  )

  val testLiteral3: TestCase = TestCase( // long
    s"""SELECT -9223372036854775807l, -9223372036854775806l, -1l, 0l, 9223372036854775806l,
       |9223372036854775807l from test_table LIMIT 1""".stripMargin,
    Seq(Row(-9223372036854775807L, -9223372036854775806L, -1L, 0L, 9223372036854775806L,
      9223372036854775807L)),
    s"""SELECT*FROM(SELECT(-9223372036854775807)AS"SUBQUERY_1_COL_0",
       |(-9223372036854775806)AS"SUBQUERY_1_COL_1",
       |(-1)AS"SUBQUERY_1_COL_2",
       |(0)AS"SUBQUERY_1_COL_3",
       |(9223372036854775806)AS"SUBQUERY_1_COL_4",
       |(9223372036854775807)AS"SUBQUERY_1_COL_5"FROM(
       |SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"LIMIT1""".stripMargin
  )

  val testLiteral4: TestCase = TestCase( // boolean
    s"""SELECT false, true from test_table LIMIT 1""".stripMargin,
    Seq(Row(false, true)),
    s"""SELECT*FROM(SELECT(false)AS"SUBQUERY_1_COL_0",(true)AS
       |"SUBQUERY_1_COL_1"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"LIMIT1""".stripMargin
  )

  val testLiteral5: TestCase = TestCase( // string
    s"""SELECT 'hello world', '', 'redshift', 'spark', 'scala', '�', 'Ј', 'Ͱ' from test_table
       |LIMIT 1""".stripMargin,
    Seq(Row("hello world", "", "redshift", "spark", "scala", "�", "Ј", "Ͱ")),
    s"""SELECT*FROM(SELECT(\\'helloworld\\')AS"SUBQUERY_1_COL_0",(\\'\\')AS
       |"SUBQUERY_1_COL_1",(\\'redshift\\')AS"SUBQUERY_1_COL_2",(\\'spark\\')AS
       |"SUBQUERY_1_COL_3",(\\'scala\\')AS"SUBQUERY_1_COL_4",(\\'�\\')AS
       |"SUBQUERY_1_COL_5",(\\'Ј\\')AS"SUBQUERY_1_COL_6",(\\'Ͱ\\')AS
       |"SUBQUERY_1_COL_7"FROM(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")
       |AS"SUBQUERY_1"LIMIT1""".stripMargin
  )

  val testLiteral6: TestCase = TestCase( // floating point
    s"""SELECT 12.578 from test_table LIMIT 1""".stripMargin,
    Seq(Row(12.578)),
    s"""SELECT*FROM(SELECT(12.578)AS"SUBQUERY_1_COL_0"FROM(SELECT*FROM$test_table
    AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS"SUBQUERY_1"LIMIT1""".stripMargin

  )

  val testLiteral7: TestCase = TestCase( // date
    s"""SELECT DATE '1997' from test_table LIMIT 1""".stripMargin,
    Seq(Row(TestUtils.toDate(year = 1997, zeroBasedMonth = 0, date = 1))),
    s"""SELECT*FROM(SELECT(DATEADD(day,9862,TO_DATE
       |(\\'1970-01-01\\',\\'YYYY-MM-DD\\')))AS"SUBQUERY_1_COL_0"FROM
       |(SELECT*FROM$test_table
       AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
       |"SUBQUERY_1"LIMIT1""".stripMargin
  )

    val testLiteral8: TestCase = TestCase( // timestamp
      s"""SELECT TIMESTAMP '1997-01-31 09:26:56.123', TIMESTAMP '2000-02-01 10:11:56.666',
         |TIMESTAMP '1997-01' from test_table LIMIT 1""".stripMargin,
      Seq(Row(
        TestUtils.toTimestamp(year = 1997, zeroBasedMonth = 0, date = 31, hour = 9, minutes = 26,
          seconds = 56, millis = 123),
        TestUtils.toTimestamp(year = 2000, zeroBasedMonth = 1, date = 1, hour = 10, minutes = 11,
          seconds = 56, millis = 666),
        TestUtils.toTimestamp(year = 1997, zeroBasedMonth = 0, date = 1, hour = 0, minutes = 0,
          seconds = 0, millis = 0),
      )),
      s"""SELECT*FROM(SELECT(\\'1997-01-3109:26:56.123\\'::TIMESTAMP)AS
         |"SUBQUERY_1_COL_0",(\\'2000-02-0110:11:56.666\\'::TIMESTAMP)AS
         |"SUBQUERY_1_COL_1",(\\'1997-01-0100:00:00\\'::TIMESTAMP)AS
         |"SUBQUERY_1_COL_2"FROM(SELECT*FROM$test_table
         AS"RS_CONNECTOR_QUERY_ALIAS")AS"SUBQUERY_0")AS
         |"SUBQUERY_1"LIMIT1""".stripMargin
    )

  test("Test AND statements", P1Test) {
    val cases = Seq(
    testAnd0,
    testAnd1,
    testAnd2,
    testAnd3,
    testAnd4,
    testAnd5,
    testAnd6,
    testAnd7,
    testAnd8)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  test("Test OR statements", P1Test) {
    val cases = Seq(
    testOr0,
    testOr1,
    testOr2,
    testOr3,
    testOr4)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  test("Test Binary + operator", P1Test) {
    val cases = Seq(
    testBinaryPlus0,
    testBinaryPlus1,
    testBinaryPlus2,
    testBinaryPlus3,
    testBinaryPlus4,
    testBinaryPlus5,
    testBinaryPlus8,
    testBinaryPlus9,
    testBinaryPlus10,
    testBinaryPlus11,
    testBinaryPlus12,
    testBinaryPlus13,
    testBinaryPlus14,
    testBinaryPlus15,
    testBinaryPlus16,
    testBinaryPlus17,
    testBinaryPlus18,
    testBinaryPlus19,
    testBinaryPlus20,
    testBinaryPlus21)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  test("Test Binary - operator") {
    val cases = Seq(
    testBinarySubtract0,
    testBinarySubtract1,
    testBinarySubtract3,
    testBinarySubtract4,
    testBinarySubtract5,
    testBinarySubtract6,
    testBinarySubtract7,
    testBinarySubtract8,
    testBinarySubtract9,
    testBinarySubtract10,
    testBinarySubtract11,
    testBinarySubtract12,
    testBinarySubtract13,
    testBinarySubtract14,
    testBinarySubtract15,
    testBinarySubtract16,
    testBinarySubtract17,
    testBinarySubtract18,
    testBinarySubtract19,
    testBinarySubtract20,
    testBinarySubtract21)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  test("Test Binary * operator") {
    val cases = Seq(
    testBinaryMultiply0,
    testBinaryMultiply1,
    testBinaryMultiply2,
    testBinaryMultiply3,
    testBinaryMultiply4,
    testBinaryMultiply5,
    testBinaryMultiply6,
    testBinaryMultiply7,
    testBinaryMultiply8,
    testBinaryMultiply9,
    testBinaryMultiply10,
    testBinaryMultiply13,
    testBinaryMultiply14,
    testBinaryMultiply15,
    testBinaryMultiply16,
    testBinaryMultiply17,
    testBinaryMultiply18,
    testBinaryMultiply19,
    testBinaryMultiply20,
    testBinaryMultiply21,
    testBinaryMultiply22,
    testBinaryMultiply23,
    testBinaryMultiply24)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  test("Test Binary / operator") {
    val cases = Seq(
    testBinaryDivide0,
    testBinaryDivide1,
    testBinaryDivide2,
    testBinaryDivide3,
    testBinaryDivide4,
    testBinaryDivide5,
    testBinaryDivide6,
    testBinaryDivide7,
    testBinaryDivide8,
    testBinaryDivide9,
    testBinaryDivide10,
    testBinaryDivide11,
    testBinaryDivide12,
    testBinaryDivide13,
    testBinaryDivide14,
    testBinaryDivide15,
    testBinaryDivide16,
    testBinaryDivide17,
    testBinaryDivide18,
    testBinaryDivide19,
    testBinaryDivide20,
    testBinaryDivide21,
    testBinaryDivide22,
    testBinaryDivide23,
    testBinaryDivide24,
    testBinaryDivide25)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  test("Test Literal", P1Test) {
    val cases = Seq(
    testLiteral0,
    testLiteral1,
    testLiteral2,
    testLiteral3,
    testLiteral4,
    testLiteral5,
    testLiteral6,
    testLiteral7,
    testLiteral8)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  ignore("Unsupported/Failing tests") {
    doTest(sqlContext, testBinaryPlus7) // Redshift-6996
    doTest(sqlContext, testBinarySubtract2) // Redshift-6996
    doTest(sqlContext, testBinaryPlus6) // Redshift-6994
    doTest(sqlContext, testBinaryMultiply11) // Redshift-7029
    doTest(sqlContext, testBinaryMultiply12) // Redshift-7030
    doTest(sqlContext, testBinarySubtract22) // Redshift-7040
    doTest(sqlContext, testBinarySubtract23) // Redshift-7041
  }
}

class TextPushdownFilterSuite extends PushdownFilterSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "true"
}

class ParquetPushdownFilterSuite extends PushdownFilterSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "true"
}

class TextNoPushdownFilterSuite extends PushdownFilterSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "false"
}

class ParquetNoPushdownFilterSuite extends PushdownFilterSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}

class TextNoCachePushdownFilterSuite
  extends TextPushdownFilterSuite {
  override protected val s3_result_cache = "false"
}

class ParquetNoCachePushdownFilterSuite
  extends ParquetPushdownFilterSuite {
  override protected val s3_result_cache = "false"
}
