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

import java.sql.{Date, Timestamp}
import java.time.ZonedDateTime

abstract class PushdownDateTimeSuite extends IntegrationPushdownSuiteBase {
  protected val test_tz_table: String = s""""PUBLIC"."pushdown_suite_tz_test_table_$randomSuffix""""

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (!preloaded_data.toBoolean) {
      conn.prepareStatement(s"drop table if exists $test_tz_table").executeUpdate()
      createDateTimeTZDataInRedshift(test_tz_table)
    }
  }

  override def afterAll(): Unit = {
    try {
      if (!preloaded_data.toBoolean) {
        conn.prepareStatement(s"drop table if exists $test_tz_table").executeUpdate()
      }
    } finally {
      super.afterAll()
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    read
      .option("dbtable", test_tz_table)
      .load()
      .createOrReplaceTempView("test_tz_table")
  }

  protected def createDateTimeTZDataInRedshift(tableName: String): Unit = {
    conn.createStatement().executeUpdate(
      s"""CREATE TABLE $tableName (
         | testid int,
         | testtimestamp timestamp,
         | testtimestamptz timestamptz
         | )""".stripMargin
    )
    // scalastyle:off
    conn.createStatement().executeUpdate(
      s"""INSERT INTO $tableName VALUES
         | (null, null, null),
         | (0, 'Jun 1,2018 09:59:59', 'Jun 1,2018 09:59:59 EDT'),
         | (1, '2018-12-17 07:37:16', '2018-12-17 08:37:16-08'),
         | (2, '12/18/2018 08:37:16.00', '12/17/2018 07:37:16.00 UTC'),
         | (3, '12.19.2018 09:37:16.00', '12.17.2018 17:37:16.00 US/Pacific')
         """.stripMargin
    )
    // scalastyle:on
  }

  val testDateAdd1: TestCase = TestCase(
    """SELECT DATE_ADD(testdate, 1) FROM test_table
      | WHERE testdate >= cast('2015-07-02' as date)""".stripMargin,
    Seq(Row(Date.valueOf("2015-07-03")), Row(Date.valueOf("2015-07-04"))),
    s"""SELECT ( DATEADD ( day, 1, "SUBQUERY_1"."TESTDATE" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTDATE" IS NOT NULL )
       | AND ( "SUBQUERY_0"."TESTDATE" >= DATEADD ( day, 16618,
       | TO_DATE ( \\'1970-01-01\\', \\'YYYY-MM-DD\\' ) ) ) ) ) AS "SUBQUERY_1"""".stripMargin
  )

  val testDateAdd2: TestCase = TestCase(
    """SELECT DATE_ADD(testdate, 1) FROM test_table
      | WHERE testdate >= to_date('2015-07-02')""".stripMargin,
    Seq(Row(Date.valueOf("2015-07-03")), Row(Date.valueOf("2015-07-04"))),
    s"""SELECT ( DATEADD ( day, 1, "SUBQUERY_1"."TESTDATE" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTDATE" IS NOT NULL )
       | AND ( "SUBQUERY_0"."TESTDATE" >= DATEADD ( day, 16618,
       | TO_DATE ( \\'1970-01-01\\', \\'YYYY-MM-DD\\' ) ) ) ) ) AS "SUBQUERY_1"""".stripMargin
  )

  val testDateAdd3: TestCase = TestCase(
    """SELECT DATE_ADD(testtimestamp, 1) FROM test_table
      | WHERE testtimestamp >= cast('2015-07-02 00:00:00' as timestamp)""".stripMargin,
    Seq(Row(Date.valueOf("2015-07-03")), Row(Date.valueOf("2015-07-04"))),
    s"""SELECT ( DATEADD ( day, 1 , CAST ( "SUBQUERY_1"."TESTTIMESTAMP" AS DATE ) ) )
       | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table
       | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | WHERE ( ( "SUBQUERY_0"."TESTTIMESTAMP" IS NOT NULL )
       | AND ( "SUBQUERY_0"."TESTTIMESTAMP" >= \\'2015-07-02 00:00:00\\' ::TIMESTAMP ) ) )
       | AS "SUBQUERY_1"""".stripMargin
  )

  val testDateAdd4: TestCase = TestCase(
    """SELECT DATE_ADD(testtimestamp, 1) FROM test_table
      | WHERE testtimestamp >= to_timestamp('2015-07-02 00:00:00')""".stripMargin,
    Seq(Row(Date.valueOf("2015-07-03")), Row(Date.valueOf("2015-07-04"))),
    s"""SELECT ( DATEADD ( day, 1 , CAST ( "SUBQUERY_1"."TESTTIMESTAMP" AS DATE ) ) )
       | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table
       | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | WHERE ( ( "SUBQUERY_0"."TESTTIMESTAMP" IS NOT NULL )
       | AND ( "SUBQUERY_0"."TESTTIMESTAMP" >= \\'2015-07-02 00:00:00\\' ::TIMESTAMP ) ) )
       | AS "SUBQUERY_1"""".stripMargin
  )

  test("Test DateAdd datetime expressions", P0Test, P1Test) {
    doTest(sqlContext, testDateAdd1)
    doTest(sqlContext, testDateAdd2)
    doTest(sqlContext, testDateAdd3)
    doTest(sqlContext, testDateAdd4)
  }

  val testDateSub1: TestCase = TestCase(
    """SELECT DATE_SUB(testdate, 1) FROM test_table
      | WHERE testdate <= cast('2015-07-02' as date)""".stripMargin,
    Seq(Row(Date.valueOf("2015-06-30")),
      Row(Date.valueOf("2015-07-01"))),
    s"""SELECT ( DATEADD ( day, (0- (1)), "SUBQUERY_1"."TESTDATE" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTDATE" IS NOT NULL )
       | AND ( "SUBQUERY_0"."TESTDATE" <= DATEADD ( day, 16618,
       | TO_DATE ( \\'1970-01-01\\', \\'YYYY-MM-DD\\' ) ) ) ) ) AS "SUBQUERY_1"""".stripMargin
  )

  val testDateSub2: TestCase = TestCase(
    """SELECT DATE_SUB(testdate, 1) FROM test_table
      | WHERE testdate >= to_date('2015-07-01')""".stripMargin,
    Seq(Row(Date.valueOf("2015-06-30")),
      Row(Date.valueOf("2015-07-01")),
      Row(Date.valueOf("2015-07-02"))),
    s"""SELECT ( DATEADD ( day, (0 - (1)), "SUBQUERY_1"."TESTDATE" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTDATE" IS NOT NULL )
       | AND ( "SUBQUERY_0"."TESTDATE" >= DATEADD ( day, 16617,
       | TO_DATE ( \\'1970-01-01\\', \\'YYYY-MM-DD\\' ) ) ) ) ) AS "SUBQUERY_1"""".stripMargin
  )

  val testDateSub3: TestCase = TestCase(
    """SELECT DATE_SUB(testtimestamp, 1) FROM test_table
      | WHERE testdate >= to_date('2015-07-01')""".stripMargin,
    Seq(Row(Date.valueOf("2015-06-30")),
      Row(Date.valueOf("2015-07-01")),
      Row(Date.valueOf("2015-07-02"))),
    s"""SELECT ( DATEADD ( day, (0 - (1)), CAST ( "SUBQUERY_1"."TESTTIMESTAMP" AS DATE ) ) )
       | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table
       | AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTDATE" IS NOT NULL )
       | AND ( "SUBQUERY_0"."TESTDATE" >=
       | DATEADD(day, 16617, TO_DATE( \\'1970-01-01\\', \\'YYYY-MM-DD\\' ) ) ) ) ) AS "SUBQUERY_1"
       | """.stripMargin
  )

  val testDateSub4: TestCase = TestCase(
    """SELECT DATE_SUB(testtimestamp, 1) FROM test_table
      | WHERE testtimestamp < to_timestamp('2015-07-02 00:00:00')""".stripMargin,
    Seq(Row(Date.valueOf("2015-06-30"))),
    s"""SELECT ( DATEADD ( day, (0 - (1)), CAST ( "SUBQUERY_1"."TESTTIMESTAMP" AS DATE ) ) )
       | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table
       | AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTTIMESTAMP" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTTIMESTAMP" < \\'2015-07-0200:00:00\\' ::TIMESTAMP) ) )
       | AS "SUBQUERY_1"""".stripMargin
  )

  val testDateSub5: TestCase = TestCase(
    """SELECT DATE_SUB(testtimestamp, 1) FROM test_table
      | WHERE testtimestamp >= cast('2015-07-02 00:00:00' as timestamp)""".stripMargin,
    Seq(Row(Date.valueOf("2015-07-01")), Row(Date.valueOf("2015-07-02"))),
    s"""SELECT ( DATEADD ( day, (0 - (1)), CAST ( "SUBQUERY_1"."TESTTIMESTAMP" AS DATE ) ) )
       | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table
       | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | WHERE ( ( "SUBQUERY_0"."TESTTIMESTAMP" IS NOT NULL )
       | AND ( "SUBQUERY_0"."TESTTIMESTAMP" >= \\'2015-07-02 00:00:00\\' ::TIMESTAMP ) ) )
       | AS "SUBQUERY_1"""".stripMargin
  )

  test("Test DateSub datetime expressions", P0Test, P1Test) {
    doTest(sqlContext, testDateSub1)
    doTest(sqlContext, testDateSub2)
    doTest(sqlContext, testDateSub3)
    doTest(sqlContext, testDateSub4)
    doTest(sqlContext, testDateSub5)
  }

  val testDatetimeCast1: TestCase = TestCase(
    """SELECT TIMESTAMP(testdate) FROM test_table
      | WHERE testtimestamp >= cast('2015-07-01 00:00:00' as timestamp)""".stripMargin,
    Seq(Row(Timestamp.valueOf("2015-07-01 00:00:00")),
      Row(Timestamp.valueOf("2015-07-02 00:00:00")),
      Row(Timestamp.valueOf("2015-07-03 00:00:00"))),
    s"""SELECT ( CAST ( "SUBQUERY_1"."TESTDATE" AS TIMESTAMP ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTTIMESTAMP" IS NOT NULL )
       | AND ( "SUBQUERY_0"."TESTTIMESTAMP" >= \\'2015-07-01 00:00:00\\' ::TIMESTAMP ) ) )
       | AS "SUBQUERY_1"""".stripMargin
  )

  val testDatetimeCast2: TestCase = TestCase(
    """SELECT DATE(testtimestamp) FROM test_table
      | WHERE testdate >= cast('2015-07-01' as date)""".stripMargin,
    Seq(Row(Date.valueOf("2015-07-01")),
      Row(Date.valueOf("2015-07-02")),
      Row(Date.valueOf("2015-07-03"))),
    s"""SELECT ( CAST ( "SUBQUERY_1"."TESTTIMESTAMP" AS DATE)) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS")
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTDATE" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTDATE" >=
       | DATEADD (day, 16617, TO_DATE ( \\'1970-01-01\\', \\'YYYY-MM-DD\\')) ) ) )
       | AS "SUBQUERY_1"""".stripMargin
  )

  test("Test casting datetime expressions", P0Test, P1Test) {
    doTest(sqlContext, testDatetimeCast1)
    doTest(sqlContext, testDatetimeCast2)
  }

  val testDateTrunc1: TestCase = TestCase(
    s"""select date_trunc('YEAR', testdate),
      | date_trunc('QUARTER', testdate),
      | date_trunc('MONTH', testdate),
      | date_trunc('WEEK', testdate),
      | date_trunc('DAY', testdate),
      | date_trunc('YEAR', testtimestamp),
      | date_trunc('QUARTER', testtimestamp),
      | date_trunc('MONTH', testtimestamp),
      | date_trunc('WEEK', testtimestamp),
      | date_trunc('DAY', testtimestamp),
      | date_trunc('HOUR', testtimestamp),
      | date_trunc('MINUTE', testtimestamp),
      | date_trunc('SECOND', testtimestamp)
      | from test_table where testtimestamp > cast('2015-07-02 00:00:00' as timestamp)
      | """.stripMargin,
    Seq(Row(Timestamp.valueOf("2015-01-01 00:00:00"),
      Timestamp.valueOf("2015-07-01 00:00:00"),
      Timestamp.valueOf("2015-07-01 00:00:00"),
      Timestamp.valueOf("2015-06-29 00:00:00"),
      Timestamp.valueOf("2015-07-03 00:00:00"),
      Timestamp.valueOf("2015-01-01 00:00:00"),
      Timestamp.valueOf("2015-07-01 00:00:00"),
      Timestamp.valueOf("2015-07-01 00:00:00"),
      Timestamp.valueOf("2015-06-29 00:00:00"),
      Timestamp.valueOf("2015-07-03 00:00:00"),
      Timestamp.valueOf("2015-07-03 12:00:00"),
      Timestamp.valueOf("2015-07-03 12:34:00"),
      Timestamp.valueOf("2015-07-03 12:34:56"))),
      s"""SELECT
         |( DATE_TRUNC ( \\'YEAR\\' , CAST ( "SUBQUERY_1"."TESTDATE" AS TIMESTAMP ) ) ) AS "SUBQUERY_2_COL_0" ,
         |( DATE_TRUNC ( \\'QUARTER\\' , CAST ( "SUBQUERY_1"."TESTDATE" AS TIMESTAMP ) ) ) AS "SUBQUERY_2_COL_1" ,
         |( DATE_TRUNC ( \\'MONTH\\' , CAST ( "SUBQUERY_1"."TESTDATE" AS TIMESTAMP ) ) ) AS "SUBQUERY_2_COL_2" ,
         |( DATE_TRUNC ( \\'WEEK\\' , CAST ( "SUBQUERY_1"."TESTDATE" AS TIMESTAMP ) ) ) AS "SUBQUERY_2_COL_3" ,
         |( DATE_TRUNC ( \\'DAY\\' , CAST ( "SUBQUERY_1"."TESTDATE" AS TIMESTAMP ) ) ) AS "SUBQUERY_2_COL_4" ,
         |( DATE_TRUNC ( \\'YEAR\\' , "SUBQUERY_1"."TESTTIMESTAMP" ) ) AS "SUBQUERY_2_COL_5" ,
         |( DATE_TRUNC ( \\'QUARTER\\' , "SUBQUERY_1"."TESTTIMESTAMP" ) ) AS "SUBQUERY_2_COL_6" ,
         |( DATE_TRUNC ( \\'MONTH\\' , "SUBQUERY_1"."TESTTIMESTAMP" ) ) AS "SUBQUERY_2_COL_7" ,
         |( DATE_TRUNC ( \\'WEEK\\' , "SUBQUERY_1"."TESTTIMESTAMP" ) ) AS "SUBQUERY_2_COL_8" ,
         |( DATE_TRUNC ( \\'DAY\\' , "SUBQUERY_1"."TESTTIMESTAMP" ) ) AS "SUBQUERY_2_COL_9" ,
         |( DATE_TRUNC ( \\'HOUR\\' , "SUBQUERY_1"."TESTTIMESTAMP" ) ) AS "SUBQUERY_2_COL_10" ,
         |( DATE_TRUNC ( \\'MINUTE\\' , "SUBQUERY_1"."TESTTIMESTAMP" ) ) AS "SUBQUERY_2_COL_11" ,
         |( DATE_TRUNC ( \\'SECOND\\' , "SUBQUERY_1"."TESTTIMESTAMP" ) ) AS "SUBQUERY_2_COL_12"
         | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTTIMESTAMP" IS NOT NULL )
         | AND ( "SUBQUERY_0"."TESTTIMESTAMP" > \\'2015-07-02 00:00:00\\' ::TIMESTAMP ) ) )
         | AS "SUBQUERY_1"""".stripMargin
  )

  // Function trunc cannot be pushed down.
  val testDateTrunc2: TestCase = TestCase(
    s"""select trunc(testdate, 'YEAR'),
       | trunc(testdate, 'QUARTER'),
       | trunc(testdate, 'MONTH'),
       | trunc(testdate, 'WEEK'),
       | trunc(testdate, 'DAY'),
       | trunc(testtimestamp, 'YEAR'),
       | trunc(testtimestamp, 'QUARTER'),
       | trunc(testtimestamp, 'MONTH'),
       | trunc(testtimestamp, 'WEEK'),
       | trunc(testtimestamp, 'DAY'),
       | trunc(testtimestamp, 'HOUR'),
       | trunc(testtimestamp, 'MINUTE'),
       | trunc(testtimestamp, 'SECOND')
       | from test_table where testtimestamp > cast('2015-07-02 00:00:00' as timestamp)
       | """.stripMargin,
    Seq(Row(Date.valueOf("2015-01-01"),
      Date.valueOf("2015-07-01"),
      Date.valueOf("2015-07-01"),
      Date.valueOf("2015-06-29"),
      null,
      Date.valueOf("2015-01-01"),
      Date.valueOf("2015-07-01"),
      Date.valueOf("2015-07-01"),
      Date.valueOf("2015-06-29"),
      null, null, null, null)),
    s"""SELECT "testdate","testtimestamp" FROM $test_table
       | WHERE "testtimestamp" IS NOT NULL
       | AND "testtimestamp" > ''2015-07-0200:00:00.0''""".stripMargin
  )

  test("Test DATE_TRUNC datetime expressions", P0Test, P1Test) {
    doTest(sqlContext, testDateTrunc1)
    doTest(sqlContext, testDateTrunc2)
  }

  val testAddMonths1: TestCase = TestCase(
    """SELECT add_months(testdate, 1) FROM test_table
      | WHERE testdate > cast('2015-07-02' as date)""".stripMargin,
    Seq(Row(Date.valueOf("2015-08-03"))),
    s"""SELECT ( ADD_MONTHS ( "SUBQUERY_1"."TESTDATE", 1 ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTDATE" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTDATE" >
       | DATEADD ( day, 16618, TO_DATE(\\'1970-01-01\\', \\'YYYY-MM-DD\\' ) ) ) ) )
       | AS "SUBQUERY_1"""".stripMargin
  )

  val testAddMonths2: TestCase = TestCase(
    """SELECT add_months(testtimestamp, 1) FROM test_table
      | WHERE testtimestamp > to_timestamp('2015-07-02 00:00:00')""".stripMargin,
    Seq(Row(Date.valueOf("2015-08-03"))),
    s"""SELECT ( ADD_MONTHS ( CAST ( "SUBQUERY_1"."TESTTIMESTAMP" AS DATE ), 1))
       | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | WHERE ( ( "SUBQUERY_0"."TESTTIMESTAMP" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTTIMESTAMP" > \\'2015-07-0200:00:00\\' ::TIMESTAMP ) ) )
       | AS "SUBQUERY_1"""".stripMargin
  )

  test("Test ADD_MONTHS datetime expressions", P0Test, P1Test) {
    doTest(sqlContext, testAddMonths1)
    doTest(sqlContext, testAddMonths2)
  }

  test("Test max timestamptz type", TimestamptzTest, P0Test) {
    checkAnswer(
      sqlContext.sql("""SELECT MAX(testtimestamptz) FROM test_tz_table"""),
      Seq(Row(Timestamp.from(
        ZonedDateTime.parse("2018-12-17 17:37:16 US/Pacific", formatter).toInstant))))

    checkSqlStatement(
      s"""SELECT ( MAX ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
         | FROM ( SELECT ( "SUBQUERY_0"."TESTTIMESTAMPTZ" ) AS "SUBQUERY_1_COL_0"
         | FROM ( SELECT * FROM $test_tz_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
         | AS "SUBQUERY_1" LIMIT 1""".stripMargin)
  }

  test("Test timestamptz type", TimestamptzTest, P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        s"""SELECT testid, testtimestamp,  testtimestamptz FROM test_tz_table
           | WHERE testtimestamp >= cast('2015-07-01 00:00:00' as timestamp) order by testid
           | """.stripMargin),
      Seq(
        Row(0, Timestamp.valueOf("2018-06-01 09:59:59"), Timestamp.from(
          ZonedDateTime.parse("2018-06-01 09:59:59 EDT", formatter).toInstant)),
        Row(1, Timestamp.valueOf("2018-12-17 07:37:16"), Timestamp.from(
          ZonedDateTime.parse("2018-12-17 16:37:16 UTC", formatter).toInstant)),
        Row(2, Timestamp.valueOf("2018-12-18 08:37:16"), Timestamp.from(
          ZonedDateTime.parse("2018-12-17 07:37:16 UTC", formatter).toInstant)),
        Row(3, Timestamp.valueOf("2018-12-19 09:37:16"), Timestamp.from(
          ZonedDateTime.parse("2018-12-17 17:37:16 US/Pacific", formatter).toInstant)))
    )

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT * FROM ( SELECT * FROM $test_tz_table
         | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         | WHERE ( ( "SUBQUERY_0"."TESTTIMESTAMP" IS NOT NULL )
         | AND ( "SUBQUERY_0"."TESTTIMESTAMP" >= \\'2015-07-01 00:00:00\\' ::TIMESTAMP ) ) )
         | AS "SUBQUERY_1" ORDER BY ( "SUBQUERY_1"."TESTID" ) ASC NULLS FIRST""".stripMargin)
  }

  test("Test DATE_ADD on timestamptz type", TimestamptzTest, P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT COUNT(DATE_ADD(testtimestamptz, 1)) FROM test_tz_table
          | WHERE testtimestamp < to_timestamp('2018-12-17 08:00:00')""".stripMargin),
      Seq(Row(2))
    )

    checkSqlStatement(
      s"""SELECT ( COUNT ( DATEADD ( day, 1, CAST ( "SUBQUERY_2"."SUBQUERY_2_COL_0" AS DATE ) ) ) )
         | AS "SUBQUERY_3_COL_0" FROM ( SELECT ( "SUBQUERY_1"."TESTTIMESTAMPTZ" )
         | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_tz_table
         | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         | WHERE ( ( "SUBQUERY_0"."TESTTIMESTAMP" IS NOT NULL ) AND
         | ( "SUBQUERY_0"."TESTTIMESTAMP" < \\'2018-12-17 08:00:00\\' ::TIMESTAMP ) ) )
         | AS "SUBQUERY_1" ) AS "SUBQUERY_2" LIMIT 1""".stripMargin)
  }

  test("Test DATE_SUB on timestamptz type", TimestamptzTest, P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT COUNT(DATE_SUB(testtimestamptz, 1)) FROM test_tz_table
          | WHERE testtimestamp < to_timestamp('2018-12-17 08:00:00')""".stripMargin),
      Seq(Row(2))
    )

    checkSqlStatement(
      s"""SELECT ( COUNT ( DATEADD ( day, ( 0 - (1) ),
         | CAST ( "SUBQUERY_2"."SUBQUERY_2_COL_0" AS DATE ) ) ) )
         | AS "SUBQUERY_3_COL_0" FROM ( SELECT ( "SUBQUERY_1"."TESTTIMESTAMPTZ" )
         | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_tz_table
         | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         | WHERE ( ( "SUBQUERY_0"."TESTTIMESTAMP" IS NOT NULL ) AND
         | ( "SUBQUERY_0"."TESTTIMESTAMP" < \\'2018-12-17 08:00:00\\' ::TIMESTAMP ) ) )
         | AS "SUBQUERY_1" ) AS "SUBQUERY_2" LIMIT 1""".stripMargin)
  }

  test("Test DATE_TRUNC on timestamptz type", TimestamptzTest, P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        s"""SELECT date_trunc('YEAR', testtimestamptz),
           | date_trunc('QUARTER', testtimestamptz),
           | date_trunc('MONTH', testtimestamptz),
           | date_trunc('HOUR', testtimestamptz)
           | FROM test_tz_table
           | WHERE testtimestamp >= cast('2018-12-19 08:37:16.00' as timestamp)
           | """.stripMargin),
      Seq(Row(Timestamp.valueOf("2018-01-01 00:00:00.0"),
        Timestamp.valueOf("2018-10-01 00:00:00.0"),
        Timestamp.valueOf("2018-12-01 00:00:00.0"),
        Timestamp.from(
          ZonedDateTime.parse("2018-12-18 01:00:00 UTC", formatter).toInstant)))
    )

    checkSqlStatement(
      s"""SELECT
      |( DATE_TRUNC ( \\'YEAR\\' , "SUBQUERY_1"."TESTTIMESTAMPTZ" ) ) AS "SUBQUERY_2_COL_0",
      |( DATE_TRUNC ( \\'QUARTER\\' , "SUBQUERY_1"."TESTTIMESTAMPTZ" ) ) AS "SUBQUERY_2_COL_1",
      |( DATE_TRUNC ( \\'MONTH\\' , "SUBQUERY_1"."TESTTIMESTAMPTZ" ) ) AS "SUBQUERY_2_COL_2",
      |( DATE_TRUNC ( \\'HOUR\\' , "SUBQUERY_1"."TESTTIMESTAMPTZ" ) ) AS "SUBQUERY_2_COL_3"
      | FROM ( SELECT * FROM ( SELECT * FROM $test_tz_table AS "RS_CONNECTOR_QUERY_ALIAS" )
      | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTTIMESTAMP" IS NOT NULL )
      | AND ( "SUBQUERY_0"."TESTTIMESTAMP" >= \\'2018-12-19 08:37:16\\' ::TIMESTAMP ) ) )
      | AS "SUBQUERY_1"""".stripMargin)
  }
}

class DefaultDateTimePushdownSuite extends PushdownDateTimeSuite {
  override protected val s3format: String = "DEFAULT"
  override protected val auto_pushdown: String = "true"
}

class ParquetDateTimePushdownSuite extends PushdownDateTimeSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "true"
}

class DefaultNoPushdownDateTimeSuite extends PushdownDateTimeSuite {
  override protected val s3format: String = "DEFAULT"
  override protected val auto_pushdown: String = "false"
}

class ParquetNoPushdownDateTimeSuite extends PushdownDateTimeSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}
