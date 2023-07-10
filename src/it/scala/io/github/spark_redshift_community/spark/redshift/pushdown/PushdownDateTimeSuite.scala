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

import java.sql.Timestamp

abstract class PushdownDateTimeSuite extends IntegrationPushdownSuiteBase {
  test("DateAdd pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT DATE_ADD(testtimestamp, 1) FROM test_table
          |WHERE testtimestamp >= cast('2015-07-02 00:00:00' as timestamp)""".stripMargin
      ),
      Seq(Row(TestUtils.toDate(2015, 6, 3))
        , Row(TestUtils.toDate(2015, 6, 4)))
    )

    checkSqlStatement(
      s"""SELECT ( DATEADD ( day, 1 , CAST ( "SUBQUERY_1"."TESTTIMESTAMP" AS DATE ) ) )
         |AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTTIMESTAMP" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTTIMESTAMP" >= \\'2015-07-02 00:00:00\\' ::TIMESTAMP ) ) )
         |AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("DateSub pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT DATE_SUB(testtimestamp, 1) FROM test_table
          |WHERE testdate >= to_date('2015-07-01')""".stripMargin
      ),
      Seq(Row(TestUtils.toDate(2015, 5, 30))
        , Row(TestUtils.toDate(2015, 6, 1))
        , Row(TestUtils.toDate(2015, 6, 2))
      )
    )

    checkSqlStatement(
      s"""SELECT ( DATEADD ( day, (0 - ( 1 ) ), CAST ( "SUBQUERY_1"."TESTTIMESTAMP" AS DATE ) ) )
         |AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTDATE" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTDATE" >=
         |DATEADD(day, 16617 , TO_DATE(\\'1970-01-01\\', \\'YYYY-MM-DD\\')) ) ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("TIMESTAMP pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT TIMESTAMP(testdate) FROM test_table
          |WHERE testtimestamp >= cast('2015-07-01 00:00:00' as timestamp)""".stripMargin
      ),
      Seq(Row(TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0))
        , Row(TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0))
        , Row(TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0))
      )
    )

    checkSqlStatement(
      s"""SELECT ( CAST ( "SUBQUERY_1"."TESTDATE" AS TIMESTAMP ) ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTTIMESTAMP" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTTIMESTAMP" >= \\'2015-07-01 00:00:00\\' ::TIMESTAMP ) ) )
         |AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("DATE_TRUNC pushdown") {
    checkAnswer(
      sqlContext.sql(
        s"""select
           |date_trunc('YEAR', testdate),
           |date_trunc('QUARTER', testdate),
           |date_trunc('MONTH', testdate),
           |date_trunc('WEEK', testdate),
           |date_trunc('DAY', testdate),
           |date_trunc('YEAR', testtimestamp),
           |date_trunc('QUARTER', testtimestamp),
           |date_trunc('MONTH', testtimestamp),
           |date_trunc('WEEK', testtimestamp),
           |date_trunc('DAY', testtimestamp),
           |date_trunc('HOUR', testtimestamp),
           |date_trunc('MINUTE', testtimestamp),
           |date_trunc('SECOND', testtimestamp)
           | from test_table where testtimestamp > cast('2015-07-02 00:00:00' as timestamp)
           | """.stripMargin),
      Seq(Row(
        TestUtils.toTimestamp(2015, 0, 1, 0, 0, 0),
        TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0),
        TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0),
        TestUtils.toTimestamp(2015, 5, 29, 0, 0, 0),
        TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0),
        TestUtils.toTimestamp(2015, 0, 1, 0, 0, 0),
        TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0),
        TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0),
        TestUtils.toTimestamp(2015, 5, 29, 0, 0, 0),
        TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0),
        TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0),
        TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0),
        TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0)
      ))
    )

    checkSqlStatement(
      s"""
         |SELECT ( DATE_TRUNC ( \\'YEAR\\' , CAST ( "SUBQUERY_1"."TESTDATE" AS TIMESTAMP ) ) ) AS "SUBQUERY_2_COL_0" ,
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
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTTIMESTAMP" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTTIMESTAMP" > \\'2015-07-02 00:00:00\\' ::TIMESTAMP ) ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }
}

class DefaultPushdownDateTimeSuite extends PushdownDateTimeSuite {
  override protected val s3format: String = "DEFAULT"

  test("timestamptz datatype support") {
    val test_tz_table: String = s""""PUBLIC"."pushdown_suite_tz_test_table_$randomSuffix""""

    try {
      createDateTimeTZDataInRedshift(test_tz_table)

      read
        .option("dbtable", test_tz_table)
        .load()
        .createOrReplaceTempView("test_tz_table")


      checkAnswer(
        sqlContext.sql(
          s"""SELECT testid, testtimestamp,  testtimestamptz FROM test_tz_table
             |WHERE testtimestamp >= cast('2015-07-01 00:00:00-00' as timestamp) order by testid
             |""".stripMargin
        ),
        Seq(
          Row(0,
            TestUtils.toTimestamp(2018, 5, 1, 9, 59, 59),
            new Timestamp(TestUtils.toMillis(
              2018, 5, 1, 14, 59, 59, 0, "UTC"))
          )
          ,
          Row(1,
            TestUtils.toTimestamp(2018, 11, 17, 7, 37, 16),
            new Timestamp(TestUtils.toMillis(
              2018, 11, 17, 15, 37, 16, 0, "UTC"))
          )
          ,
          Row(2,
            TestUtils.toTimestamp(2018, 11, 17, 7, 37, 16),
            new Timestamp(TestUtils.toMillis(
              2018, 11, 17, 7, 37, 16, 0, "UTC"))
          )
          ,
          Row(3,
            TestUtils.toTimestamp(2018, 11, 17, 7, 37, 16),
            new Timestamp(TestUtils.toMillis(
              2018, 11, 17, 15, 37, 16, 0, "UTC"))
          )
        )
      )

      checkSqlStatement(
        s"""SELECT * FROM ( SELECT * FROM ( SELECT * FROM $test_tz_table
           |AS "RS_CONNECTOR_QUERY_ALIAS" )
           |AS "SUBQUERY_0"
           |WHERE ( ( "SUBQUERY_0"."TESTTIMESTAMP" IS NOT NULL )
           |AND ( "SUBQUERY_0"."TESTTIMESTAMP" >= \\'2015-06-30 17:00:00\\' ::TIMESTAMP ) ) )
           |AS "SUBQUERY_1" ORDER BY ( "SUBQUERY_1"."TESTID" ) ASC
           |""".stripMargin
      )

    } finally {
      conn.prepareStatement(s"drop table if exists $test_tz_table").executeUpdate()
    }
  }

  protected def createDateTimeTZDataInRedshift(tableName: String): Unit = {
    conn.createStatement().executeUpdate(
      s"""
         |create table $tableName (
         |testid int,
         |testtimestamp timestamp,
         |testtimestamptz timestamptz
         |)
      """.stripMargin
    )
    // scalastyle:off
    conn.createStatement().executeUpdate(
      s"""
         |insert into $tableName values
         |(null, null, null),
         |(0, 'Jun 1,2018  09:59:59', 'Jun 1,2018 09:59:59 EST'),
         |(1, '2018-12-17 07:37:16','2018-12-17 07:37:16-08'),
         |(2, '12/17/2018 07:37:16.00','12/17/2018 07:37:16.00 UTC'),
         |(3, '12.17.2018 07:37:16.00','12.17.2018 07:37:16.00 US/Pacific')
         """.stripMargin
    )
    // scalastyle:on
  }
}

class ParquetPushdownDateTimeSuite extends PushdownDateTimeSuite {
  override protected val s3format: String = "PARQUET"
}
