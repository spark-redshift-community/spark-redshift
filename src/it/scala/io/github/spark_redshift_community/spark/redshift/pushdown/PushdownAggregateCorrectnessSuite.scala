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
import org.scalatest.DoNotDiscover

import java.sql.Timestamp
import java.time.ZonedDateTime


abstract class PushdownAggregateCorrectnessSuite extends AggregateCountCorrectnessSuite
                                                 with AggregateMaxCorrectnessSuite
                                                 with AggregateMinCorrectnessSuite
                                                 with AggregateAvgCorrectnessSuite
                                                 with AggregateSumCorrectnessSuite {
  override protected val preloaded_data: String = "true"
  override def setTestTableName(): String = """"PUBLIC"."all_shapes_dist_all_sort_compound_12col""""

  // define a test for count(*)
  test("Test COUNT(*) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount00)
    doTest(sqlContext, testCount01)
  }

  // define a test for count(short)
  test("Test COUNT(short) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount10)
    doTest(sqlContext, testCount10_2)
    doTest(sqlContext, testCount11)
    doTest(sqlContext, testCount12)
    doTest(sqlContext, testCount13)
    doTest(sqlContext, testCount14)
    doTest(sqlContext, testCount15)
    doTest(sqlContext, testCount16)
    // group by
    doTest(sqlContext, testCount17)
    doTest(sqlContext, testCount17_2)
  }

  // define a test for count(int)
  test("Test COUNT(int) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount20)
    doTest(sqlContext, testCount20_2)
    doTest(sqlContext, testCount21)
    doTest(sqlContext, testCount21_2)
    doTest(sqlContext, testCount22)
    doTest(sqlContext, testCount22_2)
    doTest(sqlContext, testCount23)
    doTest(sqlContext, testCount23_2)
    doTest(sqlContext, testCount24)
    doTest(sqlContext, testCount25)
    doTest(sqlContext, testCount26)
    // group by
    doTest(sqlContext, testCount27)
    doTest(sqlContext, testCount27_2)
  }

  // define a test for count(long)
  test("Test COUNT(long) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount30)
    doTest(sqlContext, testCount31)
    doTest(sqlContext, testCount32)
    doTest(sqlContext, testCount33)
    doTest(sqlContext, testCount34)
    doTest(sqlContext, testCount35)
    doTest(sqlContext, testCount36)
    // group by
    doTest(sqlContext, testCount37)
    doTest(sqlContext, testCount37_2)
  }

  // define a test for count(decimal)
  test("Test COUNT(decimal) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount40)
    doTest(sqlContext, testCount41)
    doTest(sqlContext, testCount42)
    doTest(sqlContext, testCount43)
    doTest(sqlContext, testCount44)
    doTest(sqlContext, testCount45)
    doTest(sqlContext, testCount46)
    doTest(sqlContext, testCount47)
    doTest(sqlContext, testCount48)
    // group by
    doTest(sqlContext, testCount49)
  }

  // define a test for count(float)
  test("Test COUNT(float) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount50)
    doTest(sqlContext, testCount51)
    doTest(sqlContext, testCount52)
    doTest(sqlContext, testCount53)
    doTest(sqlContext, testCount54)
    doTest(sqlContext, testCount55)
    doTest(sqlContext, testCount56)
    doTest(sqlContext, testCount57)
    // group by
    doTest(sqlContext, testCount58)
    doTest(sqlContext, testCount59)
  }

  // define a test for count(boolean)
  test("Test COUNT(boolean) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount60)
    doTest(sqlContext, testCount61)
    doTest(sqlContext, testCount62)
    // group by
    doTest(sqlContext, testCount63)
    doTest(sqlContext, testCount63_2)
    doTest(sqlContext, testCount64)
    doTest(sqlContext, testCount64_2)
  }

  // define a test for count(char)
  test("Test COUNT(char) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount70)
    doTest(sqlContext, testCount71)
    doTest(sqlContext, testCount72)
    doTest(sqlContext, testCount73)
  }

  // define a test for count(varchar)
  test("Test COUNT(varchar) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount80)
    doTest(sqlContext, testCount81)
    doTest(sqlContext, testCount82)
    doTest(sqlContext, testCount83)
    // group by
    doTest(sqlContext, testCount84)
  }

  // define a test for count(date)
  test("Test COUNT(date) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount90)
    doTest(sqlContext, testCount91)
    doTest(sqlContext, testCount92)
    doTest(sqlContext, testCount93)
    doTest(sqlContext, testCount94)
    doTest(sqlContext, testCount95)
    doTest(sqlContext, testCount96)
    // group by
    doTest(sqlContext, testCount97)
  }

  // define a test for count(timestamp)
  test("Test COUNT(timestamp) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount100)
    doTest(sqlContext, testCount101)
    doTest(sqlContext, testCount102)
    doTest(sqlContext, testCount103)
    doTest(sqlContext, testCount104)
    doTest(sqlContext, testCount105)
    doTest(sqlContext, testCount106)
    // group by
    doTest(sqlContext, testCount107)
  }

  // define a test for count(timestamptz)
  test("Test COUNT(timestamptz) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount110)
    doTest(sqlContext, testCount111)
    doTest(sqlContext, testCount112)
    doTest(sqlContext, testCount113)
    doTest(sqlContext, testCount114)
    doTest(sqlContext, testCount115)
    doTest(sqlContext, testCount116)
  }

  // define a test for max(short)
  test("Test MAX(short) aggregation statements against correctness dataset") {
    doTest(sqlContext, testMax00)
    doTest(sqlContext, testMax01)
    doTest(sqlContext, testMax02)
    doTest(sqlContext, testMax03)
    doTest(sqlContext, testMax04)
    doTest(sqlContext, testMax05)
    doTest(sqlContext, testMax06)
    // group by
    doTest(sqlContext, testMax07)
  }

  // define a test for max(int)
  test("Test MAX(int) aggregation statements against correctness dataset") {
    doTest(sqlContext, testMax10)
    doTest(sqlContext, testMax11)
    doTest(sqlContext, testMax12)
    doTest(sqlContext, testMax13)
    doTest(sqlContext, testMax14)
    doTest(sqlContext, testMax15)
    doTest(sqlContext, testMax16)
    // group by
    doTest(sqlContext, testMax17)
  }

  // define a test for max(long)
  test("Test MAX(long) aggregation statements against correctness dataset") {
    doTest(sqlContext, testMax20)
    doTest(sqlContext, testMax21)
    doTest(sqlContext, testMax22)
    doTest(sqlContext, testMax23)
    doTest(sqlContext, testMax24)
    doTest(sqlContext, testMax25)
    doTest(sqlContext, testMax26)
    // group by
    doTest(sqlContext, testMax27)
  }

  // define a test for max(decimal)
  test("Test MAX(decimal) aggregation statements against correctness dataset") {
    doTest(sqlContext, testMax30)
    doTest(sqlContext, testMax31)
    doTest(sqlContext, testMax32)
    doTest(sqlContext, testMax33)
    doTest(sqlContext, testMax34)
    doTest(sqlContext, testMax35)
    doTest(sqlContext, testMax36)
    doTest(sqlContext, testMax37)
    doTest(sqlContext, testMax38)
    // group by
    doTest(sqlContext, testMax39)
  }

  // define a test for max(float)
  test("Test MAX(float) aggregation statements against correctness dataset") {
    doTest(sqlContext, testMax40)
    doTest(sqlContext, testMax41)
    doTest(sqlContext, testMax42)
    doTest(sqlContext, testMax43)
    doTest(sqlContext, testMax44)
    doTest(sqlContext, testMax45)
    doTest(sqlContext, testMax46)
    doTest(sqlContext, testMax47)
    // group by
    doTest(sqlContext, testMax48)
    doTest(sqlContext, testMax49)
  }

  // define a test for max(boolean)
  // function max(boolean) does not exist in redshift
  test("Test MAX(boolean) aggregation statements against correctness dataset") {
    doTest(sqlContext, testMax50_unsupported)
    doTest(sqlContext, testMax51_unsupported)
    doTest(sqlContext, testMax52_unsupported)
    // group by
    doTest(sqlContext, testMax53)
  }

  // define a test for max(varchar)
  test("Test MAX(varchar) aggregation statements against correctness dataset") {
    doTest(sqlContext, testMax70)
    doTest(sqlContext, testMax71)
    doTest(sqlContext, testMax72)
    doTest(sqlContext, testMax73)
    // group by
    doTest(sqlContext, testMax74)
  }

  // define a test for max(date)
  test("Test MAX(date) aggregation statements against correctness dataset") {
    doTest(sqlContext, testMax80)
    doTest(sqlContext, testMax81)
    doTest(sqlContext, testMax82)
    doTest(sqlContext, testMax83)
    doTest(sqlContext, testMax84)
    doTest(sqlContext, testMax85)
    doTest(sqlContext, testMax86)
    // group by
    doTest(sqlContext, testMax87)
  }

  // define a test for max(timestamp)
  test("Test MAX(timestamp) aggregation statements against correctness dataset") {
    doTest(sqlContext, testMax90)
    doTest(sqlContext, testMax91)
    doTest(sqlContext, testMax92)
    doTest(sqlContext, testMax93)
    doTest(sqlContext, testMax94)
    doTest(sqlContext, testMax95)
    doTest(sqlContext, testMax96)
    // group by
    doTest(sqlContext, testMax97)
  }

  // define a test for min(short)
  test("Test MIN(short) aggregation statements against correctness dataset") {
    doTest(sqlContext, testMin00)
    doTest(sqlContext, testMin01)
    doTest(sqlContext, testMin02)
    doTest(sqlContext, testMin03)
    doTest(sqlContext, testMin04)
    doTest(sqlContext, testMin05)
    doTest(sqlContext, testMin06)
    // group by
    doTest(sqlContext, testMin07)
  }

  // define a test for min(int)
  test("Test MIN(int) aggregation statements against correctness dataset") {
    doTest(sqlContext, testMin10)
    doTest(sqlContext, testMin11)
    doTest(sqlContext, testMin12)
    doTest(sqlContext, testMin13)
    doTest(sqlContext, testMin14)
    doTest(sqlContext, testMin15)
    doTest(sqlContext, testMin16)
    // group by
    doTest(sqlContext, testMin17)
  }

  // define a test for min(long)
  test("Test MIN(long) aggregation statements against correctness dataset") {
    doTest(sqlContext, testMin20)
    doTest(sqlContext, testMin21)
    doTest(sqlContext, testMin22)
    doTest(sqlContext, testMin23)
    doTest(sqlContext, testMin24)
    doTest(sqlContext, testMin25)
    doTest(sqlContext, testMin26)
    // group by
    doTest(sqlContext, testMin27)
  }

  // define a test for min(decimal)
  test("Test MIN(decimal) aggregation statements against correctness dataset") {
    doTest(sqlContext, testMin30)
    doTest(sqlContext, testMin31)
    doTest(sqlContext, testMin32)
    doTest(sqlContext, testMin33)
    doTest(sqlContext, testMin34)
    doTest(sqlContext, testMin35)
    doTest(sqlContext, testMin36)
    doTest(sqlContext, testMin37)
    doTest(sqlContext, testMin38)
    // group by
    doTest(sqlContext, testMin39)
  }

  // define a test for min(float)
  test("Test MIN(float) aggregation statements against correctness dataset") {
    doTest(sqlContext, testMin40)
    doTest(sqlContext, testMin41)
    doTest(sqlContext, testMin42)
    doTest(sqlContext, testMin43)
    doTest(sqlContext, testMin44)
    doTest(sqlContext, testMin45)
    doTest(sqlContext, testMin46)
    doTest(sqlContext, testMin47)
    // group by
    doTest(sqlContext, testMin48)
    doTest(sqlContext, testMin49)
  }

  // define a test for min(boolean)
  // function min(boolean) does not exist in redshift
  test("Test MIN(boolean) aggregation statements against correctness dataset") {
    doTest(sqlContext, testMin50_unsupported)
    doTest(sqlContext, testMin51_unsupported)
    doTest(sqlContext, testMin52_unsupported)
    // group by
    doTest(sqlContext, testMin53)
  }

  // define a test for min(varchar)
  test("Test MIN(varchar) aggregation statements against correctness dataset") {
    doTest(sqlContext, testMin70)
    doTest(sqlContext, testMin71)
    doTest(sqlContext, testMin72)
    doTest(sqlContext, testMin73)
    // group by
    doTest(sqlContext, testMin74)
  }

  // define a test for min(date)
  test("Test MIN(date) aggregation statements against correctness dataset") {
    doTest(sqlContext, testMin80)
    doTest(sqlContext, testMin81)
    doTest(sqlContext, testMin82)
    doTest(sqlContext, testMin83)
    doTest(sqlContext, testMin84)
    doTest(sqlContext, testMin85)
    doTest(sqlContext, testMin86)
    // group by
    doTest(sqlContext, testMin87)
  }

  // define a test for min(timestamp)
  test("Test MIN(timestamp) aggregation statements against correctness dataset") {
    doTest(sqlContext, testMin90)
    doTest(sqlContext, testMin91)
    doTest(sqlContext, testMin92)
    doTest(sqlContext, testMin93)
    doTest(sqlContext, testMin94)
    doTest(sqlContext, testMin95)
    doTest(sqlContext, testMin96)
    // group by
    doTest(sqlContext, testMin97)
  }

  // define a test for avg(short)
  test("Test AVG(short) aggregation statements against correctness dataset") {
    doTest(sqlContext, testAvg00)
    doTest(sqlContext, testAvg01)
    doTest(sqlContext, testAvg02)
    doTest(sqlContext, testAvg03)
    doTest(sqlContext, testAvg04)
    doTest(sqlContext, testAvg05)
    doTest(sqlContext, testAvg06)
    // group by
    doTest(sqlContext, testAvg07)
  }

  // define a test for avg(int)
  test("Test AVG(int) aggregation statements against correctness dataset") {
    doTest(sqlContext, testAvg10)
    doTest(sqlContext, testAvg11)
    doTest(sqlContext, testAvg12)
    doTest(sqlContext, testAvg13)
    doTest(sqlContext, testAvg14)
    doTest(sqlContext, testAvg15)
    doTest(sqlContext, testAvg16)
    // group by
    doTest(sqlContext, testAvg17)
    doTest(sqlContext, testAvg18)
  }

  // define a test for avg(long)
  test("Test AVG(long) aggregation statements against correctness dataset") {
    doTest(sqlContext, testAvg20)
    doTest(sqlContext, testAvg21)
    doTest(sqlContext, testAvg22)
    doTest(sqlContext, testAvg23)
    doTest(sqlContext, testAvg24)
    doTest(sqlContext, testAvg25)
    doTest(sqlContext, testAvg26)
    // group by
    doTest(sqlContext, testAvg27)
  }

  // define a test for avg(decimal)
  test("Test AVG(decimal) aggregation statements against correctness dataset") {
    // doTest(sqlContext, testAvg30)
    doTest(sqlContext, testAvg31)
    doTest(sqlContext, testAvg32)
    doTest(sqlContext, testAvg33)
    doTest(sqlContext, testAvg34)
    // group by
    doTest(sqlContext, testAvg35)
  }

  // define a test for avg(float)
  test("Test AVG(float) aggregation statements against correctness dataset") {
    doTest(sqlContext, testAvg40)
    doTest(sqlContext, testAvg41)
    doTest(sqlContext, testAvg42)
    doTest(sqlContext, testAvg43)
    doTest(sqlContext, testAvg44)
    doTest(sqlContext, testAvg45)
    doTest(sqlContext, testAvg46)
    doTest(sqlContext, testAvg47)
    // group by
    doTest(sqlContext, testAvg48)
    doTest(sqlContext, testAvg49)
  }

  // define a test for sum(short)
  test("Test SUM(short) aggregation statements against correctness dataset") {
    doTest(sqlContext, testSum00)
    doTest(sqlContext, testSum01)
    doTest(sqlContext, testSum02)
    doTest(sqlContext, testSum03)
  }

  // define a test for sum(int)
  test("Test SUM(int) aggregation statements against correctness dataset") {
    doTest(sqlContext, testSum10)
    doTest(sqlContext, testSum11)
    doTest(sqlContext, testSum12)
    doTest(sqlContext, testSum13)
  }

  // define a test for sum(long)
  test("Test SUM(long) aggregation statements against correctness dataset") {
    doTest(sqlContext, testSum20)
    doTest(sqlContext, testSum21)
    doTest(sqlContext, testSum22)
    doTest(sqlContext, testSum23)
  }

  // define a test for sum(decimal)
  test("Test SUM(decimal) aggregation statements against correctness dataset") {
    doTest(sqlContext, testSum30)
    doTest(sqlContext, testSum31)
    doTest(sqlContext, testSum32)
    doTest(sqlContext, testSum33)
  }

  // define a test for sum(float)
  test("Test SUM(float) aggregation statements against correctness dataset") {
    doTest(sqlContext, testSum40)
    doTest(sqlContext, testSum41)
    doTest(sqlContext, testSum42)
    doTest(sqlContext, testSum43)
    doTest(sqlContext, testSum44)
    doTest(sqlContext, testSum45)
    doTest(sqlContext, testSum46)
    doTest(sqlContext, testSum47)
  }

  // Timestamptz column is not handled correctly in parquet format as time zone information is not
  // unloaded.
  // Issue is tracked in [Redshift-6844]
  test("Test COUNT(timestamptz) aggregation statements against correctness dataset with group by",
    TimestamptzTest) {
    checkAnswer(
      sqlContext.sql(
        """SELECT col_timestamptz_zstd, COUNT(col_timestamptz_zstd) FROM test_table
          | group by col_timestamptz_zstd
          | order by col_timestamptz_zstd limit 5""".stripMargin),
      Seq(Row(Timestamp.from(
        ZonedDateTime.parse("1970-01-01 07:08:06 UTC", formatter).toInstant), 1),
        Row(Timestamp.from(
          ZonedDateTime.parse("1970-01-01 17:04:44 UTC", formatter).toInstant), 1),
        Row(Timestamp.from(
          ZonedDateTime.parse("1970-01-02 23:12:59 UTC", formatter).toInstant), 1),
        Row(Timestamp.from(
          ZonedDateTime.parse("1970-01-07 04:12:15 UTC", formatter).toInstant), 1),
        Row(Timestamp.from(
          ZonedDateTime.parse("1970-01-15 05:54:50 UTC", formatter).toInstant), 1)))

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT * FROM (
         | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_0",
         | ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
         | SELECT ( "SUBQUERY_0"."COL_TIMESTAMPTZ_ZSTD" ) AS "SUBQUERY_1_COL_0" FROM (
         | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
         | AS "SUBQUERY_1" GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2"
         | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS"SUBQUERY_3"
         | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
         | LIMIT 5""".stripMargin)
  }

  test("Test MAX(timestamptz) aggregation statements against correctness dataset",
    TimestamptzTest) {
    // "Column name" and result set
    val input = List(
      ("col_timestamptz_raw", Seq(Row(Timestamp.from(
        ZonedDateTime.parse("2018-10-10 04:08:15 UTC", formatter).toInstant)))),
      ("col_timestamptz_bytedict", Seq(Row(Timestamp.from(
        ZonedDateTime.parse("2018-10-16 05:30:44 UTC", formatter).toInstant)))),
      ("col_timestamptz_delta", Seq(Row(Timestamp.from(
        ZonedDateTime.parse("2018-10-16 21:18:03 UTC", formatter).toInstant)))),
      ("col_timestamptz_delta32k", Seq(Row(Timestamp.from(
        ZonedDateTime.parse("2018-10-16 10:31:55 UTC", formatter).toInstant)))),
      ("col_timestamptz_lzo", Seq(Row(Timestamp.from(
        ZonedDateTime.parse("2018-10-15 03:45:35 UTC", formatter).toInstant)))),
      ("col_timestamptz_runlength", Seq(Row(Timestamp.from(
        ZonedDateTime.parse("2018-10-14 22:00:57 UTC", formatter).toInstant)))),
      ("col_timestamptz_zstd", Seq(Row(Timestamp.from(
        ZonedDateTime.parse("2018-10-15 19:50:18 UTC", formatter).toInstant))))
    )
    input.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT MAX($column_name) FROM test_table""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT ( MAX ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }

  test("Test MAX(timestamptz) aggregation statements against correctness dataset with group by",
    TimestamptzTest) {
    checkAnswer(
      sqlContext.sql(
        """SELECT col_timestamptz_zstd, MAX(col_timestamptz_lzo) FROM test_table
          | group by col_timestamptz_zstd
          | order by col_timestamptz_zstd limit 5""".stripMargin),
      Seq(Row(Timestamp.from(ZonedDateTime.parse("1970-01-01 07:08:06 UTC", formatter).toInstant),
        Timestamp.from(ZonedDateTime.parse("2001-09-28 19:24:00 UTC", formatter).toInstant)),
        Row(Timestamp.from(ZonedDateTime.parse("1970-01-01 17:04:44 UTC", formatter).toInstant),
          Timestamp.from(ZonedDateTime.parse("1976-01-08 07:53:08 UTC", formatter).toInstant)),
        Row(Timestamp.from(ZonedDateTime.parse("1970-01-02 23:12:59 UTC", formatter).toInstant),
          Timestamp.from(ZonedDateTime.parse("1988-06-22 15:43:27 UTC", formatter).toInstant)),
        Row(Timestamp.from(ZonedDateTime.parse("1970-01-07 04:12:15 UTC", formatter).toInstant),
          Timestamp.from(ZonedDateTime.parse("2006-01-13 00:40:53 UTC", formatter).toInstant)),
        Row(Timestamp.from(ZonedDateTime.parse("1970-01-15 05:54:50 UTC", formatter).toInstant),
          Timestamp.from(ZonedDateTime.parse("2014-05-21 05:54:35 UTC", formatter).toInstant))))

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT * FROM (
         | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
         | ( MAX ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
         | SELECT ( "SUBQUERY_0"."COL_TIMESTAMPTZ_LZO" ) AS "SUBQUERY_1_COL_0",
         | ( "SUBQUERY_0"."COL_TIMESTAMPTZ_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
         | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0")
         | AS "SUBQUERY_1" GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
         | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS "SUBQUERY_3"
         | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
         | LIMIT 5""".stripMargin)
  }

  test("Test MIN(timestamptz) aggregation statements against correctness dataset",
    TimestamptzTest) {
    // "Column name" and result set
    val input = List(
      ("col_timestamptz_raw", Seq(Row(Timestamp.from(
        ZonedDateTime.parse("1970-01-10 09:00:12 UTC", formatter).toInstant)))),
      ("col_timestamptz_bytedict", Seq(Row(Timestamp.from(
        ZonedDateTime.parse("1970-01-08 23:00:15 UTC", formatter).toInstant)))),
      ("col_timestamptz_delta", Seq(Row(Timestamp.from(
        ZonedDateTime.parse("1970-01-01 04:50:02 UTC", formatter).toInstant)))),
      ("col_timestamptz_delta32k", Seq(Row(Timestamp.from(
        ZonedDateTime.parse("1970-01-01 01:40:18 UTC", formatter).toInstant)))),
      ("col_timestamptz_lzo", Seq(Row(Timestamp.from(
        ZonedDateTime.parse("1970-01-03 15:24:06 UTC", formatter).toInstant)))),
      ("col_timestamptz_runlength", Seq(Row(Timestamp.from(
        ZonedDateTime.parse("1970-01-06 11:54:44 UTC", formatter).toInstant)))),
      ("col_timestamptz_zstd", Seq(Row(Timestamp.from(
        ZonedDateTime.parse("1970-01-01 07:08:06 UTC", formatter).toInstant))))
    )
    input.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT MIN($column_name) FROM test_table""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }

  test("Test MIN(timestamptz) aggregation statements against correctness dataset with group by",
    TimestamptzTest) {
    checkAnswer(
      sqlContext.sql(
        """SELECT col_timestamptz_zstd, MIN(col_timestamptz_lzo) FROM test_table
          | group by col_timestamptz_zstd
          | order by col_timestamptz_zstd limit 5""".stripMargin),
      Seq(Row(Timestamp.from(ZonedDateTime.parse("1970-01-01 07:08:06 UTC", formatter).toInstant),
        Timestamp.from(ZonedDateTime.parse("2001-09-28 19:24:00 UTC", formatter).toInstant)),
        Row(Timestamp.from(ZonedDateTime.parse("1970-01-01 17:04:44 UTC", formatter).toInstant),
          Timestamp.from(ZonedDateTime.parse("1976-01-08 07:53:08 UTC", formatter).toInstant)),
        Row(Timestamp.from(ZonedDateTime.parse("1970-01-02 23:12:59 UTC", formatter).toInstant),
          Timestamp.from(ZonedDateTime.parse("1988-06-22 15:43:27 UTC", formatter).toInstant)),
        Row(Timestamp.from(ZonedDateTime.parse("1970-01-07 04:12:15 UTC", formatter).toInstant),
          Timestamp.from(ZonedDateTime.parse("2006-01-13 00:40:53 UTC", formatter).toInstant)),
        Row(Timestamp.from(ZonedDateTime.parse("1970-01-15 05:54:50 UTC", formatter).toInstant),
          Timestamp.from(ZonedDateTime.parse("2014-05-21 05:54:35 UTC", formatter).toInstant))))

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT * FROM (
         | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
         | ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
         | SELECT ( "SUBQUERY_0"."COL_TIMESTAMPTZ_LZO" ) AS "SUBQUERY_1_COL_0",
         | ( "SUBQUERY_0"."COL_TIMESTAMPTZ_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
         | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0")
         | AS "SUBQUERY_1" GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
         | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS "SUBQUERY_3"
         | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
         | LIMIT 5""".stripMargin)
  }
}

// Please comment out this tag when you have set up test framework, preloaded dataset,
// and want to run this against it.
@DoNotDiscover
class DefaultPushdownAggregateCorrectnessSuite extends PushdownAggregateCorrectnessSuite {
  override protected val s3format: String = "DEFAULT"
  override protected val auto_pushdown: String = "true"

  // Unloading fixed-length char column data in parquet format does not trim the trailing spaces.
  // Issue is tracked in [Redshift-7057].
  test("Test COUNT(char) aggregation statements against correctness dataset with group by") {
    doTest(sqlContext, testCount74)
  }

  // define a test for max(char)
  test("Test MAX(char) aggregation statements against correctness dataset") {
    doTest(sqlContext, testMax60)
    doTest(sqlContext, testMax61)
    doTest(sqlContext, testMax62)
    doTest(sqlContext, testMax63)
    // group by
    doTest(sqlContext, testMax64)
  }

  // define a test for min(char)
  test("Test MIN(char) aggregation statements against correctness dataset") {
    doTest(sqlContext, testMin60)
    doTest(sqlContext, testMin61)
    doTest(sqlContext, testMin62)
    doTest(sqlContext, testMin63)
    // group by
    doTest(sqlContext, testMin64)
  }
}

@DoNotDiscover
class ParquetPushdownAggregateCorrectnessSuite extends PushdownAggregateCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "true"
}

@DoNotDiscover
class DefaultNoPushdownAggregateCorrectnessSuite extends DefaultPushdownAggregateCorrectnessSuite {
  override protected val s3format: String = "DEFAULT"
  override protected val auto_pushdown: String = "false"
}

@DoNotDiscover
class ParquetNoPushdownDAggregateCorrectnessSuite extends PushdownAggregateCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}
