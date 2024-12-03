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
  test("Test COUNT(*) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(testCount00, testCount01)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for count(short)
  test("Test COUNT(short) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
      testCount10,
      testCount10_2,
      testCount11,
      testCount12,
      testCount13,
      testCount14,
      testCount15,
      testCount16,
      // group by
      testCount17,
      testCount17_2
    )

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for count(int)
  test("Test COUNT(int) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testCount20,
    testCount20_2,
    testCount21,
    testCount21_2,
    testCount22,
    testCount22_2,
    testCount23,
    testCount23_2,
    testCount24,
    testCount25,
    testCount26,
    // group by
    testCount27,
    testCount27_2)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for count(long)
  test("Test COUNT(long) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testCount30,
    testCount31,
    testCount32,
    testCount33,
    testCount34,
    testCount35,
    testCount36,
    // group by
    testCount37,
    testCount37_2)

    cases.par.foreach {doTest(sqlContext, _)}
  }

  // define a test for count(decimal)
  test("Test COUNT(decimal) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testCount40,
    testCount41,
    testCount42,
    testCount43,
    testCount44,
    testCount45,
    testCount46,
    testCount47,
    testCount48,
    // group by
    testCount49)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for count(float)
  test("Test COUNT(float) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testCount50,
    testCount51,
    testCount52,
    testCount53,
    testCount54,
    testCount55,
    testCount56,
    testCount57,
    // group by
    testCount58,
    testCount59)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for count(boolean)
  test("Test COUNT(boolean) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
     testCount60,
     testCount61,
     testCount62,
    // group by
    testCount63,
    testCount63_2,
    testCount64,
    testCount64_2)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for count(char)
  test("Test COUNT(char) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testCount70,
    testCount71,
    testCount72,
    testCount73)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for count(varchar)
  test("Test COUNT(varchar) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testCount80,
    testCount81,
    testCount82,
    testCount83,
    // group by
    testCount84)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for count(date)
  test("Test COUNT(date) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testCount90,
    testCount91,
    testCount92,
    testCount93,
    testCount94,
    testCount95,
    testCount96,
    // group by
    testCount97)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for count(timestamp)
  test("Test COUNT(timestamp) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testCount100,
    testCount101,
    testCount102,
    testCount103,
    testCount104,
    testCount105,
    testCount106,
    // group by
     testCount107)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for count(timestamptz)
  test("Test COUNT(timestamptz) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testCount110,
    testCount111,
    testCount112,
    testCount113,
    testCount114,
    testCount115,
    testCount116)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for max(short)
  test("Test MAX(short) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testMax00,
    testMax01,
    testMax02,
    testMax03,
    testMax04,
    testMax05,
    testMax06,
    // group by
    testMax07)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for max(int)
  test("Test MAX(int) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testMax10,
    testMax11,
    testMax12,
    testMax13,
    testMax14,
    testMax15,
    testMax16,
    // group by
    testMax17)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for max(long)
  test("Test MAX(long) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testMax20,
    testMax21,
    testMax22,
    testMax23,
    testMax24,
    testMax25,
    testMax26,
    // group by
    testMax27)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for max(decimal)
  test("Test MAX(decimal) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testMax30,
    testMax31,
    testMax32,
    testMax33,
    testMax34,
    testMax35,
    testMax36,
    testMax37,
    testMax38,
    // group by
    testMax39)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for max(float)
  test("Test MAX(float) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testMax40,
    testMax41,
    testMax42,
    testMax43,
    testMax44,
    testMax45,
    testMax46,
    testMax47,
    // group by
    testMax48,
    testMax49)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for max(boolean)
  // function max(boolean) does not exist in redshift
  test("Test MAX(boolean) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testMax50_unsupported,
    testMax51_unsupported,
    testMax52_unsupported,
    // group by
    testMax53)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for max(varchar)
  test("Test MAX(varchar) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testMax70,
    testMax71,
    testMax72,
    testMax73,
    // group by
    testMax74)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for max(date)
  test("Test MAX(date) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testMax80,
    testMax81,
    testMax82,
    testMax83,
    testMax84,
    testMax85,
    testMax86,
    // group by
    testMax87)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for max(timestamp)
  test("Test MAX(timestamp) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testMax90,
    testMax91,
    testMax92,
    testMax93,
    testMax94,
    testMax95,
    testMax96,
    // group by
    testMax97)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for min(short)
  test("Test MIN(short) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testMin00,
    testMin01,
    testMin02,
    testMin03,
    testMin04,
    testMin05,
    testMin06,
    // group by
    testMin07)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for min(int)
  test("Test MIN(int) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testMin10,
    testMin11,
    testMin12,
    testMin13,
    testMin14,
    testMin15,
    testMin16,
    // group by
    testMin17)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for min(long)
  test("Test MIN(long) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testMin20,
    testMin21,
    testMin22,
    testMin23,
    testMin24,
    testMin25,
    testMin26,
    // group by
    testMin27)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for min(decimal)
  test("Test MIN(decimal) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testMin30,
    testMin31,
    testMin32,
    testMin33,
    testMin34,
    testMin35,
    testMin36,
    testMin37,
    testMin38,
    // group by
    testMin39)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for min(float)
  test("Test MIN(float) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testMin40,
    testMin41,
    testMin42,
    testMin43,
    testMin44,
    testMin45,
    testMin46,
    testMin47,
    // group by
    testMin48,
    testMin49)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for min(boolean)
  // function min(boolean) does not exist in redshift
  test("Test MIN(boolean) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testMin50_unsupported,
    testMin51_unsupported,
    testMin52_unsupported,
    // group by
    testMin53)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for min(varchar)
  test("Test MIN(varchar) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testMin70,
    testMin71,
    testMin72,
    testMin73,
    // group by
    testMin74)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for min(date)
  test("Test MIN(date) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testMin80,
    testMin81,
    testMin82,
    testMin83,
    testMin84,
    testMin85,
    testMin86,
    // group by
    testMin87)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for min(timestamp)
  test("Test MIN(timestamp) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testMin90,
    testMin91,
    testMin92,
    testMin93,
    testMin94,
    testMin95,
    testMin96,
    // group by
    testMin97)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for avg(short)
  test("Test AVG(short) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testAvg00,
    testAvg01,
    testAvg02,
    testAvg03,
    testAvg04,
    testAvg05,
    testAvg06,
    // group by
    testAvg07)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for avg(int)
  test("Test AVG(int) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testAvg10,
    testAvg11,
    testAvg12,
    testAvg13,
    testAvg14,
    testAvg15,
    testAvg16,
    // group by
    testAvg17,
    testAvg18)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for avg(long)
  test("Test AVG(long) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testAvg20,
    testAvg21,
    testAvg22,
    testAvg23,
    testAvg24,
    testAvg25,
    testAvg26,
    // group by
    testAvg27)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for avg(decimal)
  test("Test AVG(decimal) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    // testAvg30,
    testAvg31,
    testAvg32,
    testAvg33,
    testAvg34,
    // group by
    testAvg35)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for avg(float)
  test("Test AVG(float) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testAvg40,
    testAvg41,
    testAvg42,
    testAvg43,
    testAvg44,
    testAvg45,
    testAvg46,
    testAvg47,
    // group by
    testAvg48,
    testAvg49)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for sum(short)
  test("Test SUM(short) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testSum00,
    testSum01,
    testSum02,
    testSum03)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for sum(int)
  test("Test SUM(int) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testSum10,
    testSum11,
    testSum12,
    testSum13)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for sum(long)
  test("Test SUM(long) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testSum20,
    testSum21,
    testSum22,
    testSum23)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for sum(decimal)
  test("Test SUM(decimal) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testSum30,
    testSum31,
    testSum32,
    testSum33)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for sum(float)
  test("Test SUM(float) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testSum40,
    testSum41,
    testSum42,
    testSum43,
    testSum44,
    testSum45,
    testSum46,
    testSum47)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // Timestamptz column is not handled correctly in parquet format as time zone information is not
  // unloaded.
  // Issue is tracked in [Redshift-6844]
  test("Test COUNT(timestamptz) aggregation statements against correctness dataset with group by",
    TimestamptzTest, PreloadTest) {
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
         | SELECT ( "SQ_1"."SQ_1_COL_0" ) AS "SQ_2_COL_0",
         | ( COUNT ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_1" FROM (
         | SELECT ( "SQ_0"."COL_TIMESTAMPTZ_ZSTD" ) AS "SQ_1_COL_0" FROM (
         | SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
         | AS "SQ_1" GROUP BY "SQ_1"."SQ_1_COL_0" ) AS "SQ_2"
         | ORDER BY ( "SQ_2"."SQ_2_COL_0" ) ASC NULLS FIRST ) AS"SQ_3"
         | ORDER BY ( "SQ_3"."SQ_2_COL_0" ) ASC NULLS FIRST
         | LIMIT 5""".stripMargin)
  }

  test("Test MAX(timestamptz) aggregation statements against correctness dataset",
    TimestamptzTest, PreloadTest) {
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
    input.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT MAX($column_name) FROM test_table""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
           | FROM ( SELECT ( "SQ_0"."$column_name" ) AS "SQ_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
           | AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("Test MAX(timestamptz) aggregation statements against correctness dataset with group by",
    TimestamptzTest, PreloadTest) {
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
         | SELECT ( "SQ_1"."SQ_1_COL_1" ) AS "SQ_2_COL_0",
         | ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_1" FROM (
         | SELECT ( "SQ_0"."COL_TIMESTAMPTZ_LZO" ) AS "SQ_1_COL_0",
         | ( "SQ_0"."COL_TIMESTAMPTZ_ZSTD" ) AS "SQ_1_COL_1" FROM (
         | SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0")
         | AS "SQ_1" GROUP BY "SQ_1"."SQ_1_COL_1" ) AS "SQ_2"
         | ORDER BY ( "SQ_2"."SQ_2_COL_0" ) ASC NULLS FIRST ) AS "SQ_3"
         | ORDER BY ( "SQ_3"."SQ_2_COL_0" ) ASC NULLS FIRST
         | LIMIT 5""".stripMargin)
  }

  test("Test MIN(timestamptz) aggregation statements against correctness dataset",
    TimestamptzTest, PreloadTest) {
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
    input.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT MIN($column_name) FROM test_table""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT ( MIN ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
           | FROM ( SELECT ( "SQ_0"."$column_name" ) AS "SQ_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
           | AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("Test MIN(timestamptz) aggregation statements against correctness dataset with group by",
    TimestamptzTest, PreloadTest) {
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
         | SELECT ( "SQ_1"."SQ_1_COL_1" ) AS "SQ_2_COL_0",
         | ( MIN ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_1" FROM (
         | SELECT ( "SQ_0"."COL_TIMESTAMPTZ_LZO" ) AS "SQ_1_COL_0",
         | ( "SQ_0"."COL_TIMESTAMPTZ_ZSTD" ) AS "SQ_1_COL_1" FROM (
         | SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0")
         | AS "SQ_1" GROUP BY "SQ_1"."SQ_1_COL_1" ) AS "SQ_2"
         | ORDER BY ( "SQ_2"."SQ_2_COL_0" ) ASC NULLS FIRST ) AS "SQ_3"
         | ORDER BY ( "SQ_3"."SQ_2_COL_0" ) ASC NULLS FIRST
         | LIMIT 5""".stripMargin)
  }
}

class TextPushdownAggregateCorrectnessSuite extends PushdownAggregateCorrectnessSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "true"

  // Unloading fixed-length char column data in parquet format does not trim the trailing spaces.
  // Issue is tracked in [Redshift-7057].
  test("Test COUNT(char) aggregation statements against correctness dataset with group by",
    PreloadTest) {
    doTest(sqlContext, testCount74)
  }

  // define a test for max(char)
  test("Test MAX(char) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testMax60,
    testMax61,
    testMax62,
    testMax63,
    // group by
    testMax64)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // define a test for min(char)
  test("Test MIN(char) aggregation statements against correctness dataset", PreloadTest) {
    val cases = Seq(
    testMin60,
    testMin61,
    testMin62,
    testMin63,
    // group by
    testMin64)

    cases.par.foreach { doTest(sqlContext, _) }
  }
}

class ParquetPushdownAggregateCorrectnessSuite extends PushdownAggregateCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "true"
}

class TextNoPushdownAggregateCorrectnessSuite extends TextPushdownAggregateCorrectnessSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "false"
}

class ParquetNoPushdownAggregateCorrectnessSuite extends PushdownAggregateCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}

class TextNoCachePushdownAggregateCorrectnessSuite
  extends TextPushdownAggregateCorrectnessSuite {
  override protected val s3_result_cache = "false"
}

class ParquetNoCachePushdownAggregateCorrectnessSuite
  extends ParquetPushdownAggregateCorrectnessSuite {
  override protected val s3_result_cache = "false"
}
