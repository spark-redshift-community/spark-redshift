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

import java.sql.Date
import java.sql.Timestamp
import java.time.ZonedDateTime

trait AggregateCountCorrectnessSuite extends IntegrationPushdownSuiteBase {
  val testCount00: TestCase = TestCase(
    """SELECT COUNT(1) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( 1 ) ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount01: TestCase = TestCase(
    """SELECT COUNT(*) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( 1 ) ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount10: TestCase = TestCase(
    """SELECT COUNT(col_smallint_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount10_2: TestCase = TestCase(
    """SELECT COUNT(DISTINCT col_smallint_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(401)), // expectedResult
    s"""SELECT ( COUNT ( DISTINCT "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount11: TestCase = TestCase(
    """SELECT COUNT(col_smallint_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount12: TestCase = TestCase(
    """SELECT COUNT(col_smallint_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_DELTA" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount13: TestCase = TestCase(
    """SELECT COUNT(col_smallint_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount14: TestCase = TestCase(
    """SELECT COUNT(col_smallint_mostly8) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_MOSTLY8" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount15: TestCase = TestCase(
    """SELECT COUNT(col_smallint_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount16: TestCase = TestCase(
    """SELECT COUNT(col_smallint_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount17: TestCase = TestCase(
    """SELECT col_smallint_zstd, COUNT(col_smallint_zstd) FROM test_table
      | group by col_smallint_zstd
      | order by col_smallint_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-200, 18), Row(-199, 8), Row(-198, 12), Row(-197, 14), Row(-196, 17)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_0",
       | ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_SMALLINT_ZSTD" ) AS "SUBQUERY_1_COL_0" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS"SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )

  val testCount17_2: TestCase = TestCase(
    """SELECT col_smallint_zstd, COUNT(DISTINCT col_smallint_zstd) FROM test_table
      | group by col_smallint_zstd
      | order by col_smallint_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-200, 1), Row(-199, 1), Row(-198, 1), Row(-197, 1), Row(-196, 1)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_0",
       | ( COUNT ( DISTINCT "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS"SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_SMALLINT_ZSTD" ) AS "SUBQUERY_1_COL_0" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0") ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0") ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )

  val testCount20: TestCase = TestCase(
    """SELECT COUNT(col_int_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount20_2: TestCase = TestCase(
    """SELECT COUNT(DISTINCT col_int_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(992)), // expectedResult
    s"""SELECT ( COUNT ( DISTINCT "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount21: TestCase = TestCase(
    """SELECT COUNT(col_int_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount21_2: TestCase = TestCase(
    """SELECT COUNT(DISTINCT col_int_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(993)), // expectedResult
    s"""SELECT ( COUNT ( DISTINCT "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount22: TestCase = TestCase(
    """SELECT COUNT(col_int_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_DELTA" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount22_2: TestCase = TestCase(
    """SELECT COUNT(DISTINCT col_int_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(990)), // expectedResult
    s"""SELECT ( COUNT ( DISTINCT "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_DELTA" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount23: TestCase = TestCase(
    """SELECT COUNT(col_int_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount23_2: TestCase = TestCase(
    """SELECT COUNT(DISTINCT col_int_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(994)), // expectedResult
    s"""SELECT ( COUNT ( DISTINCT "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount24: TestCase = TestCase(
    """SELECT COUNT(col_int_mostly8) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_MOSTLY8" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount25: TestCase = TestCase(
    """SELECT COUNT(col_int_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount26: TestCase = TestCase(
    """SELECT COUNT(col_int_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount27: TestCase = TestCase(
    """SELECT col_int_runlength, COUNT(col_int_runlength) FROM test_table
      | group by col_int_runlength
      | order by col_int_runlength limit 5""".stripMargin, // sparkStatement
    Seq(Row(-500, 2), Row(-499, 4), Row(-498, 6), Row(-497, 8), Row(-496, 11)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_0",
       | ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_INT_RUNLENGTH" ) AS "SUBQUERY_1_COL_0" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS"SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )

  val testCount27_2: TestCase = TestCase(
    """SELECT col_int_runlength, COUNT(DISTINCT col_int_runlength) FROM test_table
      | group by col_int_runlength
      | order by col_int_runlength limit 5""".stripMargin, // sparkStatement
    Seq(Row(-500, 1), Row(-499, 1), Row(-498, 1), Row(-497, 1), Row(-496, 1)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_0",
       | ( COUNT ( DISTINCT "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_INT_RUNLENGTH" ) AS "SUBQUERY_1_COL_0" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS"SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )

  val testCount30: TestCase = TestCase(
    """SELECT COUNT(col_bigint_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BIGINT_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount31: TestCase = TestCase(
    """SELECT COUNT(col_bigint_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BIGINT_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount32: TestCase = TestCase(
    """SELECT COUNT(col_bigint_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BIGINT_DELTA" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount33: TestCase = TestCase(
    """SELECT COUNT(col_bigint_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BIGINT_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount34: TestCase = TestCase(
    """SELECT COUNT(col_bigint_mostly8) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BIGINT_MOSTLY8" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount35: TestCase = TestCase(
    """SELECT COUNT(col_bigint_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BIGINT_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount36: TestCase = TestCase(
    """SELECT COUNT(col_bigint_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BIGINT_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount37: TestCase = TestCase(
    """SELECT col_bigint_mostly8, COUNT(col_bigint_mostly8) FROM test_table
      | group by col_bigint_mostly8
      | order by col_bigint_mostly8 limit 5""".stripMargin, // sparkStatement
    Seq(Row(-9994, 1), Row(-9991, 1), Row(-9987, 2), Row(-9980, 2),
      Row(-9978, 1)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_0",
       | ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_BIGINT_MOSTLY8" ) AS "SUBQUERY_1_COL_0" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS"SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )


  val testCount37_2: TestCase = TestCase(
    """SELECT col_bigint_mostly8, COUNT(DISTINCT col_bigint_mostly8) FROM test_table
      | group by col_bigint_mostly8
      | order by col_bigint_mostly8 limit 5""".stripMargin, // sparkStatement
    Seq(Row(-9994, 1), Row(-9991, 1), Row(-9987, 1), Row(-9980, 1),
      Row(-9978, 1)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_0",
       | ( COUNT ( DISTINCT "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_BIGINT_MOSTLY8" ) AS "SUBQUERY_1_COL_0" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS"SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )

  val testCount40: TestCase = TestCase(
    """SELECT COUNT(col_decimal_1_0_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_1_0_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount41: TestCase = TestCase(
    """SELECT COUNT(col_decimal_18_0_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_18_0_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount42: TestCase = TestCase(
    """SELECT COUNT(col_decimal_18_18_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_18_18_DELTA" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount43: TestCase = TestCase(
    """SELECT COUNT(col_decimal_38_0_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_38_0_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount44: TestCase = TestCase(
    """SELECT COUNT(col_decimal_38_37_mostly8) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_38_37_MOSTLY8" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount45: TestCase = TestCase(
    """SELECT COUNT(col_decimal_38_37_mostly16) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_38_37_MOSTLY16" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount46: TestCase = TestCase(
    """SELECT COUNT(col_decimal_38_37_mostly32) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_38_37_MOSTLY32" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount47: TestCase = TestCase(
    """SELECT COUNT(col_decimal_18_0_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_18_0_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount48: TestCase = TestCase(
    """SELECT COUNT(col_decimal_18_18_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_18_18_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount49: TestCase = TestCase(
    """SELECT col_decimal_38_37_mostly32, COUNT(col_decimal_38_37_mostly32) FROM test_table
      | group by col_decimal_38_37_mostly32
      | order by col_decimal_38_37_mostly32 limit 5""".stripMargin, // sparkStatement
    Seq(Row(0.100037, 1),
      Row(0.10029566, 1),
      Row(0.10062834, 1),
      Row(0.10084006, 1),
      Row(0.10130769, 1)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_0",
       | ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_DECIMAL_38_37_MOSTLY32" ) AS "SUBQUERY_1_COL_0" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS"SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )

  val testCount50: TestCase = TestCase(
    """SELECT COUNT(col_float4_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT4_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount51: TestCase = TestCase(
    """SELECT COUNT(col_float4_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT4_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount52: TestCase = TestCase(
    """SELECT COUNT(col_float4_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT4_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount53: TestCase = TestCase(
    """SELECT COUNT(col_float4_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT4_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount54: TestCase = TestCase(
    """SELECT COUNT(col_float8_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT8_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount55: TestCase = TestCase(
    """SELECT COUNT(col_float8_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT8_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount56: TestCase = TestCase(
    """SELECT COUNT(col_float8_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT8_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount57: TestCase = TestCase(
    """SELECT COUNT(col_float8_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT8_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount58: TestCase = TestCase(
    """SELECT col_float4_zstd, COUNT(col_float4_zstd) FROM test_table
      | group by col_float4_zstd
      | order by col_float4_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-39.99878.toFloat, 1), Row(-39.968693.toFloat, 1),
      Row(-39.94803.toFloat, 1), Row(-39.920315.toFloat, 1),
      Row(-39.919968.toFloat, 1)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_0",
       | ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_FLOAT4_ZSTD" ) AS "SUBQUERY_1_COL_0" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS"SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )

  val testCount59: TestCase = TestCase(
    """SELECT col_float8_bytedict, COUNT(col_float8_bytedict) FROM test_table
      | group by col_float8_bytedict
      | order by col_float8_bytedict limit 5""".stripMargin, // sparkStatement
    Seq(Row(-59.96662513909352, 1), Row(-59.88922941808153, 1),
      Row(-59.88099700252659, 1), Row(-59.83914554423634, 1),
      Row(-59.80894236180991, 1)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_0",
       | ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_FLOAT8_BYTEDICT" ) AS "SUBQUERY_1_COL_0" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS"SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )

  val testCount60: TestCase = TestCase(
    """SELECT COUNT(col_boolean_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BOOLEAN_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount61: TestCase = TestCase(
    """SELECT COUNT(col_boolean_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BOOLEAN_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount62: TestCase = TestCase(
    """SELECT COUNT(col_boolean_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BOOLEAN_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount63: TestCase = TestCase(
    """SELECT col_boolean_zstd, COUNT(col_boolean_zstd) FROM test_table
      | group by col_boolean_zstd
      | order by col_boolean_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(false, 2473), Row(true, 2527)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_0",
       | ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_BOOLEAN_ZSTD" ) AS "SUBQUERY_1_COL_0" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS"SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )

  val testCount63_2: TestCase = TestCase(
    """SELECT col_boolean_zstd, COUNT(DISTINCT col_boolean_zstd) FROM test_table
      | group by col_boolean_zstd
      | order by col_boolean_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(false, 1), Row(true, 1)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_0",
       | ( COUNT ( DISTINCT "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_BOOLEAN_ZSTD" ) AS "SUBQUERY_1_COL_0" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS"SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )

  val testCount64: TestCase = TestCase(
    """SELECT col_boolean_zstd, COUNT(col_int_zstd) FROM test_table
      | group by col_boolean_zstd
      | order by col_boolean_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(false, 2473), Row(true, 2527)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_INT_ZSTD" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )

  val testCount64_2: TestCase = TestCase(
    """SELECT col_boolean_zstd, COUNT(DISTINCT col_int_zstd) FROM test_table
      | group by col_boolean_zstd
      | order by col_boolean_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(false, 916), Row(true, 926)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( COUNT ( DISTINCT "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_INT_ZSTD" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )

  val testCount70: TestCase = TestCase(
    """SELECT COUNT(col_char_1_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_CHAR_1_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount71: TestCase = TestCase(
    """SELECT COUNT(col_char_255_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_CHAR_255_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount72: TestCase = TestCase(
    """SELECT COUNT(col_char_2000_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_CHAR_2000_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount73: TestCase = TestCase(
    """SELECT COUNT(col_char_max_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_CHAR_MAX_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  // This test can only pass in default format as parquet format will have trailing spaces.
  val testCount74: TestCase = TestCase(
    """SELECT col_char_max_zstd, COUNT(col_char_max_zstd) FROM test_table
      | group by col_char_max_zstd
      | order by col_char_max_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row("A central require.", 1),
      Row("A compare stuff report trial place.", 1),
      Row("A from nearly she certain maybe choose.", 1),
      Row("A significant Democrat through.", 1),
      Row("Ability gas sport win.", 1)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_0",
       | ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_CHAR_MAX_ZSTD" ) AS "SUBQUERY_1_COL_0" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS"SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )

  val testCount80: TestCase = TestCase(
    """SELECT COUNT(col_varchar_1_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_VARCHAR_1_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount81: TestCase = TestCase(
    """SELECT COUNT(col_varchar_255_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_VARCHAR_255_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount82: TestCase = TestCase(
    """SELECT COUNT(col_varchar_2000_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_VARCHAR_2000_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount83: TestCase = TestCase(
    """SELECT COUNT(col_varchar_max_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_VARCHAR_MAX_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount84: TestCase = TestCase(
    """SELECT col_varchar_max_zstd, COUNT(col_varchar_max_zstd) FROM test_table
      | group by col_varchar_max_zstd
      | order by col_varchar_max_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row("A arm ready mean.", 1),
      Row("A become nation gas organization door.", 1),
      Row("A discussion forward debate.", 1),
      Row("A rest new apply.", 1),
      Row("A science whom seven and.", 1)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_0",
       | ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_VARCHAR_MAX_ZSTD" ) AS "SUBQUERY_1_COL_0" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS"SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )

  val testCount90: TestCase = TestCase(
    """SELECT COUNT(col_date_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DATE_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount91: TestCase = TestCase(
    """SELECT COUNT(col_date_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DATE_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount92: TestCase = TestCase(
    """SELECT COUNT(col_date_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DATE_DELTA" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount93: TestCase = TestCase(
    """SELECT COUNT(col_date_delta32k) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DATE_DELTA32K" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount94: TestCase = TestCase(
    """SELECT COUNT(col_date_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DATE_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount95: TestCase = TestCase(
    """SELECT COUNT(col_date_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DATE_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount96: TestCase = TestCase(
    """SELECT COUNT(col_date_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DATE_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount97: TestCase = TestCase(
    """SELECT col_date_zstd, COUNT(col_date_zstd) FROM test_table
      | group by col_date_zstd
      | order by col_date_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(Date.valueOf("1970-01-02"), 1),
      Row(Date.valueOf("1970-01-05"), 1),
      Row(Date.valueOf("1970-01-11"), 1),
      Row(Date.valueOf("1970-01-15"), 1),
      Row(Date.valueOf("1970-01-19"), 1)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_0",
       | ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_DATE_ZSTD" ) AS "SUBQUERY_1_COL_0" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS"SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )

  val testCount100: TestCase = TestCase(
    """SELECT COUNT(col_timestamp_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMP_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount101: TestCase = TestCase(
    """SELECT COUNT(col_timestamp_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMP_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount102: TestCase = TestCase(
    """SELECT COUNT(col_timestamp_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMP_DELTA" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount103: TestCase = TestCase(
    """SELECT COUNT(col_timestamp_delta32k) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMP_DELTA32K" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount104: TestCase = TestCase(
    """SELECT COUNT(col_timestamp_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMP_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount105: TestCase = TestCase(
    """SELECT COUNT(col_timestamp_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMP_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount106: TestCase = TestCase(
    """SELECT COUNT(col_timestamp_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMP_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount107: TestCase = TestCase(
    """SELECT col_timestamp_zstd, COUNT(col_timestamp_zstd) FROM test_table
      | group by col_timestamp_zstd
      | order by col_timestamp_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.valueOf("1970-01-01 06:36:17.0"), 1),
      Row(Timestamp.valueOf("1970-01-02 01:29:50.0"), 1),
      Row(Timestamp.valueOf("1970-01-03 14:49:06.0"), 1),
      Row(Timestamp.valueOf("1970-01-05 14:43:54.0"), 1),
      Row(Timestamp.valueOf("1970-01-05 20:52:40.0"), 1)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_0",
       | ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_TIMESTAMP_ZSTD" ) AS "SUBQUERY_1_COL_0" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS"SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )

  val testCount110: TestCase = TestCase(
    """SELECT COUNT(col_timestamptz_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMPTZ_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount111: TestCase = TestCase(
    """SELECT COUNT(col_timestamptz_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMPTZ_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount112: TestCase = TestCase(
    """SELECT COUNT(col_timestamptz_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMPTZ_DELTA" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount113: TestCase = TestCase(
    """SELECT COUNT(col_timestamptz_delta32k) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMPTZ_DELTA32K" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount114: TestCase = TestCase(
    """SELECT COUNT(col_timestamptz_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMPTZ_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount115: TestCase = TestCase(
    """SELECT COUNT(col_timestamptz_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMPTZ_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testCount116: TestCase = TestCase(
    """SELECT COUNT(col_timestamptz_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(5000)), // expectedResult
    s"""SELECT ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMPTZ_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  // This test can only pass in default format, as timestamptz column cannot be handled correctly.
  val testCount117: TestCase = TestCase(
    """SELECT col_timestamptz_zstd, COUNT(col_timestamptz_zstd) FROM test_table
      | group by col_timestamptz_zstd
      | order by col_timestamptz_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.from(
      ZonedDateTime.parse("1970-01-01 07:08:06 UTC", formatter).toInstant), 1),
      Row(Timestamp.from(
        ZonedDateTime.parse("1970-01-01 17:04:44 UTC", formatter).toInstant), 1),
      Row(Timestamp.from(
        ZonedDateTime.parse("1970-01-02 23:12:59 UTC", formatter).toInstant), 1),
      Row(Timestamp.from(
        ZonedDateTime.parse("1970-01-07 04:12:15 UTC", formatter).toInstant), 1),
      Row(Timestamp.from(
        ZonedDateTime.parse("1970-01-15 05:54:50 UTC", formatter).toInstant), 1)),
    // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_0",
       | ( COUNT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_TIMESTAMPTZ_ZSTD" ) AS "SUBQUERY_1_COL_0" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS"SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )
}