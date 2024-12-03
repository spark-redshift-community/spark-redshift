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

trait AggregateMaxCorrectnessSuite extends IntegrationPushdownSuiteBase {
  val testMax00: TestCase = TestCase(
    """SELECT MAX(col_smallint_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(200)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_SMALLINT_RAW" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax01: TestCase = TestCase(
    """SELECT MAX(col_smallint_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(200)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_SMALLINT_BYTEDICT" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax02: TestCase = TestCase(
    """SELECT MAX(col_smallint_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(200)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_SMALLINT_DELTA" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax03: TestCase = TestCase(
    """SELECT MAX(col_smallint_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(200)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_SMALLINT_LZO" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax04: TestCase = TestCase(
    """SELECT MAX(col_smallint_mostly8) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(200)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_SMALLINT_MOSTLY8" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax05: TestCase = TestCase(
    """SELECT MAX(col_smallint_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(200)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_SMALLINT_RUNLENGTH" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax06: TestCase = TestCase(
    """SELECT MAX(col_smallint_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(200)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_SMALLINT_ZSTD" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax07: TestCase = TestCase(
    """SELECT col_smallint_zstd, MAX(col_smallint_raw) FROM test_table
      | group by col_smallint_zstd
      | order by col_smallint_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-200, 189),
      Row(-199, 156),
      Row(-198, 197),
      Row(-197, 187),
      Row(-196, 185)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SQ_1"."SQ_1_COL_1" ) AS "SQ_2_COL_0",
       | ( MAX ( "SQ_1"."SQ_1_COL_0") ) AS "SQ_2_COL_1" FROM (
       | SELECT ( "SQ_0"."COL_SMALLINT_RAW" ) AS "SQ_1_COL_0",
       | ( "SQ_0"."COL_SMALLINT_ZSTD" ) AS "SQ_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" ) AS "SQ_1"
       | GROUP BY "SQ_1"."SQ_1_COL_1" ) AS "SQ_2"
       | ORDER BY ( "SQ_2"."SQ_2_COL_0" ) ASC NULLS FIRST ) AS "SQ_3"
       | ORDER BY ( "SQ_3"."SQ_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testMax10: TestCase = TestCase(
    """SELECT MAX(col_int_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(500)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_INT_RAW" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax11: TestCase = TestCase(
    """SELECT MAX(col_int_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(500)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_INT_BYTEDICT" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax12: TestCase = TestCase(
    """SELECT MAX(col_int_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(500)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_INT_DELTA" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax13: TestCase = TestCase(
    """SELECT MAX(col_int_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(500)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_INT_LZO" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax14: TestCase = TestCase(
    """SELECT MAX(col_int_mostly8) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(500)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_INT_MOSTLY8" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax15: TestCase = TestCase(
    """SELECT MAX(col_int_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(500)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_INT_RUNLENGTH" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax16: TestCase = TestCase(
    """SELECT MAX(col_int_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(500)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_INT_ZSTD" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax17: TestCase = TestCase(
    """SELECT col_int_zstd, MAX(col_int_raw) FROM test_table
      | group by col_int_zstd
      | order by col_int_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-500, 481),
      Row(-499, 31),
      Row(-498, 461),
      Row(-497, 471),
      Row(-496, 464)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SQ_1"."SQ_1_COL_1" ) AS "SQ_2_COL_0",
       | ( MAX ( "SQ_1"."SQ_1_COL_0") ) AS "SQ_2_COL_1" FROM (
       | SELECT ( "SQ_0"."COL_INT_RAW" ) AS "SQ_1_COL_0",
       | ( "SQ_0"."COL_INT_ZSTD" ) AS "SQ_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" ) AS "SQ_1"
       | GROUP BY "SQ_1"."SQ_1_COL_1" ) AS "SQ_2"
       | ORDER BY ( "SQ_2"."SQ_2_COL_0" ) ASC NULLS FIRST ) AS "SQ_3"
       | ORDER BY ( "SQ_3"."SQ_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testMax20: TestCase = TestCase(
    """SELECT MAX(col_bigint_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(9995)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_BIGINT_RAW" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax21: TestCase = TestCase(
    """SELECT MAX(col_bigint_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(10000)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_BIGINT_BYTEDICT" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax22: TestCase = TestCase(
    """SELECT MAX(col_bigint_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(9998)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_BIGINT_DELTA" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax23: TestCase = TestCase(
    """SELECT MAX(col_bigint_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(9999)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_BIGINT_LZO" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax24: TestCase = TestCase(
    """SELECT MAX(col_bigint_mostly8) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(9998)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_BIGINT_MOSTLY8" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax25: TestCase = TestCase(
    """SELECT MAX(col_bigint_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(10000)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_BIGINT_RUNLENGTH" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax26: TestCase = TestCase(
    """SELECT MAX(col_bigint_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(9997)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_BIGINT_ZSTD" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax27: TestCase = TestCase(
    """SELECT col_bigint_zstd, MAX(col_bigint_raw) FROM test_table
      | group by col_bigint_zstd
      | order by col_bigint_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-9987, 7747),
      Row(-9979, -3749),
      Row(-9970, -7372),
      Row(-9965, 2510),
      Row(-9951, 5378)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SQ_1"."SQ_1_COL_1" ) AS "SQ_2_COL_0",
       | ( MAX ( "SQ_1"."SQ_1_COL_0") ) AS "SQ_2_COL_1" FROM (
       | SELECT ( "SQ_0"."COL_BIGINT_RAW" ) AS "SQ_1_COL_0",
       | ( "SQ_0"."COL_BIGINT_ZSTD" ) AS "SQ_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" ) AS "SQ_1"
       | GROUP BY "SQ_1"."SQ_1_COL_1" ) AS "SQ_2"
       | ORDER BY ( "SQ_2"."SQ_2_COL_0" ) ASC NULLS FIRST ) AS "SQ_3"
       | ORDER BY ( "SQ_3"."SQ_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testMax30: TestCase = TestCase(
    """SELECT MAX(col_decimal_1_0_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(9)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_DECIMAL_1_0_RAW" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax31: TestCase = TestCase(
    """SELECT MAX(col_decimal_18_0_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(99962)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_DECIMAL_18_0_BYTEDICT" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax32: TestCase = TestCase(
    """SELECT MAX(col_decimal_18_18_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(0.99981)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_DECIMAL_18_18_DELTA" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax33: TestCase = TestCase(
    """SELECT MAX(col_decimal_38_0_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(99975731)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_DECIMAL_38_0_LZO" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax34: TestCase = TestCase(
    """SELECT MAX(col_decimal_38_37_mostly8) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(0.99976899)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_DECIMAL_38_37_MOSTLY8" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax35: TestCase = TestCase(
    """SELECT MAX(col_decimal_38_37_mostly16) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(0.9999629)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_DECIMAL_38_37_MOSTLY16" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax36: TestCase = TestCase(
    """SELECT MAX(col_decimal_38_37_mostly32) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(0.99977416)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_DECIMAL_38_37_MOSTLY32" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax37: TestCase = TestCase(
    """SELECT MAX(col_decimal_18_0_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(99999)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_DECIMAL_18_0_RUNLENGTH" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax38: TestCase = TestCase(
    """SELECT MAX(col_decimal_18_18_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(0.9999)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_DECIMAL_18_18_ZSTD" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax39: TestCase = TestCase(
    """SELECT col_decimal_18_18_zstd, MAX(col_decimal_18_18_raw) FROM test_table
      | group by col_decimal_18_18_zstd
      | order by col_decimal_18_18_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(0.10022, 0.1124),
      Row(0.1006, 0.33246),
      Row(0.10076, 0.97193),
      Row(0.10112, 0.699),
      Row(0.10175, 0.54476)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SQ_1"."SQ_1_COL_1" ) AS "SQ_2_COL_0",
       | ( MAX ( "SQ_1"."SQ_1_COL_0") ) AS "SQ_2_COL_1" FROM (
       | SELECT ( "SQ_0"."COL_DECIMAL_18_18_RAW" ) AS "SQ_1_COL_0",
       | ( "SQ_0"."COL_DECIMAL_18_18_ZSTD" ) AS "SQ_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" ) AS "SQ_1"
       | GROUP BY "SQ_1"."SQ_1_COL_1" ) AS "SQ_2"
       | ORDER BY ( "SQ_2"."SQ_2_COL_0" ) ASC NULLS FIRST ) AS "SQ_3"
       | ORDER BY ( "SQ_3"."SQ_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testMax40: TestCase = TestCase(
    """SELECT MAX(col_float4_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(39.99.toFloat)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_FLOAT4_RAW" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax41: TestCase = TestCase(
    """SELECT MAX(col_float4_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(39.995865.toFloat)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_FLOAT4_BYTEDICT" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax42: TestCase = TestCase(
    """SELECT MAX(col_float4_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(39.998566.toFloat)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_FLOAT4_RUNLENGTH" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax43: TestCase = TestCase(
    """SELECT MAX(col_float4_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(39.99679.toFloat)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_FLOAT4_ZSTD" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax44: TestCase = TestCase(
    """SELECT MAX(col_float8_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(59.96688070413485)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_FLOAT8_RAW" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax45: TestCase = TestCase(
    """SELECT MAX(col_float8_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(59.996839690010944)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_FLOAT8_BYTEDICT" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax46: TestCase = TestCase(
    """SELECT MAX(col_float8_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(59.910869577021174)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_FLOAT8_RUNLENGTH" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax47: TestCase = TestCase(
    """SELECT MAX(col_float8_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(59.9918639574595)), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_FLOAT8_ZSTD" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax48: TestCase = TestCase(
    """SELECT col_float4_zstd, MAX(col_float4_raw) FROM test_table
      | group by col_float4_zstd
      | order by col_float4_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-39.99878.toFloat, -34.339485.toFloat),
      Row(-39.968693.toFloat, -27.364058.toFloat),
      Row(-39.94803.toFloat, 3.893314.toFloat),
      Row(-39.920315.toFloat, -18.851852.toFloat),
      Row(-39.919968.toFloat, -15.938475.toFloat)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SQ_1"."SQ_1_COL_1" ) AS "SQ_2_COL_0",
       | ( MAX ( "SQ_1"."SQ_1_COL_0") ) AS "SQ_2_COL_1" FROM (
       | SELECT ( "SQ_0"."COL_FLOAT4_RAW" ) AS "SQ_1_COL_0",
       | ( "SQ_0"."COL_FLOAT4_ZSTD" ) AS "SQ_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" ) AS "SQ_1"
       | GROUP BY "SQ_1"."SQ_1_COL_1" ) AS "SQ_2"
       | ORDER BY ( "SQ_2"."SQ_2_COL_0" ) ASC NULLS FIRST ) AS "SQ_3"
       | ORDER BY ( "SQ_3"."SQ_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testMax49: TestCase = TestCase(
    """SELECT col_float8_zstd, MAX(col_float8_raw) FROM test_table
      | group by col_float8_zstd
      | order by col_float8_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-59.98139065242519, -38.50286940512086),
      Row(-59.96652420830154, 29.95038540500704),
      Row(-59.93905605936271, 50.49272294340629),
      Row(-59.90738346844318, -53.58293774248173),
      Row(-59.89910788903543, 37.013841179953545)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SQ_1"."SQ_1_COL_1" ) AS "SQ_2_COL_0",
       | ( MAX ( "SQ_1"."SQ_1_COL_0") ) AS "SQ_2_COL_1" FROM (
       | SELECT ( "SQ_0"."COL_FLOAT8_RAW" ) AS "SQ_1_COL_0",
       | ( "SQ_0"."COL_FLOAT8_ZSTD" ) AS "SQ_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" ) AS "SQ_1"
       | GROUP BY "SQ_1"."SQ_1_COL_1" ) AS "SQ_2"
       | ORDER BY ( "SQ_2"."SQ_2_COL_0" ) ASC NULLS FIRST ) AS "SQ_3"
       | ORDER BY ( "SQ_3"."SQ_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  // function max(boolean) does not exist in redshift
  val testMax50_unsupported: TestCase = TestCase(
    """SELECT MAX(col_boolean_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(true)), // expectedResult
    s"""SELECT ( "SQ_0"."COL_BOOLEAN_RAW" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0"""".stripMargin // expectedPushdownStatement
  )

  val testMax51_unsupported: TestCase = TestCase(
    """SELECT MAX(col_boolean_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(true)), // expectedResult
    s"""SELECT ( "SQ_0"."COL_BOOLEAN_RUNLENGTH" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0"""".stripMargin // expectedPushdownStatement
  )

  val testMax52_unsupported: TestCase = TestCase(
    """SELECT MAX(col_boolean_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(true)), // expectedResult
    s"""SELECT ( "SQ_0"."COL_BOOLEAN_ZSTD" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0"""".stripMargin // expectedPushdownStatement
  )

  val testMax53: TestCase = TestCase(
    """SELECT col_boolean_zstd, MAX(col_int_zstd) FROM test_table
      | group by col_boolean_zstd
      | order by col_boolean_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(false, 500), Row(true, 500)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SQ_1"."SQ_1_COL_1" ) AS "SQ_2_COL_0",
       | ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_1" FROM (
       | SELECT ( "SQ_0"."COL_INT_ZSTD" ) AS "SQ_1_COL_0",
       | ( "SQ_0"."COL_BOOLEAN_ZSTD" ) AS "SQ_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0") AS "SQ_1"
       | GROUP BY "SQ_1"."SQ_1_COL_1" ) AS "SQ_2"
       | ORDER BY ( "SQ_2"."SQ_2_COL_0" ) ASC NULLS FIRST ) AS "SQ_3"
       | ORDER BY ( "SQ_3"."SQ_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testMax60: TestCase = TestCase(
    """SELECT MAX(col_char_1_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row("Y")), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_CHAR_1_RAW" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax61: TestCase = TestCase(
    """SELECT MAX(col_char_255_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row("Yourself.")), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_CHAR_255_LZO" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax62: TestCase = TestCase(
    """SELECT MAX(col_char_2000_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row("Yourself.")), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_CHAR_2000_RUNLENGTH" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax63: TestCase = TestCase(
    """SELECT MAX(col_char_max_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row("Yourself image eight bad.")), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_CHAR_MAX_ZSTD" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax64: TestCase = TestCase(
    """SELECT col_char_max_zstd, MAX(col_char_255_lzo) FROM test_table
      | group by col_char_max_zstd
      | order by col_char_max_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row("A central require.", "Bill bad risk."),
      Row("A compare stuff report trial place.", "Half turn simple."),
      Row("A from nearly she certain maybe choose.", "Adult admit less."),
      Row("A significant Democrat through.", "Oil for blue happen."),
      Row("Ability gas sport win.", "Tough audience land.")), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SQ_1"."SQ_1_COL_1" ) AS "SQ_2_COL_0",
       | ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_1" FROM (
       | SELECT ( "SQ_0"."COL_CHAR_255_LZO" ) AS "SQ_1_COL_0",
       | ( "SQ_0"."COL_CHAR_MAX_ZSTD" ) AS "SQ_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0") AS "SQ_1"
       | GROUP BY "SQ_1"."SQ_1_COL_1" ) AS "SQ_2"
       | ORDER BY ( "SQ_2"."SQ_2_COL_0" ) ASC NULLS FIRST ) AS "SQ_3"
       | ORDER BY ( "SQ_3"."SQ_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testMax70: TestCase = TestCase(
    """SELECT MAX(col_varchar_1_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row("Y")), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_VARCHAR_1_RAW" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax71: TestCase = TestCase(
    """SELECT MAX(col_varchar_255_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row("Yourself world.")), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_VARCHAR_255_LZO" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax72: TestCase = TestCase(
    """SELECT MAX(col_varchar_2000_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row("Yourself what.")), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_VARCHAR_2000_RUNLENGTH" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax73: TestCase = TestCase(
    """SELECT MAX(col_varchar_max_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row("Yourself true foreign reason type.")), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_VARCHAR_MAX_ZSTD" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax74: TestCase = TestCase(
    """SELECT col_varchar_max_zstd, MAX(col_varchar_255_lzo) FROM test_table
      | group by col_varchar_max_zstd
      | order by col_varchar_max_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row("A arm ready mean.", "Result financial."),
      Row("A become nation gas organization door.", "Structure mouth."),
      Row("A discussion forward debate.", "Ball draw sing."),
      Row("A rest new apply.", "Within that become."),
      Row("A science whom seven and.", "Approach right set.")), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SQ_1"."SQ_1_COL_1" ) AS "SQ_2_COL_0",
       | ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_1" FROM (
       | SELECT ( "SQ_0"."COL_VARCHAR_255_LZO" ) AS "SQ_1_COL_0",
       | ( "SQ_0"."COL_VARCHAR_MAX_ZSTD" ) AS "SQ_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0") AS "SQ_1"
       | GROUP BY "SQ_1"."SQ_1_COL_1" ) AS "SQ_2"
       | ORDER BY ( "SQ_2"."SQ_2_COL_0" ) ASC NULLS FIRST ) AS "SQ_3"
       | ORDER BY ( "SQ_3"."SQ_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testMax80: TestCase = TestCase(
    """SELECT MAX(col_date_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Date.valueOf("2018-10-13"))), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_DATE_RAW" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax81: TestCase = TestCase(
    """SELECT MAX(col_date_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Date.valueOf("2018-10-04"))), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_DATE_BYTEDICT" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax82: TestCase = TestCase(
    """SELECT MAX(col_date_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Date.valueOf("2018-10-13"))), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_DATE_DELTA" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax83: TestCase = TestCase(
    """SELECT MAX(col_date_delta32k) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Date.valueOf("2018-10-16"))), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_DATE_DELTA32K" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax84: TestCase = TestCase(
    """SELECT MAX(col_date_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Date.valueOf("2018-10-12"))), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_DATE_LZO" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax85: TestCase = TestCase(
    """SELECT MAX(col_date_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Date.valueOf("2018-10-10"))), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_DATE_RUNLENGTH" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax86: TestCase = TestCase(
    """SELECT MAX(col_date_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Date.valueOf("2018-10-16"))), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_DATE_ZSTD" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax87: TestCase = TestCase(
    """SELECT col_date_zstd, MAX(col_date_lzo) FROM test_table
      | group by col_date_zstd
      | order by col_date_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(Date.valueOf("1970-01-02"), Date.valueOf("2016-07-24")),
      Row(Date.valueOf("1970-01-05"), Date.valueOf("1974-06-29")),
      Row(Date.valueOf("1970-01-11"), Date.valueOf("1997-01-12")),
      Row(Date.valueOf("1970-01-15"), Date.valueOf("2010-09-19")),
      Row(Date.valueOf("1970-01-19"), Date.valueOf("2004-03-17"))), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SQ_1"."SQ_1_COL_1" ) AS "SQ_2_COL_0",
       | ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_1" FROM (
       | SELECT ( "SQ_0"."COL_DATE_LZO" ) AS "SQ_1_COL_0",
       | ( "SQ_0"."COL_DATE_ZSTD" ) AS "SQ_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0") AS "SQ_1"
       | GROUP BY "SQ_1"."SQ_1_COL_1" ) AS "SQ_2"
       | ORDER BY ( "SQ_2"."SQ_2_COL_0" ) ASC NULLS FIRST ) AS "SQ_3"
       | ORDER BY ( "SQ_3"."SQ_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testMax90: TestCase = TestCase(
    """SELECT MAX(col_timestamp_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.valueOf("2018-10-16 12:59:32"))), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_TIMESTAMP_RAW" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax91: TestCase = TestCase(
    """SELECT MAX(col_timestamp_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.valueOf("2018-10-14 17:20:47"))), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_TIMESTAMP_BYTEDICT" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax92: TestCase = TestCase(
    """SELECT MAX(col_timestamp_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.valueOf("2018-10-13 11:16:38"))), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_TIMESTAMP_DELTA" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax93: TestCase = TestCase(
    """SELECT MAX(col_timestamp_delta32k) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.valueOf("2018-10-06 12:20:31"))), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_TIMESTAMP_DELTA32K" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax94: TestCase = TestCase(
    """SELECT MAX(col_timestamp_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.valueOf("2018-10-07 07:04:25"))), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_TIMESTAMP_LZO" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax95: TestCase = TestCase(
    """SELECT MAX(col_timestamp_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.valueOf("2018-10-14 04:35:37"))), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_TIMESTAMP_RUNLENGTH" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax96: TestCase = TestCase(
    """SELECT MAX(col_timestamp_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.valueOf("2018-10-16 10:50:57"))), // expectedResult
    s"""SELECT ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT ( "SQ_0"."COL_TIMESTAMP_ZSTD" ) AS "SQ_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
       | AS "SQ_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax97: TestCase = TestCase(
    """SELECT col_timestamp_zstd, MAX(col_timestamp_lzo) FROM test_table
      | group by col_timestamp_zstd
      | order by col_timestamp_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.valueOf("1970-01-01 06:36:17.0"), Timestamp.valueOf("2012-03-06 07:02:38.0")),
      Row(Timestamp.valueOf("1970-01-02 01:29:50.0"), Timestamp.valueOf("2016-01-12 12:58:13.0")),
      Row(Timestamp.valueOf("1970-01-03 14:49:06.0"), Timestamp.valueOf("1971-07-05 14:41:20.0")),
      Row(Timestamp.valueOf("1970-01-05 14:43:54.0"), Timestamp.valueOf("1995-07-09 13:28:00.0")),
      Row(Timestamp.valueOf("1970-01-05 20:52:40.0"), Timestamp.valueOf("2002-10-17 23:10:20.0"))),
    // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SQ_1"."SQ_1_COL_1" ) AS "SQ_2_COL_0",
       | ( MAX ( "SQ_1"."SQ_1_COL_0" ) ) AS "SQ_2_COL_1" FROM (
       | SELECT ( "SQ_0"."COL_TIMESTAMP_LZO" ) AS "SQ_1_COL_0",
       | ( "SQ_0"."COL_TIMESTAMP_ZSTD" ) AS "SQ_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0") AS "SQ_1"
       | GROUP BY "SQ_1"."SQ_1_COL_1" ) AS "SQ_2"
       | ORDER BY ( "SQ_2"."SQ_2_COL_0" ) ASC NULLS FIRST ) AS "SQ_3"
       | ORDER BY ( "SQ_3"."SQ_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )
}