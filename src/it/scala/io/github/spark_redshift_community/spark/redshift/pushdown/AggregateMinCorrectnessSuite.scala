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

trait AggregateMinCorrectnessSuite extends IntegrationPushdownSuiteBase {
  val testMin00: TestCase = TestCase(
    """SELECT MIN(col_smallint_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-200)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin01: TestCase = TestCase(
    """SELECT MIN(col_smallint_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-200)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin02: TestCase = TestCase(
    """SELECT MIN(col_smallint_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-200)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_DELTA" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin03: TestCase = TestCase(
    """SELECT MIN(col_smallint_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-200)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin04: TestCase = TestCase(
    """SELECT MIN(col_smallint_mostly8) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-200)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_MOSTLY8" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin05: TestCase = TestCase(
    """SELECT MIN(col_smallint_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-200)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin06: TestCase = TestCase(
    """SELECT MIN(col_smallint_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-200)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin07: TestCase = TestCase(
    """SELECT col_smallint_zstd, MIN(col_smallint_raw) FROM test_table
      | group by col_smallint_zstd
      | order by col_smallint_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-200, -177),
      Row(-199, -2),
      Row(-198, -157),
      Row(-197, -150),
      Row(-196, -166)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0") ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_SMALLINT_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_SMALLINT_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testMin10: TestCase = TestCase(
    """SELECT MIN(col_int_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-500)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin11: TestCase = TestCase(
    """SELECT MIN(col_int_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-500)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin12: TestCase = TestCase(
    """SELECT MIN(col_int_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-500)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_DELTA" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin13: TestCase = TestCase(
    """SELECT MIN(col_int_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-500)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin14: TestCase = TestCase(
    """SELECT MIN(col_int_mostly8) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-500)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_MOSTLY8" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin15: TestCase = TestCase(
    """SELECT MIN(col_int_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-500)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin16: TestCase = TestCase(
    """SELECT MIN(col_int_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-500)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin17: TestCase = TestCase(
    """SELECT col_int_zstd, MIN(col_int_raw) FROM test_table
      | group by col_int_zstd
      | order by col_int_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-500, -284),
      Row(-499, 31),
      Row(-498, -399),
      Row(-497, -473),
      Row(-496, 464)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0") ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_INT_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_INT_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testMin20: TestCase = TestCase(
    """SELECT MIN(col_bigint_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-9993)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BIGINT_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin21: TestCase = TestCase(
    """SELECT MIN(col_bigint_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-9994)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BIGINT_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin22: TestCase = TestCase(
    """SELECT MIN(col_bigint_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-10000)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BIGINT_DELTA" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin23: TestCase = TestCase(
    """SELECT MIN(col_bigint_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-9995)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BIGINT_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin24: TestCase = TestCase(
    """SELECT MIN(col_bigint_mostly8) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-9994)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BIGINT_MOSTLY8" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin25: TestCase = TestCase(
    """SELECT MIN(col_bigint_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-9996)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BIGINT_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin26: TestCase = TestCase(
    """SELECT MIN(col_bigint_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-9987)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BIGINT_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin27: TestCase = TestCase(
    """SELECT col_bigint_zstd, MIN(col_bigint_raw) FROM test_table
      | group by col_bigint_zstd
      | order by col_bigint_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-9987, 7747),
      Row(-9979, -3749),
      Row(-9970, -7372),
      Row(-9965, 2510),
      Row(-9951, 5378)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0") ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_BIGINT_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BIGINT_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testMin30: TestCase = TestCase(
    """SELECT MIN(col_decimal_1_0_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-9)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_1_0_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin31: TestCase = TestCase(
    """SELECT MIN(col_decimal_18_0_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-99974)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_18_0_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin32: TestCase = TestCase(
    """SELECT MIN(col_decimal_18_18_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(0.10006)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_18_18_DELTA" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin33: TestCase = TestCase(
    """SELECT MIN(col_decimal_38_0_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-99969669)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_38_0_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin34: TestCase = TestCase(
    """SELECT MIN(col_decimal_38_37_mostly8) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(0.10002635)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_38_37_MOSTLY8" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin35: TestCase = TestCase(
    """SELECT MIN(col_decimal_38_37_mostly16) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(0.1000715)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_38_37_MOSTLY16" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin36: TestCase = TestCase(
    """SELECT MIN(col_decimal_38_37_mostly32) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(0.100037)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_38_37_MOSTLY32" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin37: TestCase = TestCase(
    """SELECT MIN(col_decimal_18_0_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-99959)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_18_0_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin38: TestCase = TestCase(
    """SELECT MIN(col_decimal_18_18_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(0.10022)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_18_18_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin39: TestCase = TestCase(
    """SELECT col_decimal_18_18_zstd, MIN(col_decimal_18_18_raw) FROM test_table
      | group by col_decimal_18_18_zstd
      | order by col_decimal_18_18_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(0.10022, 0.1124),
      Row(0.1006, 0.33246),
      Row(0.10076, 0.97193),
      Row(0.10112, 0.699),
      Row(0.10175, 0.54476)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0") ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_DECIMAL_18_18_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_DECIMAL_18_18_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testMin40: TestCase = TestCase(
    """SELECT MIN(col_float4_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-39.9913.toFloat)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT4_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin41: TestCase = TestCase(
    """SELECT MIN(col_float4_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-39.981083.toFloat)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT4_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin42: TestCase = TestCase(
    """SELECT MIN(col_float4_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-39.99827.toFloat)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT4_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin43: TestCase = TestCase(
    """SELECT MIN(col_float4_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-39.99878.toFloat)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT4_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin44: TestCase = TestCase(
    """SELECT MIN(col_float8_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-59.98780168437003)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT8_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin45: TestCase = TestCase(
    """SELECT MIN(col_float8_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-59.96662513909352)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT8_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin46: TestCase = TestCase(
    """SELECT MIN(col_float8_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-59.91385721198996)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT8_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin47: TestCase = TestCase(
    """SELECT MIN(col_float8_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-59.98139065242519)), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT8_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin48: TestCase = TestCase(
    """SELECT col_float4_zstd, MIN(col_float4_raw) FROM test_table
      | group by col_float4_zstd
      | order by col_float4_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-39.99878.toFloat, -34.339485.toFloat),
      Row(-39.968693.toFloat, -27.364058.toFloat),
      Row(-39.94803.toFloat, 3.893314.toFloat),
      Row(-39.920315.toFloat, -18.851852.toFloat),
      Row(-39.919968.toFloat, -15.938475.toFloat)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0") ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_FLOAT4_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_FLOAT4_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testMin49: TestCase = TestCase(
    """SELECT col_float8_zstd, MIN(col_float8_raw) FROM test_table
      | group by col_float8_zstd
      | order by col_float8_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-59.98139065242519, -38.50286940512086),
      Row(-59.96652420830154, 29.95038540500704),
      Row(-59.93905605936271, 50.49272294340629),
      Row(-59.90738346844318, -53.58293774248173),
      Row(-59.89910788903543, 37.013841179953545)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0") ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_FLOAT8_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_FLOAT8_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  // function min(boolean) does not exist in redshift
  val testMin50_unsupported: TestCase = TestCase(
    """SELECT MIN(col_boolean_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(false)), // expectedResult
    s"""SELECT ( "SUBQUERY_0"."COL_BOOLEAN_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0"""".stripMargin // expectedPushdownStatement
  )

  val testMin51_unsupported: TestCase = TestCase(
    """SELECT MIN(col_boolean_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(false)), // expectedResult
    s"""SELECT ( "SUBQUERY_0"."COL_BOOLEAN_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0"""".stripMargin // expectedPushdownStatement
  )

  val testMin52_unsupported: TestCase = TestCase(
    """SELECT MIN(col_boolean_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(false)), // expectedResult
    s"""SELECT ( "SUBQUERY_0"."COL_BOOLEAN_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0"""".stripMargin // expectedPushdownStatement
  )

  val testMin53: TestCase = TestCase(
    """SELECT col_boolean_zstd, MIN(col_int_zstd) FROM test_table
      | group by col_boolean_zstd
      | order by col_boolean_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(false, -500), Row(true, -500)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_INT_ZSTD" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testMin60: TestCase = TestCase(
    """SELECT MIN(col_char_1_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row("A")), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_CHAR_1_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin61: TestCase = TestCase(
    """SELECT MIN(col_char_255_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row("A probably.")), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_CHAR_255_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin62: TestCase = TestCase(
    """SELECT MIN(col_char_2000_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row("A beautiful much.")), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_CHAR_2000_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin63: TestCase = TestCase(
    """SELECT MIN(col_char_max_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row("A central require.")), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_CHAR_MAX_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin64: TestCase = TestCase(
    """SELECT col_char_max_zstd, MIN(col_char_255_lzo) FROM test_table
      | group by col_char_max_zstd
      | order by col_char_max_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row("A central require.", "Bill bad risk."),
      Row("A compare stuff report trial place.", "Half turn simple."),
      Row("A from nearly she certain maybe choose.", "Adult admit less."),
      Row("A significant Democrat through.", "Oil for blue happen."),
      Row("Ability gas sport win.", "Tough audience land.")), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_CHAR_255_LZO" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_CHAR_MAX_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testMin70: TestCase = TestCase(
    """SELECT MIN(col_varchar_1_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row("A")), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_VARCHAR_1_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin71: TestCase = TestCase(
    """SELECT MIN(col_varchar_255_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row("A another act.")), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_VARCHAR_255_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin72: TestCase = TestCase(
    """SELECT MIN(col_varchar_2000_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row("A day over manage.")), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_VARCHAR_2000_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin73: TestCase = TestCase(
    """SELECT MIN(col_varchar_max_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row("A arm ready mean.")), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_VARCHAR_MAX_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin74: TestCase = TestCase(
    """SELECT col_varchar_max_zstd, MIN(col_varchar_255_lzo) FROM test_table
      | group by col_varchar_max_zstd
      | order by col_varchar_max_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row("A arm ready mean.", "Result financial."),
      Row("A become nation gas organization door.", "Structure mouth."),
      Row("A discussion forward debate.", "Ball draw sing."),
      Row("A rest new apply.", "Within that become."),
      Row("A science whom seven and.", "Approach right set.")), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_VARCHAR_255_LZO" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_VARCHAR_MAX_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testMin80: TestCase = TestCase(
    """SELECT MIN(col_date_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Date.valueOf("1970-01-03"))), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DATE_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin81: TestCase = TestCase(
    """SELECT MIN(col_date_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Date.valueOf("1970-01-07"))), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DATE_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin82: TestCase = TestCase(
    """SELECT MIN(col_date_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Date.valueOf("1970-01-06"))), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DATE_DELTA" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin83: TestCase = TestCase(
    """SELECT MIN(col_date_delta32k) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Date.valueOf("1970-01-10"))), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DATE_DELTA32K" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin84: TestCase = TestCase(
    """SELECT MIN(col_date_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Date.valueOf("1970-01-02"))), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DATE_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin85: TestCase = TestCase(
    """SELECT MIN(col_date_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Date.valueOf("1970-01-01"))), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DATE_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin86: TestCase = TestCase(
    """SELECT MIN(col_date_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Date.valueOf("1970-01-02"))), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DATE_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin87: TestCase = TestCase(
    """SELECT col_date_zstd, MIN(col_date_lzo) FROM test_table
      | group by col_date_zstd
      | order by col_date_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(Date.valueOf("1970-01-02"), Date.valueOf("2016-07-24")),
      Row(Date.valueOf("1970-01-05"), Date.valueOf("1974-06-29")),
      Row(Date.valueOf("1970-01-11"), Date.valueOf("1997-01-12")),
      Row(Date.valueOf("1970-01-15"), Date.valueOf("2010-09-19")),
      Row(Date.valueOf("1970-01-19"), Date.valueOf("2004-03-17"))), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_DATE_LZO" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_DATE_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testMin90: TestCase = TestCase(
    """SELECT MIN(col_timestamp_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.valueOf("1970-01-03 22:34:25.0"))), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMP_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin91: TestCase = TestCase(
    """SELECT MIN(col_timestamp_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.valueOf("1970-01-07 04:00:48.0"))), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMP_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin92: TestCase = TestCase(
    """SELECT MIN(col_timestamp_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.valueOf("1970-01-04 18:35:46.0"))), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMP_DELTA" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin93: TestCase = TestCase(
    """SELECT MIN(col_timestamp_delta32k) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.valueOf("1970-01-03 20:55:29.0"))), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMP_DELTA32K" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin94: TestCase = TestCase(
    """SELECT MIN(col_timestamp_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.valueOf("1970-01-01 02:05:23.0"))), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMP_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin95: TestCase = TestCase(
    """SELECT MIN(col_timestamp_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.valueOf("1970-01-10 02:35:20.0"))), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMP_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin96: TestCase = TestCase(
    """SELECT MIN(col_timestamp_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.valueOf("1970-01-01 06:36:17.0"))), // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMP_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin97: TestCase = TestCase(
    """SELECT col_timestamp_zstd, MIN(col_timestamp_lzo) FROM test_table
      | group by col_timestamp_zstd
      | order by col_timestamp_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.valueOf("1970-01-01 06:36:17.0"), Timestamp.valueOf("2012-03-06 07:02:38.0")),
      Row(Timestamp.valueOf("1970-01-02 01:29:50.0"), Timestamp.valueOf("2016-01-12 12:58:13.0")),
      Row(Timestamp.valueOf("1970-01-03 14:49:06.0"), Timestamp.valueOf("1971-07-05 14:41:20.0")),
      Row(Timestamp.valueOf("1970-01-05 14:43:54.0"), Timestamp.valueOf("1995-07-09 13:28:00.0")),
      Row(Timestamp.valueOf("1970-01-05 20:52:40.0"), Timestamp.valueOf("2002-10-17 23:10:20.0"))),
    // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_TIMESTAMP_LZO" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_TIMESTAMP_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testMin100: TestCase = TestCase(
    """SELECT MIN(col_timestamptz_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.from(ZonedDateTime.parse("1970-01-10 09:00:12 UTC", formatter).toInstant))),
    // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMPTZ_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin101: TestCase = TestCase(
    """SELECT MIN(col_timestamptz_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.from(ZonedDateTime.parse("1970-01-08 23:00:15 UTC", formatter).toInstant))),
    // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMPTZ_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin102: TestCase = TestCase(
    """SELECT MIN(col_timestamptz_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.from(ZonedDateTime.parse("1970-01-01 04:50:02 UTC", formatter).toInstant))),
    // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMPTZ_DELTA" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin103: TestCase = TestCase(
    """SELECT MIN(col_timestamptz_delta32k) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.from(ZonedDateTime.parse("1970-01-01 01:40:18 UTC", formatter).toInstant))),
    // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMPTZ_DELTA32K" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin104: TestCase = TestCase(
    """SELECT MIN(col_timestamptz_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.from(ZonedDateTime.parse("1970-01-03 15:24:06 UTC", formatter).toInstant))),
    // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMPTZ_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin105: TestCase = TestCase(
    """SELECT MIN(col_timestamptz_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.from(ZonedDateTime.parse("1970-01-06 11:54:44 UTC", formatter).toInstant))),
    // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMPTZ_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin106: TestCase = TestCase(
    """SELECT MIN(col_timestamptz_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.from(ZonedDateTime.parse("1970-01-01 07:08:06 UTC", formatter).toInstant))),
    // expectedResult
    s"""SELECT ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_TIMESTAMPTZ_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMin107: TestCase = TestCase(
    """SELECT col_timestamptz_zstd, MIN(col_timestamptz_lzo) FROM test_table
      | group by col_timestamptz_zstd
      | order by col_timestamptz_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(Timestamp.from(ZonedDateTime.parse("1970-01-01 07:08:06 UTC", formatter).toInstant),
      Timestamp.from(ZonedDateTime.parse("2001-09-28 19:24:00 UTC", formatter).toInstant)),
      Row(Timestamp.from(ZonedDateTime.parse("1970-01-01 17:04:44 UTC", formatter).toInstant),
        Timestamp.from(ZonedDateTime.parse("1976-01-08 07:53:08 UTC", formatter).toInstant)),
      Row(Timestamp.from(ZonedDateTime.parse("1970-01-02 23:12:59 UTC", formatter).toInstant),
        Timestamp.from(ZonedDateTime.parse("1988-06-22 15:43:27 UTC", formatter).toInstant)),
      Row(Timestamp.from(ZonedDateTime.parse("1970-01-07 04:12:15 UTC", formatter).toInstant),
        Timestamp.from(ZonedDateTime.parse("2006-01-13 00:40:53 UTC", formatter).toInstant)),
      Row(Timestamp.from(ZonedDateTime.parse("1970-01-15 05:54:50 UTC", formatter).toInstant),
        Timestamp.from(ZonedDateTime.parse("2014-05-21 05:54:35 UTC", formatter).toInstant))),
    // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( MIN ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_TIMESTAMPTZ_LZO" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_TIMESTAMPTZ_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )
}
