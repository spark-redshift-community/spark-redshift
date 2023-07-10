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

trait AggregateSumCorrectnessSuite extends IntegrationPushdownSuiteBase {
  val testSum00: TestCase = TestCase(
    """SELECT col_smallint_zstd, SUM(col_smallint_raw) FROM test_table
      | group by col_smallint_zstd
      | order by col_smallint_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-200, 55),
      Row(-199, 720),
      Row(-198, 149),
      Row(-197, 202),
      Row(-196, 358)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_SMALLINT_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_SMALLINT_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testSum01: TestCase = TestCase(
    """SELECT col_boolean_raw, SUM(col_smallint_raw) FROM test_table
      | group by col_boolean_raw
      | order by col_boolean_raw limit 2""".stripMargin, // sparkStatement
    Seq(Row(false, 2467), Row(true, 4217)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_SMALLINT_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_RAW" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT 2""".stripMargin // expectedPushdownStatement
  )

  val testSum02: TestCase = TestCase(
    """SELECT col_boolean_runlength, SUM(col_smallint_runlength) FROM test_table
      | group by col_boolean_runlength
      | order by col_boolean_runlength limit 2""".stripMargin, // sparkStatement
    Seq(Row(false, -10462), Row(true, -8263)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_SMALLINT_RUNLENGTH" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_RUNLENGTH" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT 2""".stripMargin // expectedPushdownStatement
  )

  val testSum03: TestCase = TestCase(
    """SELECT col_boolean_zstd, SUM(col_smallint_zstd) FROM test_table
      | group by col_boolean_zstd
      | order by col_boolean_zstd limit 2""".stripMargin, // sparkStatement
    Seq(Row(false, 4604), Row(true, 3639)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_SMALLINT_ZSTD" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT 2""".stripMargin // expectedPushdownStatement
  )

  val testSum10: TestCase = TestCase(
    """SELECT col_int_zstd, SUM(col_int_raw) FROM test_table
      | group by col_int_zstd
      | order by col_int_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-500, 1484),
      Row(-499, 31),
      Row(-498, -920),
      Row(-497, 35),
      Row(-496, 464)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_INT_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_INT_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )

  val testSum11: TestCase = TestCase(
    """SELECT col_boolean_raw, SUM(col_int_raw) FROM test_table
      | group by col_boolean_raw
      | order by col_boolean_raw limit 2""".stripMargin, // sparkStatement
    Seq(Row(false, 4506), Row(true, 10313)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_INT_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_RAW" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT 2""".stripMargin // expectedPushdownStatement
  )

  val testSum12: TestCase = TestCase(
    """SELECT col_boolean_runlength, SUM(col_int_runlength) FROM test_table
      | group by col_boolean_runlength
      | order by col_boolean_runlength limit 2""".stripMargin, // sparkStatement
    Seq(Row(false, -5336), Row(true, 7842)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_INT_RUNLENGTH" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_RUNLENGTH" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT 2""".stripMargin // expectedPushdownStatement
  )

  val testSum13: TestCase = TestCase(
    """SELECT col_boolean_zstd, SUM(col_int_zstd) FROM test_table
      | group by col_boolean_zstd
      | order by col_boolean_zstd limit 2""".stripMargin, // sparkStatement
    Seq(Row(false, 1676), Row(true, 1651)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_INT_ZSTD" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT 2""".stripMargin // expectedPushdownStatement
  )

  val testSum20: TestCase = TestCase(
    """SELECT col_bigint_zstd, SUM(col_bigint_raw) FROM test_table
      | group by col_bigint_zstd
      | order by col_bigint_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-9987, 7747),
      Row(-9979, -3749),
      Row(-9970, -7372),
      Row(-9965, 2510),
      Row(-9951, 5378)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_BIGINT_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BIGINT_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testSum21: TestCase = TestCase(
    """SELECT col_boolean_raw, SUM(col_bigint_raw) FROM test_table
      | group by col_boolean_raw
      | order by col_boolean_raw limit 2""".stripMargin, // sparkStatement
    Seq(Row(false, 114727), Row(true, 748497)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_BIGINT_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_RAW" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT 2""".stripMargin // expectedPushdownStatement
  )

  val testSum22: TestCase = TestCase(
    """SELECT col_boolean_runlength, SUM(col_bigint_runlength) FROM test_table
      | group by col_boolean_runlength
      | order by col_boolean_runlength limit 2""".stripMargin, // sparkStatement
    Seq(Row(false, -74323), Row(true, 74429)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_BIGINT_RUNLENGTH" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_RUNLENGTH" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT 2""".stripMargin // expectedPushdownStatement
  )

  val testSum23: TestCase = TestCase(
    """SELECT col_boolean_zstd, SUM(col_bigint_zstd) FROM test_table
      | group by col_boolean_zstd
      | order by col_boolean_zstd limit 2""".stripMargin, // sparkStatement
    Seq(Row(false, -45514), Row(true, -93277)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_BIGINT_ZSTD" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT 2""".stripMargin // expectedPushdownStatement
  )

  val testSum30: TestCase = TestCase(
    """SELECT col_decimal_18_0_zstd, SUM(col_decimal_18_0_raw) FROM test_table
      | group by col_decimal_18_0_zstd
      | order by col_decimal_18_0_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-99917, 78608.0),
      Row(-99888, 1099.0),
      Row(-99863, -9247.0),
      Row(-99822, -30101.0),
      Row(-99780, -74476.0)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_DECIMAL_18_0_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_DECIMAL_18_0_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )

  val testSum31: TestCase = TestCase(
    """SELECT col_boolean_raw, SUM(col_decimal_18_0_raw) FROM test_table
      | group by col_boolean_raw
      | order by col_boolean_raw limit 2""".stripMargin, // sparkStatement
    Seq(Row(false, -3404299), Row(true, 4798491)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_DECIMAL_18_0_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_RAW" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT 2""".stripMargin // expectedPushdownStatement
  )

  val testSum32: TestCase = TestCase(
    """SELECT col_boolean_runlength, SUM(col_decimal_18_18_runlength) FROM test_table
      | group by col_boolean_runlength
      | order by col_boolean_runlength limit 2""".stripMargin, // sparkStatement
    Seq(Row(false, 1399.60942), Row(true, 1352.03843)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_DECIMAL_18_18_RUNLENGTH" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_RUNLENGTH" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT 2""".stripMargin // expectedPushdownStatement
  )

  val testSum33: TestCase = TestCase(
    """SELECT col_boolean_zstd, SUM(col_decimal_1_0_zstd) FROM test_table
      | group by col_boolean_zstd
      | order by col_boolean_zstd limit 2""".stripMargin, // sparkStatement
    Seq(Row(false, 57), Row(true, -274)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( CAST ( ( SUM ( ( "SUBQUERY_1"."SUBQUERY_1_COL_0" * POW(10,0) ) ) / POW ( 10, 0 ) )
       | AS DECIMAL(11,0) ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_DECIMAL_1_0_ZSTD" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT 2""".stripMargin // expectedPushdownStatement
  )

  val testSum40: TestCase = TestCase(
    """SELECT col_float4_zstd, SUM(col_float4_raw) FROM test_table
      | group by col_float4_zstd
      | order by col_float4_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-39.99878.toFloat, -34.339485.toFloat),
      Row(-39.968693.toFloat, -27.364058.toFloat),
      Row(-39.94803.toFloat, 3.893314.toFloat),
      Row(-39.920315.toFloat, -18.851852.toFloat),
      Row(-39.919968.toFloat, -15.938475.toFloat)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_FLOAT4_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_FLOAT4_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )

  val testSum41: TestCase = TestCase(
    """SELECT col_float8_zstd, SUM(col_float8_raw) FROM test_table
      | group by col_float8_zstd
      | order by col_float8_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-59.98139065242519, -38.50286940512086),
      Row(-59.96652420830154, 29.95038540500704),
      Row(-59.93905605936271, 50.49272294340629),
      Row(-59.90738346844318, -53.58293774248173),
      Row(-59.89910788903543, 37.013841179953545)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_FLOAT8_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_FLOAT8_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )

  val testSum42: TestCase = TestCase(
    """SELECT col_boolean_raw, SUM(col_float4_raw) FROM test_table
      | group by col_boolean_raw
      | order by col_boolean_raw limit 2""".stripMargin, // sparkStatement
    Seq(Row(false, -416.4173880573362), Row(true, -311.8814963847399)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_FLOAT4_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_RAW" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT 2""".stripMargin // expectedPushdownStatement
  )

  val testSum43: TestCase = TestCase(
    """SELECT col_boolean_runlength, SUM(col_float4_runlength) FROM test_table
      | group by col_boolean_runlength
      | order by col_boolean_runlength limit 2""".stripMargin, // sparkStatement
    Seq(Row(false, -675.9250739216805), Row(true, 853.6241302993149)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_FLOAT4_RUNLENGTH" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_RUNLENGTH" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT 2""".stripMargin // expectedPushdownStatement
  )

  val testSum44: TestCase = TestCase(
    """SELECT col_boolean_zstd, SUM(col_float4_zstd) FROM test_table
      | group by col_boolean_zstd
      | order by col_boolean_zstd limit 2""".stripMargin, // sparkStatement
    Seq(Row(false, 562.8900937438011), Row(true, 689.6534534092061)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_FLOAT4_ZSTD" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT 2""".stripMargin // expectedPushdownStatement
  )

  val testSum45: TestCase = TestCase(
    """SELECT col_boolean_raw, SUM(col_float8_raw) FROM test_table
      | group by col_boolean_raw
      | order by col_boolean_raw limit 2""".stripMargin, // sparkStatement
    Seq(Row(false, 575.8576385314492), Row(true, -1408.706167787151)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_FLOAT8_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_RAW" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT 2""".stripMargin // expectedPushdownStatement
  )

  val testSum46: TestCase = TestCase(
    """SELECT col_boolean_runlength, SUM(col_float8_runlength) FROM test_table
      | group by col_boolean_runlength
      | order by col_boolean_runlength limit 2""".stripMargin, // sparkStatement
    Seq(Row(false, -225.26299646296752), Row(true, -1763.9249873129531)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_FLOAT8_RUNLENGTH" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_RUNLENGTH" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT 2""".stripMargin // expectedPushdownStatement
  )

  val testSum47: TestCase = TestCase(
    """SELECT col_boolean_zstd, SUM(col_float8_zstd) FROM test_table
      | group by col_boolean_zstd
      | order by col_boolean_zstd limit 2""".stripMargin, // sparkStatement
    Seq(Row(false, 3098.4745398014834), Row(true, -1424.084129014804)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( SUM ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_FLOAT8_ZSTD" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT 2""".stripMargin // expectedPushdownStatement
  )
}
