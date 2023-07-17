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

trait AggregateAvgCorrectnessSuite extends IntegrationPushdownSuiteBase {
  val testAvg00: TestCase = TestCase(
    """SELECT AVG(col_smallint_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(1.3368)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg01: TestCase = TestCase(
    """SELECT AVG(col_smallint_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(1.1216)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg02: TestCase = TestCase(
    """SELECT AVG(col_smallint_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-0.4714)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_DELTA" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg03: TestCase = TestCase(
    """SELECT AVG(col_smallint_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(1.5176)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg04: TestCase = TestCase(
    """SELECT AVG(col_smallint_mostly8) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-0.5756)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_MOSTLY8" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg05: TestCase = TestCase(
    """SELECT AVG(col_smallint_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-3.745)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg06: TestCase = TestCase(
    """SELECT AVG(col_smallint_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(1.6486)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg07: TestCase = TestCase(
    """SELECT col_smallint_zstd, AVG(col_smallint_raw) FROM test_table
      | group by col_smallint_zstd
      | order by col_smallint_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-200, 3.0555555555555554),
      Row(-199, 90.0),
      Row(-198, 12.416666666666666),
      Row(-197, 14.428571428571429),
      Row(-196, 21.058823529411764)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_SMALLINT_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_SMALLINT_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testAvg10: TestCase = TestCase(
    """SELECT AVG(col_int_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(2.9638)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg11: TestCase = TestCase(
    """SELECT AVG(col_int_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(9.4802)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg12: TestCase = TestCase(
    """SELECT AVG(col_int_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-0.3862)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_DELTA" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg13: TestCase = TestCase(
    """SELECT AVG(col_int_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(4.4422)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg14: TestCase = TestCase(
    """SELECT AVG(col_int_mostly8) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(0.2172)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_MOSTLY8" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg15: TestCase = TestCase(
    """SELECT AVG(col_int_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(0.5012)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg16: TestCase = TestCase(
    """SELECT AVG(col_int_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(0.6654)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_INT_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg17: TestCase = TestCase(
    """SELECT col_int_zstd, AVG(col_int_raw) FROM test_table
      | group by col_int_zstd
      | order by col_int_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-500, 185.5),
      Row(-499, 31.0),
      Row(-498, -184.0),
      Row(-497, 11.666666666666666),
      Row(-496, 464.0)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_INT_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_INT_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testAvg18: TestCase = TestCase(
    """SELECT col_boolean_zstd, AVG(col_int_zstd) FROM test_table
      | group by col_boolean_zstd
      | order by col_boolean_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(false, 0.677719369187222), Row(true, 0.6533438860308667)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_INT_ZSTD" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BOOLEAN_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0") AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testAvg20: TestCase = TestCase(
    """SELECT AVG(col_bigint_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(172.6448)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BIGINT_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg21: TestCase = TestCase(
    """SELECT AVG(col_bigint_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-176.6256)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BIGINT_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg22: TestCase = TestCase(
    """SELECT AVG(col_bigint_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(25.9622)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BIGINT_DELTA" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg23: TestCase = TestCase(
    """SELECT AVG(col_bigint_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(160.4768)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BIGINT_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg24: TestCase = TestCase(
    """SELECT AVG(col_bigint_mostly8) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(6.3812)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BIGINT_MOSTLY8" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg25: TestCase = TestCase(
    """SELECT AVG(col_bigint_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(0.0212)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BIGINT_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg26: TestCase = TestCase(
    """SELECT AVG(col_bigint_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-27.7582)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_BIGINT_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg27: TestCase = TestCase(
    """SELECT col_bigint_zstd, AVG(col_bigint_raw) FROM test_table
      | group by col_bigint_zstd
      | order by col_bigint_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-9987, 7747),
      Row(-9979, -3749),
      Row(-9970, -7372),
      Row(-9965, 2510),
      Row(-9951, 5378)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_BIGINT_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_BIGINT_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testAvg30: TestCase = TestCase(
    """SELECT AVG(col_decimal_1_0_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-0.0572)), // expectedResult
    s"""SELECT ( CAST ( ( AVG ( ( "SUBQUERY_1"."SUBQUERY_1_COL_0" * POW ( 10,0 ) ) ::FLOAT ) / 1.0 )
       | AS DECIMAL(5,4) ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_1_0_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg31: TestCase = TestCase(
    """SELECT AVG(col_decimal_18_0_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-733.6378)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_18_0_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg32: TestCase = TestCase(
    """SELECT AVG(col_decimal_18_18_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(0.550180366)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_18_18_DELTA" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg33: TestCase = TestCase(
    """SELECT AVG(col_decimal_18_0_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(648.1886)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_18_0_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg34: TestCase = TestCase(
    """SELECT AVG(col_decimal_18_18_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(0.554921764)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_DECIMAL_18_18_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg35: TestCase = TestCase(
    """SELECT col_decimal_18_0_zstd, AVG(col_decimal_18_0_raw) FROM test_table
      | group by col_decimal_18_0_zstd
      | order by col_decimal_18_0_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-99917, 78608.0),
      Row(-99888, 1099.0),
      Row(-99863, -9247.0),
      Row(-99822, -30101.0),
      Row(-99780, -74476.0)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ::FLOAT ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_DECIMAL_18_0_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_DECIMAL_18_0_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testAvg40: TestCase = TestCase(
    """SELECT AVG(col_float4_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-0.14565977688841522)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT4_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg41: TestCase = TestCase(
    """SELECT AVG(col_float4_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(0.0061145949998637665)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT4_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg42: TestCase = TestCase(
    """SELECT AVG(col_float4_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(0.035539811275526884)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT4_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg43: TestCase = TestCase(
    """SELECT AVG(col_float4_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(0.25050870943060144)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT4_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg44: TestCase = TestCase(
    """SELECT AVG(col_float8_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-0.16656970585113978)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT8_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg45: TestCase = TestCase(
    """SELECT AVG(col_float8_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(0.522842350282609)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT8_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg46: TestCase = TestCase(
    """SELECT AVG(col_float8_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(-0.397837596755183)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT8_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg47: TestCase = TestCase(
    """SELECT AVG(col_float8_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(0.3348780821573379)), // expectedResult
    s"""SELECT ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_FLOAT8_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testAvg48: TestCase = TestCase(
    """SELECT col_float4_zstd, AVG(col_float4_raw) FROM test_table
      | group by col_float4_zstd
      | order by col_float4_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-39.99878.toFloat, -34.339485.toFloat),
      Row(-39.968693.toFloat, -27.364058.toFloat),
      Row(-39.94803.toFloat, 3.893314.toFloat),
      Row(-39.920315.toFloat, -18.851852.toFloat),
      Row(-39.919968.toFloat, -15.938475.toFloat)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_FLOAT4_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_FLOAT4_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )

  val testAvg49: TestCase = TestCase(
    """SELECT col_float8_zstd, AVG(col_float8_raw) FROM test_table
      | group by col_float8_zstd
      | order by col_float8_zstd limit 5""".stripMargin, // sparkStatement
    Seq(Row(-59.98139065242519, -38.50286940512086),
      Row(-59.96652420830154, 29.95038540500704),
      Row(-59.93905605936271, 50.49272294340629),
      Row(-59.90738346844318, -53.58293774248173),
      Row(-59.89910788903543, 37.013841179953545)), // expectedResult
    s"""SELECT * FROM ( SELECT * FROM (
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( AVG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_FLOAT8_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_FLOAT8_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC NULLS FIRST
       | LIMIT 5""".stripMargin // expectedPushdownStatement
  )
}
