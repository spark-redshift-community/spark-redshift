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

import java.sql.Date
import java.sql.Timestamp

trait AggregateMaxCorrectnessSuite extends IntegrationPushdownSuiteBase {
  val testMax00: TestCase = TestCase(
    """SELECT MAX(col_smallint_raw) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(200)), // expectedResult
    s"""SELECT ( MAX ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_RAW" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax01: TestCase = TestCase(
    """SELECT MAX(col_smallint_bytedict) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(200)), // expectedResult
    s"""SELECT ( MAX ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_BYTEDICT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax02: TestCase = TestCase(
    """SELECT MAX(col_smallint_delta) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(200)), // expectedResult
    s"""SELECT ( MAX ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_DELTA" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax03: TestCase = TestCase(
    """SELECT MAX(col_smallint_lzo) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(200)), // expectedResult
    s"""SELECT ( MAX ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_LZO" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax04: TestCase = TestCase(
    """SELECT MAX(col_smallint_mostly8) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(200)), // expectedResult
    s"""SELECT ( MAX ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_MOSTLY8" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax05: TestCase = TestCase(
    """SELECT MAX(col_smallint_runlength) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(200)), // expectedResult
    s"""SELECT ( MAX ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_RUNLENGTH" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
  )

  val testMax06: TestCase = TestCase(
    """SELECT MAX(col_smallint_zstd) FROM test_table""".stripMargin, // sparkStatement
    Seq(Row(200)), // expectedResult
    s"""SELECT ( MAX ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT ( "SUBQUERY_0"."COL_SMALLINT_ZSTD" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LIMIT 1""".stripMargin // expectedPushdownStatement
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
       | SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2_COL_0",
       | ( MAX ( "SUBQUERY_1"."SUBQUERY_1_COL_0") ) AS "SUBQUERY_2_COL_1" FROM (
       | SELECT ( "SUBQUERY_0"."COL_SMALLINT_RAW" ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."COL_SMALLINT_ZSTD" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1"
       | GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_2"
       | ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC ) AS "SUBQUERY_3"
       | ORDER BY ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) ASC
       | LIMIT5""".stripMargin // expectedPushdownStatement
  )
}