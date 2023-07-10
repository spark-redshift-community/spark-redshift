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

trait StringLengthCorrectnessSuite extends StringIntegrationPushdownSuiteBase {

  val testLength00: TestCase = TestCase(
    """SELECT LENGTH(testfixedstring) FROM test_table
      | WHERE testid=0""".stripMargin, // sparkStatement
    Seq(Row(null)), // expectedResult
    s"""SELECT ( LENGTH ( "SUBQUERY_1"."TESTFIXEDSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 0 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLength01: TestCase = TestCase(
    """SELECT LENGTH(testvarstring) FROM test_table
      | WHERE testid=0""".stripMargin, // sparkStatement
    Seq(Row(null)), // expectedResult
    s"""SELECT ( LENGTH ( "SUBQUERY_1"."TESTVARSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 0 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLength02: TestCase = TestCase(
    """SELECT LENGTH(testfixedstring) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row(11)), // expectedResult
    s"""SELECT ( LENGTH ( "SUBQUERY_1"."TESTFIXEDSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLength03: TestCase = TestCase(
    """SELECT LENGTH(testvarstring) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row(11)), // expectedResult
    s"""SELECT ( LENGTH ( "SUBQUERY_1"."TESTVARSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLength04: TestCase = TestCase(
    """SELECT LENGTH(testfixedstring) FROM test_table
      | WHERE testid=2""".stripMargin, // sparkStatement
    Seq(Row(17)), // expectedResult
    s"""SELECT ( LENGTH ( "SUBQUERY_1"."TESTFIXEDSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 2 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLength05: TestCase = TestCase(
    """SELECT LENGTH(testvarstring) FROM test_table
      | WHERE testid=2""".stripMargin, // sparkStatement
    Seq(Row(17)), // expectedResult
    s"""SELECT ( LENGTH ( "SUBQUERY_1"."TESTVARSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 2 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLength06: TestCase = TestCase(
    """SELECT LENGTH(testfixedstring) FROM test_table
      | WHERE testid=3""".stripMargin, // sparkStatement
    Seq(Row(10)), // expectedResult
    s"""SELECT ( LENGTH ( "SUBQUERY_1"."TESTFIXEDSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 3 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLength07: TestCase = TestCase(
    """SELECT LENGTH(testvarstring) FROM test_table
      | WHERE testid=3""".stripMargin, // sparkStatement
    Seq(Row(10)), // expectedResult
    s"""SELECT ( LENGTH ( "SUBQUERY_1"."TESTVARSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 3 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLength08: TestCase = TestCase(
    """SELECT LENGTH(testfixedstring) FROM test_table
      | WHERE testid=4""".stripMargin, // sparkStatement
    Seq(Row(16)), // expectedResult
    s"""SELECT ( LENGTH ( "SUBQUERY_1"."TESTFIXEDSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 4 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLength09: TestCase = TestCase(
    """SELECT LENGTH(testvarstring) FROM test_table
      | WHERE testid=4""".stripMargin, // sparkStatement
    Seq(Row(16)), // expectedResult
    s"""SELECT ( LENGTH ( "SUBQUERY_1"."TESTVARSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 4 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLength10: TestCase = TestCase(
    """SELECT LENGTH(testfixedstring) FROM test_table
      | WHERE testid=5""".stripMargin, // sparkStatement
    Seq(Row(0)), // expectedResult
    s"""SELECT ( LENGTH ( "SUBQUERY_1"."TESTFIXEDSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 5 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLength11: TestCase = TestCase(
    """SELECT LENGTH(testvarstring) FROM test_table
      | WHERE testid=5""".stripMargin, // sparkStatement
    Seq(Row(0)), // expectedResult
    s"""SELECT ( LENGTH ( "SUBQUERY_1"."TESTVARSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 5 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLength12: TestCase = TestCase(
    """SELECT LENGTH(testfixedstring) FROM test_table
      | WHERE testid=6""".stripMargin, // sparkStatement
    Seq(Row(13)), // expectedResult
    s"""SELECT ( LENGTH ( "SUBQUERY_1"."TESTFIXEDSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 6 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLength13: TestCase = TestCase(
    """SELECT LENGTH(testvarstring) FROM test_table
      | WHERE testid=6""".stripMargin, // sparkStatement
    Seq(Row(15)), // expectedResult
    s"""SELECT ( LENGTH ( "SUBQUERY_1"."TESTVARSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 6 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )
}
