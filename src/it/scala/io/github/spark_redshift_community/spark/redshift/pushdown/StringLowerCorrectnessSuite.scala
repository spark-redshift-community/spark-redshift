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

trait StringLowerCorrectnessSuite extends StringIntegrationPushdownSuiteBase {

  val testLower00: TestCase = TestCase(
    """SELECT LOWER(testfixedstring) FROM test_table
      | WHERE testid=0""".stripMargin, // sparkStatement
    Seq(Row(null)), // expectedResult
    s"""SELECT ( LOWER ( "SUBQUERY_1"."TESTFIXEDSTRING" ) ) AS "SUBQUERY_2_COL_0"
      | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
      | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
      | ( "SUBQUERY_0"."TESTID" = 0 ) ) ) AS
      | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLower01: TestCase = TestCase(
    """SELECT LOWER(testvarstring) FROM test_table
      | WHERE testid=0""".stripMargin, // sparkStatement
    Seq(Row(null)), // expectedResult
    s"""SELECT ( LOWER ( "SUBQUERY_1"."TESTVARSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 0 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLower02: TestCase = TestCase(
    """SELECT LOWER(testfixedstring) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("hello world")), // expectedResult
    s"""SELECT ( LOWER ( "SUBQUERY_1"."TESTFIXEDSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLower03: TestCase = TestCase(
    """SELECT LOWER(testvarstring) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("hello world")), // expectedResult
    s"""SELECT ( LOWER ( "SUBQUERY_1"."TESTVARSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLower04: TestCase = TestCase(
    """SELECT LOWER(testfixedstring) FROM test_table
      | WHERE testid=2""".stripMargin, // sparkStatement
    Seq(Row("controls\t \b\n\r\f\\'\"")), // expectedResult
    s"""SELECT ( LOWER ( "SUBQUERY_1"."TESTFIXEDSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 2 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLower05: TestCase = TestCase(
    """SELECT LOWER(testvarstring) FROM test_table
      | WHERE testid=2""".stripMargin, // sparkStatement
    Seq(Row("controls\t \b\n\r\f\\'\"")), // expectedResult
    s"""SELECT ( LOWER ( "SUBQUERY_1"."TESTVARSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 2 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLower06: TestCase = TestCase(
    """SELECT LOWER(testfixedstring) FROM test_table
      | WHERE testid=3""".stripMargin, // sparkStatement
    Seq(Row("specials/%")), // expectedResult
    s"""SELECT ( LOWER ( "SUBQUERY_1"."TESTFIXEDSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 3 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLower07: TestCase = TestCase(
    """SELECT LOWER(testvarstring) FROM test_table
      | WHERE testid=3""".stripMargin, // sparkStatement
    Seq(Row("specials/%")), // expectedResult
    s"""SELECT ( LOWER ( "SUBQUERY_1"."TESTVARSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 3 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLower08: TestCase = TestCase(
    """SELECT LOWER(testfixedstring) FROM test_table
      | WHERE testid=4""".stripMargin, // sparkStatement
    Seq(Row("singl_byte_chars")), // expectedResult
    s"""SELECT ( LOWER ( "SUBQUERY_1"."TESTFIXEDSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 4 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLower09: TestCase = TestCase(
    """SELECT LOWER(testvarstring) FROM test_table
      | WHERE testid=4""".stripMargin, // sparkStatement
    Seq(Row("multi\u6A02byte\u8DA3chars")), // expectedResult
    s"""SELECT ( LOWER ( "SUBQUERY_1"."TESTVARSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 4 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLower10: TestCase = TestCase(
    """SELECT LOWER(testfixedstring) FROM test_table
      | WHERE testid=5""".stripMargin, // sparkStatement
    Seq(Row("")), // expectedResult
    s"""SELECT ( LOWER ( "SUBQUERY_1"."TESTFIXEDSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 5 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testLower11: TestCase = TestCase(
    """SELECT LOWER(testvarstring) FROM test_table
      | WHERE testid=5""".stripMargin, // sparkStatement
    Seq(Row("")), // expectedResult
    s"""SELECT ( LOWER ( "SUBQUERY_1"."TESTVARSTRING" ) ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
       | ( "SUBQUERY_0"."TESTID" = 5 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )
}
