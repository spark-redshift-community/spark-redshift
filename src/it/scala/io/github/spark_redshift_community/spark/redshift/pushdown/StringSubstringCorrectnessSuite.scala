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

trait StringSubstringCorrectnessSuite extends StringIntegrationPushdownSuiteBase {

  val testSubstr00: TestCase = TestCase(
    """SELECT SUBSTRING(testfixedstring, 1, 5) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("Hello")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 1 , 5 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr01: TestCase = TestCase(
    """SELECT SUBSTRING(testvarstring, 1, 5) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("Hello")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 1 , 5 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr02: TestCase = TestCase(
    """SELECT SUBSTRING(testfixedstring FROM 1 FOR 5) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("Hello")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 1 , 5 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr03: TestCase = TestCase(
    """SELECT SUBSTRING(testvarstring FROM 1 FOR 5) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("Hello")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 1 , 5 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr04: TestCase = TestCase(
    """SELECT SUBSTRING(testfixedstring, 7, 5) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("World")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 7 , 5 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr05: TestCase = TestCase(
    """SELECT SUBSTRING(testvarstring, 7, 5) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("World")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 7 , 5 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr06: TestCase = TestCase(
    """SELECT SUBSTRING(testfixedstring FROM 7 FOR 5) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("World")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 7 , 5 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr07: TestCase = TestCase(
    """SELECT SUBSTRING(testvarstring FROM 7 FOR 5) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("World")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 7 , 5 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr08: TestCase = TestCase(
    """SELECT SUBSTRING(testfixedstring, 0, 6) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("Hello")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 0, 6 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr09: TestCase = TestCase(
    """SELECT SUBSTRING(testvarstring, 0, 6) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("Hello")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 0 , 6 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr10: TestCase = TestCase(
    """SELECT SUBSTRING(testfixedstring FROM 0 FOR 6) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("Hello")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 0 , 6 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr11: TestCase = TestCase(
    """SELECT SUBSTRING(testvarstring FROM 0 FOR 6) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("Hello")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 0 , 6 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr12: TestCase = TestCase(
    """SELECT SUBSTRING(testfixedstring, -5, 11) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("Hello")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , -5 , 11 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr13: TestCase = TestCase(
    """SELECT SUBSTRING(testvarstring, -5, 11) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("Hello")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , -5 , 11 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr14: TestCase = TestCase(
    """SELECT SUBSTRING(testfixedstring FROM -5 FOR 11) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("Hello")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , -5 , 11 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr15: TestCase = TestCase(
    """SELECT SUBSTRING(testvarstring FROM -5 FOR 11) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("Hello")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , -5 , 11 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr16: TestCase = TestCase(
    """SELECT SUBSTRING(testfixedstring, 7, 50) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("World")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 7 , 50 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr17: TestCase = TestCase(
    """SELECT SUBSTRING(testvarstring, 7, 50) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("World")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 7 , 50 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr18: TestCase = TestCase(
    """SELECT SUBSTRING(testfixedstring FROM 7 FOR 50) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("World")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 7 , 50 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr19: TestCase = TestCase(
    """SELECT SUBSTRING(testvarstring FROM 7 FOR 50) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("World")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 7 , 50 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr20: TestCase = TestCase(
    """SELECT SUBSTRING(testfixedstring FROM 7) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("World")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 7 , 2147483647 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr21: TestCase = TestCase(
    """SELECT SUBSTRING(testvarstring FROM 7) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("World")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 7 , 2147483647 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr22: TestCase = TestCase(
    """SELECT SUBSTRING(testfixedstring, 50, 100) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 50 , 100 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr23: TestCase = TestCase(
    """SELECT SUBSTRING(testvarstring, 50, 100) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 50 , 100 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr24: TestCase = TestCase(
    """SELECT SUBSTRING(testfixedstring FROM 50 FOR 100) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 50 , 100 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr25: TestCase = TestCase(
    """SELECT SUBSTRING(testvarstring FROM 50 FOR 100) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 50 , 100 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr26: TestCase = TestCase(
    """SELECT SUBSTRING(testfixedstring FROM 50) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 50 , 2147483647 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr27: TestCase = TestCase(
    """SELECT SUBSTRING(testvarstring FROM 50) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 50 , 2147483647 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr28: TestCase = TestCase(
    """SELECT SUBSTRING(testfixedstring, 9, 9) FROM test_table
      | WHERE testid=2""".stripMargin, // sparkStatement
    Seq(Row("\t \b\n\r\f\\'\"")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 9 , 9 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 2 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr29: TestCase = TestCase(
    """SELECT SUBSTRING(testvarstring, 9, 9) FROM test_table
      | WHERE testid=2""".stripMargin, // sparkStatement
    Seq(Row("\t \b\n\r\f\\'\"")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 9 , 9 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 2 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr30: TestCase = TestCase(
    """SELECT SUBSTRING(testfixedstring, 9, 2) FROM test_table
      | WHERE testid=3""".stripMargin, // sparkStatement
    Seq(Row("/%")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 9 , 2 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 3 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr31: TestCase = TestCase(
    """SELECT SUBSTRING(testvarstring, 9, 2) FROM test_table
      | WHERE testid=3""".stripMargin, // sparkStatement
    Seq(Row("/%")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 9 , 2 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 3 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr32: TestCase = TestCase(
    """SELECT SUBSTRING(testvarstring, 6, 6) FROM test_table
      | WHERE testid=4""".stripMargin, // sparkStatement
    Seq(Row("\u6A02Byte\u8DA3")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 6 , 6 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 4 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr33: TestCase = TestCase(
    """SELECT SUBSTRING(testfixedstring, 6, 7) FROM test_table
      | WHERE testid=0""".stripMargin, // sparkStatement
    Seq(Row(null)), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 6 , 7 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 0 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr34: TestCase = TestCase(
    """SELECT SUBSTRING(testvarstring, 6, 7) FROM test_table
      | WHERE testid=0""".stripMargin, // sparkStatement
    Seq(Row(null)), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 6 , 7 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 0 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr35: TestCase = TestCase(
    """SELECT SUBSTRING(testfixedstring, 1, 1) FROM test_table
      | WHERE testid=5""".stripMargin, // sparkStatement
    Seq(Row("")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 1 , 1 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 5 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr36: TestCase = TestCase(
    """SELECT SUBSTRING(testvarstring, 1, 1) FROM test_table
      | WHERE testid=5""".stripMargin, // sparkStatement
    Seq(Row("")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 1 , 1 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 5 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr37: TestCase = TestCase(
    """SELECT SUBSTR(testfixedstring, 1, 5) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("Hello")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 1 , 5 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr38: TestCase = TestCase(
    """SELECT SUBSTR(testvarstring, 1, 5) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("Hello")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 1 , 5 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr39: TestCase = TestCase(
    """SELECT SUBSTR(testfixedstring FROM 1 FOR 5) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("Hello")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 1 , 5 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr40: TestCase = TestCase(
    """SELECT SUBSTR(testvarstring FROM 1 FOR 5) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("Hello")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 1 , 5 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr41: TestCase = TestCase(
    """SELECT SUBSTR(testfixedstring FROM 3) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("llo World")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 3 , 2147483647 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testSubstr42: TestCase = TestCase(
    """SELECT SUBSTR(testvarstring FROM 3) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("llo World")), // expectedResult
    s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 3 , 2147483647 ) ) AS
       | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
       | ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 1 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )
}
