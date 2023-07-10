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

trait StringAsciiCorrectnessSuite extends StringIntegrationPushdownSuiteBase {

  val testAscii00: TestCase = TestCase(
    """SELECT ASCII(SUBSTRING(testfixedstring, 1, 3)) FROM test_table
      | WHERE testid=0""".stripMargin, // sparkStatement
    Seq(Row(null)), // expectedResult
    s"""SELECT ( ASCII ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 1 , 3 ) ) )
       | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table AS
       | "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID"
       | IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 0 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testAscii01: TestCase = TestCase(
    """SELECT ASCII(SUBSTRING(testvarstring, 1, 3)) FROM test_table
      | WHERE testid=0""".stripMargin, // sparkStatement
    Seq(Row(null)), // expectedResult
    s"""SELECT ( ASCII ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 1 , 3 ) ) )
       | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table AS
       | "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID"
       | IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 0 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testAscii02: TestCase = TestCase(
    """SELECT ASCII(SUBSTRING(testfixedstring, 1, 5)) FROM test_table
      | WHERE testid=2""".stripMargin, // sparkStatement
    Seq(Row(67)), // expectedResult
    s"""SELECT ( ASCII ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 1 , 5 ) ) )
       | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table AS
       | "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID"
       | IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 2 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testAscii03: TestCase = TestCase(
    """SELECT ASCII(SUBSTRING(testvarstring, 1, 5)) FROM test_table
      | WHERE testid=2""".stripMargin, // sparkStatement
    Seq(Row(67)), // expectedResult
    s"""SELECT ( ASCII ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 1 , 5 ) ) )
       | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table AS
       | "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID"
       | IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 2 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testAscii04: TestCase = TestCase(
    """SELECT ASCII(SUBSTRING(testfixedstring, 9, 1)) FROM test_table
      | WHERE testid=2""".stripMargin, // sparkStatement
    Seq(Row(9)), // expectedResult
    s"""SELECT ( ASCII ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 9 , 1 ) ) )
       | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table AS
       | "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID"
       | IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 2 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testAscii05: TestCase = TestCase(
    """SELECT ASCII(SUBSTRING(testvarstring, 9, 5)) FROM test_table
      | WHERE testid=2""".stripMargin, // sparkStatement
    Seq(Row(9)), // expectedResult
    s"""SELECT ( ASCII ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 9 , 5 ) ) )
       | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table AS
       | "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID"
       | IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 2 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testAscii06: TestCase = TestCase(
    """SELECT ASCII(SUBSTRING(testfixedstring, 9, 2)) FROM test_table
      | WHERE testid=3""".stripMargin, // sparkStatement
    Seq(Row(47)), // expectedResult
    s"""SELECT ( ASCII ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 9 , 2 ) ) )
       | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table AS
       | "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID"
       | IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 3) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testAscii07: TestCase = TestCase(
    """SELECT ASCII(SUBSTRING(testvarstring, 9, 3)) FROM test_table
      | WHERE testid=3""".stripMargin, // sparkStatement
    Seq(Row(47)), // expectedResult
    s"""SELECT ( ASCII ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 9 , 3 ) ) )
       | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table AS
       | "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID"
       | IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 3 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testAscii08: TestCase = TestCase(
    """SELECT ASCII(SUBSTRING(testfixedstring, 1, 1)) FROM test_table
      | WHERE testid=5""".stripMargin, // sparkStatement
    Seq(Row(0)), // expectedResult
    s"""SELECT ( ASCII ( SUBSTRING ( "SUBQUERY_1"."TESTFIXEDSTRING" , 1 , 1 ) ) )
       | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table AS
       | "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID"
       | IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 5) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )

  val testAscii09: TestCase = TestCase(
    """SELECT ASCII(SUBSTRING(testvarstring, 1, 1)) FROM test_table
      | WHERE testid=5""".stripMargin, // sparkStatement
    Seq(Row(0)), // expectedResult
    s"""SELECT ( ASCII ( SUBSTRING ( "SUBQUERY_1"."TESTVARSTRING" , 1 , 1 ) ) )
       | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table AS
       | "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID"
       | IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = 5 ) ) ) AS
       | "SUBQUERY_1"""".stripMargin // expectedPushdownStatement
  )
}
