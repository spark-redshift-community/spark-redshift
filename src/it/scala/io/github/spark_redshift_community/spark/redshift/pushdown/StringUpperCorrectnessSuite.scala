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

trait StringUpperCorrectnessSuite extends StringIntegrationPushdownSuiteBase {

  val testUpper00: TestCase = TestCase(
    """SELECT UPPER(testfixedstring) FROM test_table
      | WHERE testid=0""".stripMargin, // sparkStatement
    Seq(Row(null)), // expectedResult
    s"""SELECT ( UPPER ( "SQ_1"."TESTFIXEDSTRING" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
       | ( "SQ_0"."TESTID" = 0 ) ) ) AS
       | "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testUpper01: TestCase = TestCase(
    """SELECT UPPER(testvarstring) FROM test_table
      | WHERE testid=0""".stripMargin, // sparkStatement
    Seq(Row(null)), // expectedResult
    s"""SELECT ( UPPER ( "SQ_1"."TESTVARSTRING" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
       | ( "SQ_0"."TESTID" = 0 ) ) ) AS
       | "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testUpper02: TestCase = TestCase(
    """SELECT UPPER(testfixedstring) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("HELLO WORLD")), // expectedResult
    s"""SELECT ( UPPER ( "SQ_1"."TESTFIXEDSTRING" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
       | ( "SQ_0"."TESTID" = 1 ) ) ) AS
       | "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testUpper03: TestCase = TestCase(
    """SELECT UPPER(testvarstring) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("HELLO WORLD")), // expectedResult
    s"""SELECT ( UPPER ( "SQ_1"."TESTVARSTRING" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
       | ( "SQ_0"."TESTID" = 1 ) ) ) AS
       | "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testUpper04: TestCase = TestCase(
    """SELECT UPPER(testfixedstring) FROM test_table
      | WHERE testid=2""".stripMargin, // sparkStatement
    Seq(Row("CONTROLS\t \b\n\r\f\\'\"")), // expectedResult
    s"""SELECT ( UPPER ( "SQ_1"."TESTFIXEDSTRING" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
       | ( "SQ_0"."TESTID" = 2 ) ) ) AS
       | "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testUpper05: TestCase = TestCase(
    """SELECT UPPER(testvarstring) FROM test_table
      | WHERE testid=2""".stripMargin, // sparkStatement
    Seq(Row("CONTROLS\t \b\n\r\f\\'\"")), // expectedResult
    s"""SELECT ( UPPER ( "SQ_1"."TESTVARSTRING" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
       | ( "SQ_0"."TESTID" = 2 ) ) ) AS
       | "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testUpper06: TestCase = TestCase(
    """SELECT UPPER(testfixedstring) FROM test_table
      | WHERE testid=3""".stripMargin, // sparkStatement
    Seq(Row("SPECIALS/%")), // expectedResult
    s"""SELECT ( UPPER ( "SQ_1"."TESTFIXEDSTRING" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
       | ( "SQ_0"."TESTID" = 3 ) ) ) AS
       | "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testUpper07: TestCase = TestCase(
    """SELECT UPPER(testvarstring) FROM test_table
      | WHERE testid=3""".stripMargin, // sparkStatement
    Seq(Row("SPECIALS/%")), // expectedResult
    s"""SELECT ( UPPER ( "SQ_1"."TESTVARSTRING" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
       | ( "SQ_0"."TESTID" = 3 ) ) ) AS
       | "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testUpper08: TestCase = TestCase(
    """SELECT UPPER(testfixedstring) FROM test_table
      | WHERE testid=4""".stripMargin, // sparkStatement
    Seq(Row("SINGL_BYTE_CHARS")), // expectedResult
    s"""SELECT ( UPPER ( "SQ_1"."TESTFIXEDSTRING" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
       | ( "SQ_0"."TESTID" = 4 ) ) ) AS
       | "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testUpper09: TestCase = TestCase(
    """SELECT UPPER(testvarstring) FROM test_table
      | WHERE testid=4""".stripMargin, // sparkStatement
    Seq(Row("MULTI\u6A02BYTE\u8DA3CHARS")), // expectedResult
    s"""SELECT ( UPPER ( "SQ_1"."TESTVARSTRING" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
       | ( "SQ_0"."TESTID" = 4 ) ) ) AS
       | "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testUpper10: TestCase = TestCase(
    """SELECT UPPER(testfixedstring) FROM test_table
      | WHERE testid=5""".stripMargin, // sparkStatement
    Seq(Row("")), // expectedResult
    s"""SELECT ( UPPER ( "SQ_1"."TESTFIXEDSTRING" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
       | ( "SQ_0"."TESTID" = 5 ) ) ) AS
       | "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testUpper11: TestCase = TestCase(
    """SELECT UPPER(testvarstring) FROM test_table
      | WHERE testid=5""".stripMargin, // sparkStatement
    Seq(Row("")), // expectedResult
    s"""SELECT ( UPPER ( "SQ_1"."TESTVARSTRING" ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
       | ( "SQ_0"."TESTID" = 5 ) ) ) AS
       | "SQ_1"""".stripMargin // expectedPushdownStatement
  )
}
