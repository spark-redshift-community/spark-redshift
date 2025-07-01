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
package io.github.spark_redshift_community.spark.redshift.pushdown.test

import org.apache.spark.sql.Row

trait StringTrimCorrectnessSuite extends StringIntegrationPushdownSuiteBase {

  val testTrim00: TestCase = TestCase(
    """SELECT TRIM(testfixedstring) FROM test_table
      | WHERE testid=0""".stripMargin, // sparkStatement
    Seq(Row(null)), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTFIXEDSTRING" , \\' \\' ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
       | ( "SQ_0"."TESTID" = 0 ) ) ) AS
       | "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim01: TestCase = TestCase(
    """SELECT TRIM(testvarstring) FROM test_table
      | WHERE testid=0""".stripMargin, // sparkStatement
    Seq(Row(null)), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTVARSTRING" , \\' \\' ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
       | ( "SQ_0"."TESTID" = 0 ) ) ) AS
       | "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim02: TestCase = TestCase(
    """SELECT TRIM(testfixedstring) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("Hello World")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTFIXEDSTRING" , \\' \\' ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
       | ( "SQ_0"."TESTID" = 1 ) ) ) AS
       | "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim03: TestCase = TestCase(
    """SELECT TRIM(testvarstring) FROM test_table
      | WHERE testid=1""".stripMargin, // sparkStatement
    Seq(Row("Hello World")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTVARSTRING" , \\' \\' ) ) AS "SQ_2_COL_0"
      | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS
      | "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID"
      | = 1 ) ) ) AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim04: TestCase = TestCase(
    """SELECT TRIM(testfixedstring) FROM test_table
      | WHERE testid=5""".stripMargin, // sparkStatement
    Seq(Row("")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTFIXEDSTRING" , \\' \\' ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS
       | "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID"
       | = 5 ) ) ) AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim05: TestCase = TestCase(
    """SELECT TRIM(testvarstring) FROM test_table
      | WHERE testid=5""".stripMargin, // sparkStatement
    Seq(Row("")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTVARSTRING" , \\' \\' ) ) AS "SQ_2_COL_0" FROM
       | ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0"
       | WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID" = 5 ) ) )
       | AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim06: TestCase = TestCase(
    """SELECT TRIM(testfixedstring) FROM test_table
      | WHERE testid=6""".stripMargin, // sparkStatement
    Seq(Row("Hello World")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTFIXEDSTRING" , \\' \\' ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS
       | "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID"
       | = 6 ) ) ) AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim07: TestCase = TestCase(
    """SELECT TRIM(testvarstring) FROM test_table
      | WHERE testid=6""".stripMargin, // sparkStatement
    Seq(Row("Hello World")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTVARSTRING" , \\' \\' ) ) AS "SQ_2_COL_0" FROM
       | ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0"
       | WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID" = 6 ) ) )
       | AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim08: TestCase = TestCase(
    """SELECT TRIM(testfixedstring) FROM test_table
      | WHERE testid=7""".stripMargin, // sparkStatement
    Seq(Row("\t\b\nFoo\r\f\\'\"")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTFIXEDSTRING" , \\' \\' ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS
       | "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID"
       | = 7 ) ) ) AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim09: TestCase = TestCase(
    """SELECT TRIM(testvarstring) FROM test_table
      | WHERE testid=7""".stripMargin, // sparkStatement
    Seq(Row("\t\b\nFoo\r\f\\'\"")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTVARSTRING" , \\' \\' ) ) AS "SQ_2_COL_0" FROM
       | ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0"
       | WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID" = 7 ) ) )
       | AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim10: TestCase = TestCase(
    """SELECT TRIM(testfixedstring) FROM test_table
      | WHERE testid=8""".stripMargin, // sparkStatement
    Seq(Row("/%Foo%/")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTFIXEDSTRING" , \\' \\' ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS
       | "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID"
       | = 8 ) ) ) AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim11: TestCase = TestCase(
    """SELECT TRIM(testvarstring) FROM test_table
      | WHERE testid=8""".stripMargin, // sparkStatement
    Seq(Row("/%Foo%/")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTVARSTRING" , \\' \\' ) ) AS "SQ_2_COL_0" FROM
       | ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0"
       | WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID" = 8 ) ) )
       | AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim12: TestCase = TestCase(
    """SELECT TRIM(testfixedstring) FROM test_table
      | WHERE testid=9""".stripMargin, // sparkStatement
    Seq(Row("_Single_")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTFIXEDSTRING" , \\' \\' ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS
       | "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID"
       | = 9 ) ) ) AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim13: TestCase = TestCase(
    """SELECT TRIM(testvarstring) FROM test_table
      | WHERE testid=9""".stripMargin, // sparkStatement
    Seq(Row("\u6A02Multi\u8DA3")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTVARSTRING" , \\' \\' ) ) AS "SQ_2_COL_0" FROM
       | ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0"
       | WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID" = 9 ) ) )
       | AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim14: TestCase = TestCase(
    """SELECT TRIM(BOTH FROM testfixedstring) FROM test_table
      | WHERE testid=6""".stripMargin, // sparkStatement
    Seq(Row("Hello World")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTFIXEDSTRING" , \\' \\' ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
       | ( "SQ_0"."TESTID" = 6 ) ) ) AS
       | "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim15: TestCase = TestCase(
    """SELECT TRIM(BOTH FROM testvarstring) FROM test_table
      | WHERE testid=6""".stripMargin, // sparkStatement
    Seq(Row("Hello World")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTVARSTRING" , \\' \\' ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
       | ( "SQ_0"."TESTID" = 6 ) ) ) AS
       | "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim16: TestCase = TestCase(
    """SELECT TRIM(LEADING FROM testfixedstring) FROM test_table
      | WHERE testid=6""".stripMargin, // sparkStatement
    Seq(Row("Hello World")), // expectedResult
    s"""SELECT ( LTRIM ( "SQ_1"."TESTFIXEDSTRING" , \\' \\' ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
       | ( "SQ_0"."TESTID" = 6 ) ) ) AS
       | "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim17: TestCase = TestCase(
    """SELECT TRIM(LEADING FROM testvarstring) FROM test_table
      | WHERE testid=6""".stripMargin, // sparkStatement
    Seq(Row("Hello World  ")), // expectedResult
    s"""SELECT ( LTRIM ( "SQ_1"."TESTVARSTRING" , \\' \\' ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
       | ( "SQ_0"."TESTID" = 6 ) ) ) AS
       | "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim18: TestCase = TestCase(
    """SELECT TRIM(TRAILING FROM testfixedstring) FROM test_table
      | WHERE testid=6""".stripMargin, // sparkStatement
    Seq(Row("  Hello World")), // expectedResult
    s"""SELECT ( RTRIM ( "SQ_1"."TESTFIXEDSTRING" , \\' \\' ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
       | ( "SQ_0"."TESTID" = 6 ) ) ) AS
       | "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim19: TestCase = TestCase(
    """SELECT TRIM(TRAILING FROM testvarstring) FROM test_table
      | WHERE testid=6""".stripMargin, // sparkStatement
    Seq(Row("  Hello World")), // expectedResult
    s"""SELECT ( RTRIM ( "SQ_1"."TESTVARSTRING" , \\' \\' ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
       | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
       | ( "SQ_0"."TESTID" = 6 ) ) ) AS
       | "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim20: TestCase = TestCase(
    """SELECT TRIM(' ' FROM testfixedstring) FROM test_table
      | WHERE testid=6""".stripMargin, // sparkStatement
    Seq(Row("Hello World")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTFIXEDSTRING" , \\' \\' ) ) AS
      | "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
      | $test_table AS "RCQ_ALIAS" ) AS "SQ_0" WHERE
      | ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID"
      | = 6 ) ) ) AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim21: TestCase = TestCase(
    """SELECT TRIM(' ' FROM testvarstring) FROM test_table
      | WHERE testid=6""".stripMargin, // sparkStatement
    Seq(Row("Hello World")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTVARSTRING" , \\' \\' ) ) AS
       | "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RCQ_ALIAS" ) AS "SQ_0" WHERE
       | ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID"
       | = 6 ) ) ) AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim22: TestCase = TestCase(
    """SELECT TRIM(BOTH ' ' FROM testfixedstring) FROM test_table
      | WHERE testid=6""".stripMargin, // sparkStatement
    Seq(Row("Hello World")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTFIXEDSTRING" , \\' \\' ) ) AS
       | "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RCQ_ALIAS" ) AS "SQ_0" WHERE
       | ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID"
       | = 6 ) ) ) AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim23: TestCase = TestCase(
    """SELECT TRIM(BOTH ' ' FROM testvarstring) FROM test_table
      | WHERE testid=6""".stripMargin, // sparkStatement
    Seq(Row("Hello World")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTVARSTRING" , \\' \\' ) ) AS
       | "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RCQ_ALIAS" ) AS "SQ_0" WHERE
       | ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID"
       | = 6 ) ) ) AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim24: TestCase = TestCase(
    """SELECT TRIM(LEADING ' ' FROM testfixedstring) FROM test_table
      | WHERE testid=6""".stripMargin, // sparkStatement
    Seq(Row("Hello World")), // expectedResult
    s"""SELECT ( LTRIM ( "SQ_1"."TESTFIXEDSTRING" , \\' \\' ) ) AS
       | "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RCQ_ALIAS" ) AS "SQ_0" WHERE
       | ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID"
       | = 6 ) ) ) AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim25: TestCase = TestCase(
    """SELECT TRIM(LEADING ' ' FROM testvarstring) FROM test_table
      | WHERE testid=6""".stripMargin, // sparkStatement
    Seq(Row("Hello World  ")), // expectedResult
    s"""SELECT ( LTRIM ( "SQ_1"."TESTVARSTRING" , \\' \\' ) ) AS
       | "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RCQ_ALIAS" ) AS "SQ_0" WHERE
       | ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID"
       | = 6 ) ) ) AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim26: TestCase = TestCase(
    """SELECT TRIM(TRAILING ' ' FROM testfixedstring) FROM test_table
      | WHERE testid=6""".stripMargin, // sparkStatement
    Seq(Row("  Hello World")), // expectedResult
    s"""SELECT ( RTRIM ( "SQ_1"."TESTFIXEDSTRING" , \\' \\' ) ) AS
       | "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RCQ_ALIAS" ) AS "SQ_0" WHERE
       | ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID"
       | = 6 ) ) ) AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim27: TestCase = TestCase(
    """SELECT TRIM(TRAILING ' ' FROM testvarstring) FROM test_table
      | WHERE testid=6""".stripMargin, // sparkStatement
    Seq(Row("  Hello World")), // expectedResult
    s"""SELECT ( RTRIM ( "SQ_1"."TESTVARSTRING" , \\' \\' ) ) AS
       | "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RCQ_ALIAS" ) AS "SQ_0" WHERE
       | ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID"
       | = 6 ) ) ) AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim28: TestCase = TestCase(
    """SELECT TRIM(' \t\b\n' FROM testfixedstring) FROM test_table
      | WHERE testid=7""".stripMargin, // sparkStatement
    Seq(Row("Foo\r\f\\\'\"")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTFIXEDSTRING" , \\' \t\b\n\\' ) ) AS
       | "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RCQ_ALIAS" ) AS "SQ_0" WHERE
       | ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID"
       | = 7 ) ) ) AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim29: TestCase = TestCase(
    """SELECT TRIM(' \t\b\n' FROM testvarstring) FROM test_table
      | WHERE testid=7""".stripMargin, // sparkStatement
    Seq(Row("Foo\r\f\\\'\"")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTVARSTRING" , \\' \t\b\n\\' ) ) AS
       | "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RCQ_ALIAS" ) AS "SQ_0" WHERE
       | ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID"
       | = 7 ) ) ) AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim30: TestCase = TestCase(
    """SELECT TRIM(' /%' FROM testfixedstring) FROM test_table
      | WHERE testid=8""".stripMargin, // sparkStatement
    Seq(Row("Foo")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTFIXEDSTRING" , \\' /%\\' ) ) AS
       | "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RCQ_ALIAS" ) AS "SQ_0" WHERE
       | ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID"
       | = 8 ) ) ) AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim31: TestCase = TestCase(
    """SELECT TRIM(' /%' FROM testvarstring) FROM test_table
      | WHERE testid=8""".stripMargin, // sparkStatement
    Seq(Row("Foo")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTVARSTRING" , \\' /%\\' ) ) AS
       | "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RCQ_ALIAS" ) AS "SQ_0" WHERE
       | ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID"
       | = 8 ) ) ) AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim32: TestCase = TestCase(
    """SELECT TRIM(' _' FROM testfixedstring) FROM test_table
      | WHERE testid=9""".stripMargin, // sparkStatement
    Seq(Row("Single")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTFIXEDSTRING" , \\' _\\' ) ) AS
       | "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
       | $test_table AS "RCQ_ALIAS" ) AS "SQ_0" WHERE
       | ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID"
       | = 9 ) ) ) AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim33: TestCase = TestCase(
    """SELECT TRIM(' \u6A02\u8DA3' FROM testvarstring) FROM test_table
      | WHERE testid=9""".stripMargin, // sparkStatement
    Seq(Row("Multi")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTVARSTRING" , \\' \u6A02\u8DA3\\' ) ) AS
      | "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
      | $test_table AS "RCQ_ALIAS" ) AS "SQ_0" WHERE
      | ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID"
      | = 9 ) ) ) AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim34: TestCase = TestCase(
    """SELECT TRIM(LEADING FROM testvarstring) FROM test_table
      | WHERE testid=7""".stripMargin, // sparkStatement
    Seq(Row("\t\b\nFoo\r\f\\'\"  ")), // expectedResult
    s"""SELECT ( LTRIM ( "SQ_1"."TESTVARSTRING" , \\' \\' ) ) AS "SQ_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS
       | "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID"
       | = 7 ) ) ) AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim35: TestCase = TestCase(
    """SELECT TRIM(TRAILING FROM testvarstring) FROM test_table
      | WHERE testid=7""".stripMargin, // sparkStatement
    Seq(Row("  \t\b\nFoo\r\f\\'\"")), // expectedResult
    s"""SELECT ( RTRIM ( "SQ_1"."TESTVARSTRING" , \\' \\' ) ) AS "SQ_2_COL_0" FROM
       | ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0"
       | WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID" = 7 ) ) )
       | AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )

  val testTrim36: TestCase = TestCase(
    """SELECT TRIM(BOTH FROM testvarstring) FROM test_table
      | WHERE testid=7""".stripMargin, // sparkStatement
    Seq(Row("\t\b\nFoo\r\f\\'\"")), // expectedResult
    s"""SELECT ( TRIM ( "SQ_1"."TESTVARSTRING" , \\' \\' ) ) AS "SQ_2_COL_0" FROM
       | ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0"
       | WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND ( "SQ_0"."TESTID" = 7 ) ) )
       | AS "SQ_1"""".stripMargin // expectedPushdownStatement
  )
}
