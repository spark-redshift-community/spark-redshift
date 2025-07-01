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

import java.io.ByteArrayOutputStream

abstract class PushdownOnlyShowSuite extends IntegrationPushdownSuiteBase {
  test ("show()", P0Test, P1Test) {
    val baos = new ByteArrayOutputStream()
    Console.withOut(baos) {
      sqlContext.sql("""SELECT * FROM test_table where teststring is null or teststring not like 'Unicode%'""").show()
    }
    var expectedResult =
      """+--------+--------+----------+-------------------+---------+-------+----------------+---------+----------+-------------------+
         ||testbyte|testbool|  testdate|         testdouble|testfloat|testint|        testlong|testshort|teststring|      testtimestamp|
         |+--------+--------+----------+-------------------+---------+-------+----------------+---------+----------+-------------------+
         ||    null|    null|      null|               null|     null|   null|            null|     null|      null|               null|
         ||       0|    null|2015-07-03|                  0|       -1|4141214|1239012341823719|     null|         f|2015-07-03 12:34:56|
         ||       0|   false|      null|-1234152.1231249799|   100000|   null|1239012341823719|       24|  ___|_123|               null|
         ||       1|   false|2015-07-02|                  0|        0|     42|1239012341823719|      -13|      asdf|2015-07-02 00:00:00|
         |+--------+--------+----------+-------------------+---------+-------+----------------+---------+----------+-------------------+""".stripMargin
    // Adjust for null values being upper-case for Spark 3.5 or newer.
    if (sc.version.replace(".", "").toInt >= 350) {
      expectedResult = expectedResult.replace("null", "NULL")
    }
    checkResult(expectedResult, baos.toString)

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT ( CAST ( "SQ_1"."TESTBYTE" AS VARCHAR ) ) AS "SQ_2_COL_0",
         |( CASE "SQ_1"."TESTBOOL" WHEN TRUE THEN \\'true\\' WHEN FALSE THEN \\'false\\' ELSE null END ) AS "SQ_2_COL_1",
         |( CAST ( "SQ_1"."TESTDATE" AS VARCHAR ) ) AS "SQ_2_COL_2",
         |( CAST ( "SQ_1"."TESTDOUBLE" AS VARCHAR ) ) AS "SQ_2_COL_3",
         |( CAST ( "SQ_1"."TESTFLOAT" AS VARCHAR ) ) AS "SQ_2_COL_4",
         |( CAST ( "SQ_1"."TESTINT" AS VARCHAR ) ) AS "SQ_2_COL_5",
         |( CAST ( "SQ_1"."TESTLONG" AS VARCHAR ) ) AS "SQ_2_COL_6",
         |( CAST ( "SQ_1"."TESTSHORT" AS VARCHAR ) ) AS "SQ_2_COL_7",
         |( "SQ_1"."TESTSTRING" ) AS "SQ_2_COL_8",
         |( CAST ( "SQ_1"."TESTTIMESTAMP" AS VARCHAR ) ) AS "SQ_2_COL_9"
         | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0"
         | WHERE ( ( "SQ_0"."TESTSTRING" IS NULL ) OR NOT ( ( CAST ( "SQ_0"."TESTSTRING" AS VARCHAR ) LIKE CONCAT ( \\'Unicode\\' , \\'%\\' ) ) ) ) )
         | AS "SQ_1" ) AS "SQ_2" LIMIT 21""".stripMargin,
      s"""SELECT * FROM ( SELECT
         |( CASE WHEN "SQ_1"."TESTBYTE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTBYTE" AS VARCHAR ) END ) AS "SQ_2_COL_0",
         |( CASE WHEN "SQ_1"."TESTBOOL" IS NULL THEN \\'NULL\\' ELSE CASE "SQ_1"."TESTBOOL" WHEN TRUE THEN \\'true\\' WHEN FALSE THEN \\'false\\' ELSE null END END ) AS "SQ_2_COL_1",
         |( CASE WHEN "SQ_1"."TESTDATE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTDATE" AS VARCHAR ) END ) AS "SQ_2_COL_2",
         |( CASE WHEN "SQ_1"."TESTDOUBLE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTDOUBLE" AS VARCHAR ) END ) AS "SQ_2_COL_3",
         |( CASE WHEN "SQ_1"."TESTFLOAT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTFLOAT" AS VARCHAR ) END ) AS "SQ_2_COL_4",
         |( CASE WHEN "SQ_1"."TESTINT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTINT" AS VARCHAR ) END ) AS "SQ_2_COL_5",
         |( CASE WHEN "SQ_1"."TESTLONG" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTLONG" AS VARCHAR ) END ) AS "SQ_2_COL_6",
         |( CASE WHEN "SQ_1"."TESTSHORT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTSHORT" AS VARCHAR ) END ) AS "SQ_2_COL_7",
         |( CASE WHEN "SQ_1"."TESTSTRING" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTSTRING" AS VARCHAR ) END ) AS "SQ_2_COL_8",
         |( CASE WHEN "SQ_1"."TESTTIMESTAMP" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTTIMESTAMP" AS VARCHAR ) END ) AS "SQ_2_COL_9"
         | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0"
         | WHERE ( ( "SQ_0"."TESTSTRING" IS NULL ) OR NOT ( ( CAST ( "SQ_0"."TESTSTRING" AS VARCHAR ) LIKE CONCAT ( \\'Unicode\\', \\'%\\' ) ) ) ) )
         | AS "SQ_1" ) AS "SQ_2" LIMIT 21""".stripMargin
    )
  }

  test ("show(true)", P0Test, P1Test) {
    val baos = new ByteArrayOutputStream()
    Console.withOut(baos) {
      sqlContext.sql("""SELECT * FROM test_table where teststring is null or teststring not like 'Unicode%'""").show(true)
    }
    var expectedResult =
      """+--------+--------+----------+-------------------+---------+-------+----------------+---------+----------+-------------------+
        ||testbyte|testbool|  testdate|         testdouble|testfloat|testint|        testlong|testshort|teststring|      testtimestamp|
        |+--------+--------+----------+-------------------+---------+-------+----------------+---------+----------+-------------------+
        ||    null|    null|      null|               null|     null|   null|            null|     null|      null|               null|
        ||       0|    null|2015-07-03|                  0|       -1|4141214|1239012341823719|     null|         f|2015-07-03 12:34:56|
        ||       0|   false|      null|-1234152.1231249799|   100000|   null|1239012341823719|       24|  ___|_123|               null|
        ||       1|   false|2015-07-02|                  0|        0|     42|1239012341823719|      -13|      asdf|2015-07-02 00:00:00|
        |+--------+--------+----------+-------------------+---------+-------+----------------+---------+----------+-------------------+""".stripMargin
    // Adjust for null values being upper-case for Spark 3.5 or newer.
    if (sc.version.replace(".", "").toInt >= 350) {
      expectedResult = expectedResult.replace("null", "NULL")
    }
    checkResult(expectedResult, baos.toString)

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT ( CAST ( "SQ_1"."TESTBYTE" AS VARCHAR ) ) AS "SQ_2_COL_0",
         |( CASE "SQ_1"."TESTBOOL" WHEN TRUE THEN \\'true\\' WHEN FALSE THEN \\'false\\' ELSE null END ) AS "SQ_2_COL_1",
         |( CAST ( "SQ_1"."TESTDATE" AS VARCHAR ) ) AS "SQ_2_COL_2",
         |( CAST ( "SQ_1"."TESTDOUBLE" AS VARCHAR ) ) AS "SQ_2_COL_3",
         |( CAST ( "SQ_1"."TESTFLOAT" AS VARCHAR ) ) AS "SQ_2_COL_4",
         |( CAST ( "SQ_1"."TESTINT" AS VARCHAR ) ) AS "SQ_2_COL_5",
         |( CAST ( "SQ_1"."TESTLONG" AS VARCHAR ) ) AS "SQ_2_COL_6",
         |( CAST ( "SQ_1"."TESTSHORT" AS VARCHAR ) ) AS "SQ_2_COL_7",
         |( "SQ_1"."TESTSTRING" ) AS "SQ_2_COL_8",
         |( CAST ( "SQ_1"."TESTTIMESTAMP" AS VARCHAR ) ) AS "SQ_2_COL_9"
         | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0"
         | WHERE ( ( "SQ_0"."TESTSTRING" IS NULL ) OR NOT ( ( CAST ( "SQ_0"."TESTSTRING" AS VARCHAR ) LIKE CONCAT ( \\'Unicode\\' , \\'%\\' ) ) ) ) )
         | AS "SQ_1" ) AS "SQ_2" LIMIT 21""".stripMargin,
      s"""SELECT * FROM ( SELECT
         |( CASE WHEN "SQ_1"."TESTBYTE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTBYTE" AS VARCHAR ) END ) AS "SQ_2_COL_0",
         |( CASE WHEN "SQ_1"."TESTBOOL" IS NULL THEN \\'NULL\\' ELSE CASE "SQ_1"."TESTBOOL" WHEN TRUE THEN \\'true\\' WHEN FALSE THEN \\'false\\' ELSE null END END ) AS "SQ_2_COL_1",
         |( CASE WHEN "SQ_1"."TESTDATE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTDATE" AS VARCHAR ) END ) AS "SQ_2_COL_2",
         |( CASE WHEN "SQ_1"."TESTDOUBLE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTDOUBLE" AS VARCHAR ) END ) AS "SQ_2_COL_3",
         |( CASE WHEN "SQ_1"."TESTFLOAT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTFLOAT" AS VARCHAR ) END ) AS "SQ_2_COL_4",
         |( CASE WHEN "SQ_1"."TESTINT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTINT" AS VARCHAR ) END ) AS "SQ_2_COL_5",
         |( CASE WHEN "SQ_1"."TESTLONG" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTLONG" AS VARCHAR ) END ) AS "SQ_2_COL_6",
         |( CASE WHEN "SQ_1"."TESTSHORT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTSHORT" AS VARCHAR ) END ) AS "SQ_2_COL_7",
         |( CASE WHEN "SQ_1"."TESTSTRING" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTSTRING" AS VARCHAR ) END ) AS "SQ_2_COL_8",
         |( CASE WHEN "SQ_1"."TESTTIMESTAMP" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTTIMESTAMP" AS VARCHAR ) END ) AS "SQ_2_COL_9"
         | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0"
         | WHERE ( ( "SQ_0"."TESTSTRING" IS NULL ) OR NOT ( ( CAST ( "SQ_0"."TESTSTRING" AS VARCHAR ) LIKE CONCAT ( \\'Unicode\\', \\'%\\' ) ) ) ) )
         | AS "SQ_1" ) AS "SQ_2" LIMIT 21""".stripMargin
    )
  }

  test ("show(false)", P0Test, P1Test) {
    val baos = new ByteArrayOutputStream()
    Console.withOut(baos) {
      sqlContext.sql("""SELECT * FROM test_table where teststring is null or teststring not like 'Unicode%'""").show(false)
    }
    var expectedResult =
      """+--------+--------+----------+-------------------+---------+-------+----------------+---------+----------+-------------------+
        ||testbyte|testbool|testdate  |testdouble         |testfloat|testint|testlong        |testshort|teststring|testtimestamp      |
        |+--------+--------+----------+-------------------+---------+-------+----------------+---------+----------+-------------------+
        ||null    |null    |null      |null               |null     |null   |null            |null     |null      |null               |
        ||0       |null    |2015-07-03|0                  |-1       |4141214|1239012341823719|null     |f         |2015-07-03 12:34:56|
        ||0       |false   |null      |-1234152.1231249799|100000   |null   |1239012341823719|24       |___|_123  |null               |
        ||1       |false   |2015-07-02|0                  |0        |42     |1239012341823719|-13      |asdf      |2015-07-02 00:00:00|
        |+--------+--------+----------+-------------------+---------+-------+----------------+---------+----------+-------------------+""".stripMargin
    // Adjust for null values being upper-case for Spark 3.5 or newer.
    if (sc.version.replace(".", "").toInt >= 350) {
      expectedResult = expectedResult.replace("null", "NULL")
    }
    checkResult(expectedResult, baos.toString)

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT ( CAST ( "SQ_1"."TESTBYTE" AS VARCHAR ) ) AS "SQ_2_COL_0",
         |( CASE "SQ_1"."TESTBOOL" WHEN TRUE THEN \\'true\\' WHEN FALSE THEN \\'false\\' ELSE null END ) AS "SQ_2_COL_1",
         |( CAST ( "SQ_1"."TESTDATE" AS VARCHAR ) ) AS "SQ_2_COL_2",
         |( CAST ( "SQ_1"."TESTDOUBLE" AS VARCHAR ) ) AS "SQ_2_COL_3",
         |( CAST ( "SQ_1"."TESTFLOAT" AS VARCHAR ) ) AS "SQ_2_COL_4",
         |( CAST ( "SQ_1"."TESTINT" AS VARCHAR ) ) AS "SQ_2_COL_5",
         |( CAST ( "SQ_1"."TESTLONG" AS VARCHAR ) ) AS "SQ_2_COL_6",
         |( CAST ( "SQ_1"."TESTSHORT" AS VARCHAR ) ) AS "SQ_2_COL_7",
         |( "SQ_1"."TESTSTRING" ) AS "SQ_2_COL_8",
         |( CAST ( "SQ_1"."TESTTIMESTAMP" AS VARCHAR ) ) AS "SQ_2_COL_9"
         | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0"
         | WHERE ( ( "SQ_0"."TESTSTRING" IS NULL ) OR NOT ( ( CAST ( "SQ_0"."TESTSTRING" AS VARCHAR ) LIKE CONCAT ( \\'Unicode\\' , \\'%\\' ) ) ) ) )
         | AS "SQ_1" ) AS "SQ_2" LIMIT 21""".stripMargin,
      s"""SELECT * FROM ( SELECT
         |( CASE WHEN "SQ_1"."TESTBYTE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTBYTE" AS VARCHAR ) END ) AS "SQ_2_COL_0",
         |( CASE WHEN "SQ_1"."TESTBOOL" IS NULL THEN \\'NULL\\' ELSE CASE "SQ_1"."TESTBOOL" WHEN TRUE THEN \\'true\\' WHEN FALSE THEN \\'false\\' ELSE null END END ) AS "SQ_2_COL_1",
         |( CASE WHEN "SQ_1"."TESTDATE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTDATE" AS VARCHAR ) END ) AS "SQ_2_COL_2",
         |( CASE WHEN "SQ_1"."TESTDOUBLE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTDOUBLE" AS VARCHAR ) END ) AS "SQ_2_COL_3",
         |( CASE WHEN "SQ_1"."TESTFLOAT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTFLOAT" AS VARCHAR ) END ) AS "SQ_2_COL_4",
         |( CASE WHEN "SQ_1"."TESTINT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTINT" AS VARCHAR ) END ) AS "SQ_2_COL_5",
         |( CASE WHEN "SQ_1"."TESTLONG" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTLONG" AS VARCHAR ) END ) AS "SQ_2_COL_6",
         |( CASE WHEN "SQ_1"."TESTSHORT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTSHORT" AS VARCHAR ) END ) AS "SQ_2_COL_7",
         |( CASE WHEN "SQ_1"."TESTSTRING" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTSTRING" AS VARCHAR ) END ) AS "SQ_2_COL_8",
         |( CASE WHEN "SQ_1"."TESTTIMESTAMP" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_1"."TESTTIMESTAMP" AS VARCHAR ) END ) AS "SQ_2_COL_9"
         | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0"
         | WHERE ( ( "SQ_0"."TESTSTRING" IS NULL ) OR NOT ( ( CAST ( "SQ_0"."TESTSTRING" AS VARCHAR ) LIKE CONCAT ( \\'Unicode\\', \\'%\\' ) ) ) ) )
         | AS "SQ_1" ) AS "SQ_2" LIMIT 21""".stripMargin
    )
  }

  test ("show(3)", P0Test, P1Test) {
    val baos = new ByteArrayOutputStream()
    Console.withOut(baos) {
      sqlContext.sql("""SELECT * FROM test_table""").show(3)
    }
    var expectedResult =
      """+--------+--------+----------+-------------------+---------+-------+----------------+---------+----------+-------------------+
        ||testbyte|testbool|  testdate|         testdouble|testfloat|testint|        testlong|testshort|teststring|      testtimestamp|
        |+--------+--------+----------+-------------------+---------+-------+----------------+---------+----------+-------------------+
        ||    null|    null|      null|               null|     null|   null|            null|     null|      null|               null|
        ||       0|    null|2015-07-03|                  0|       -1|4141214|1239012341823719|     null|         f|2015-07-03 12:34:56|
        ||       0|   false|      null|-1234152.1231249799|   100000|   null|1239012341823719|       24|  ___|_123|               null|
        |+--------+--------+----------+-------------------+---------+-------+----------------+---------+----------+-------------------+
        |only showing top 3 rows""".stripMargin
    // Adjust for null values being upper-case for Spark 3.5 or newer.
    if (sc.version.replace(".", "").toInt >= 350) {
      expectedResult = expectedResult.replace("null", "NULL")
    }
    checkResult(expectedResult, baos.toString)

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT
         |( CAST( "SQ_0"."TESTBYTE" AS VARCHAR ) ) AS "SQ_1_COL_0",
         |( CASE "SQ_0"."TESTBOOL" WHEN TRUE THEN \\'true\\' WHEN FALSE THEN \\'false\\' ELSE null END ) AS "SQ_1_COL_1",
         |( CAST( "SQ_0"."TESTDATE" AS VARCHAR ) ) AS "SQ_1_COL_2",
         |( CAST( "SQ_0"."TESTDOUBLE" AS VARCHAR ) ) AS "SQ_1_COL_3",
         |( CAST( "SQ_0"."TESTFLOAT" AS VARCHAR ) ) AS "SQ_1_COL_4",
         |( CAST( "SQ_0"."TESTINT" AS VARCHAR ) ) AS "SQ_1_COL_5",
         |( CAST( "SQ_0"."TESTLONG" AS VARCHAR ) ) AS "SQ_1_COL_6",
         |( CAST( "SQ_0"."TESTSHORT" AS VARCHAR ) ) AS "SQ_1_COL_7",
         |( "SQ_0"."TESTSTRING") AS "SQ_1_COL_8",
         |( CAST( "SQ_0"."TESTTIMESTAMP" AS VARCHAR ) ) AS "SQ_1_COL_9"
         | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS") AS "SQ_0" )
         | AS "SQ_1" LIMIT 4""".stripMargin,
      s"""SELECT * FROM ( SELECT
         |( CASE WHEN "SQ_0"."TESTBYTE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTBYTE" AS VARCHAR ) END ) AS "SQ_1_COL_0",
         |( CASE WHEN "SQ_0"."TESTBOOL" IS NULL THEN \\'NULL\\' ELSE CASE "SQ_0"."TESTBOOL" WHEN TRUE THEN \\'true\\' WHEN FALSE THEN \\'false\\' ELSE null END END ) AS "SQ_1_COL_1",
         |( CASE WHEN "SQ_0"."TESTDATE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTDATE" AS VARCHAR ) END ) AS" SQ_1_COL_2",
         |( CASE WHEN "SQ_0"."TESTDOUBLE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTDOUBLE" AS VARCHAR ) END ) AS "SQ_1_COL_3",
         |( CASE WHEN "SQ_0"."TESTFLOAT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTFLOAT" AS VARCHAR ) END ) AS "SQ_1_COL_4",
         |( CASE WHEN "SQ_0"."TESTINT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTINT" AS VARCHAR ) END ) AS "SQ_1_COL_5",
         |( CASE WHEN "SQ_0"."TESTLONG" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTLONG" AS VARCHAR ) END ) AS "SQ_1_COL_6",
         |( CASE WHEN "SQ_0"."TESTSHORT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTSHORT" AS VARCHAR ) END ) AS "SQ_1_COL_7",
         |( CASE WHEN "SQ_0"."TESTSTRING" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTSTRING" AS VARCHAR ) END ) AS "SQ_1_COL_8",
         |( CASE WHEN "SQ_0"."TESTTIMESTAMP" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTTIMESTAMP" AS VARCHAR ) END ) AS "SQ_1_COL_9"
         | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
         | AS "SQ_1" LIMIT 4""".stripMargin
    )
  }

  test ("show(3, true)", P0Test, P1Test) {
    val baos = new ByteArrayOutputStream()
    Console.withOut(baos) {
      sqlContext.sql("""SELECT * FROM test_table""").show(3, true)
    }
    var expectedResult =
      """|+--------+--------+----------+-------------------+---------+-------+----------------+---------+----------+-------------------+
         ||testbyte|testbool|  testdate|         testdouble|testfloat|testint|        testlong|testshort|teststring|      testtimestamp|
         |+--------+--------+----------+-------------------+---------+-------+----------------+---------+----------+-------------------+
         ||    null|    null|      null|               null|     null|   null|            null|     null|      null|               null|
         ||       0|    null|2015-07-03|                  0|       -1|4141214|1239012341823719|     null|         f|2015-07-03 12:34:56|
         ||       0|   false|      null|-1234152.1231249799|   100000|   null|1239012341823719|       24|  ___|_123|               null|
         |+--------+--------+----------+-------------------+---------+-------+----------------+---------+----------+-------------------+
         |only showing top 3 rows""".stripMargin
    // Adjust for null values being upper-case for Spark 3.5 or newer.
    if (sc.version.replace(".", "").toInt >= 350) {
      expectedResult = expectedResult.replace("null", "NULL")
    }
    checkResult(expectedResult, baos.toString)

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT
         |( CAST( "SQ_0"."TESTBYTE" AS VARCHAR ) ) AS "SQ_1_COL_0",
         |( CASE "SQ_0"."TESTBOOL" WHEN TRUE THEN \\'true\\' WHEN FALSE THEN \\'false\\' ELSE null END ) AS "SQ_1_COL_1",
         |( CAST( "SQ_0"."TESTDATE" AS VARCHAR ) ) AS "SQ_1_COL_2",
         |( CAST( "SQ_0"."TESTDOUBLE" AS VARCHAR ) ) AS "SQ_1_COL_3",
         |( CAST( "SQ_0"."TESTFLOAT" AS VARCHAR ) ) AS "SQ_1_COL_4",
         |( CAST( "SQ_0"."TESTINT" AS VARCHAR ) ) AS "SQ_1_COL_5",
         |( CAST( "SQ_0"."TESTLONG" AS VARCHAR ) ) AS "SQ_1_COL_6",
         |( CAST( "SQ_0"."TESTSHORT" AS VARCHAR ) ) AS "SQ_1_COL_7",
         |( "SQ_0"."TESTSTRING") AS "SQ_1_COL_8",
         |( CAST( "SQ_0"."TESTTIMESTAMP" AS VARCHAR ) ) AS "SQ_1_COL_9"
         | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS") AS "SQ_0" )
         | AS "SQ_1" LIMIT 4""".stripMargin,
      s"""SELECT * FROM ( SELECT
         |( CASE WHEN "SQ_0"."TESTBYTE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTBYTE" AS VARCHAR ) END ) AS "SQ_1_COL_0",
         |( CASE WHEN "SQ_0"."TESTBOOL" IS NULL THEN \\'NULL\\' ELSE CASE "SQ_0"."TESTBOOL" WHEN TRUE THEN \\'true\\' WHEN FALSE THEN \\'false\\' ELSE null END END ) AS "SQ_1_COL_1",
         |( CASE WHEN "SQ_0"."TESTDATE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTDATE" AS VARCHAR ) END ) AS" SQ_1_COL_2",
         |( CASE WHEN "SQ_0"."TESTDOUBLE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTDOUBLE" AS VARCHAR ) END ) AS "SQ_1_COL_3",
         |( CASE WHEN "SQ_0"."TESTFLOAT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTFLOAT" AS VARCHAR ) END ) AS "SQ_1_COL_4",
         |( CASE WHEN "SQ_0"."TESTINT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTINT" AS VARCHAR ) END ) AS "SQ_1_COL_5",
         |( CASE WHEN "SQ_0"."TESTLONG" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTLONG" AS VARCHAR ) END ) AS "SQ_1_COL_6",
         |( CASE WHEN "SQ_0"."TESTSHORT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTSHORT" AS VARCHAR ) END ) AS "SQ_1_COL_7",
         |( CASE WHEN "SQ_0"."TESTSTRING" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTSTRING" AS VARCHAR ) END ) AS "SQ_1_COL_8",
         |( CASE WHEN "SQ_0"."TESTTIMESTAMP" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTTIMESTAMP" AS VARCHAR ) END ) AS "SQ_1_COL_9"
         | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
         | AS "SQ_1" LIMIT 4""".stripMargin
    )
  }

  test ("show(3, false)", P0Test, P1Test) {
    val baos = new ByteArrayOutputStream()
    Console.withOut(baos) {
      sqlContext.sql("""SELECT * FROM test_table""").show(3, false)
    }
    var expectedResult =
      """+--------+--------+----------+-------------------+---------+-------+----------------+---------+----------+-------------------+
        ||testbyte|testbool|testdate  |testdouble         |testfloat|testint|testlong        |testshort|teststring|testtimestamp      |
        |+--------+--------+----------+-------------------+---------+-------+----------------+---------+----------+-------------------+
        ||null    |null    |null      |null               |null     |null   |null            |null     |null      |null               |
        ||0       |null    |2015-07-03|0                  |-1       |4141214|1239012341823719|null     |f         |2015-07-03 12:34:56|
        ||0       |false   |null      |-1234152.1231249799|100000   |null   |1239012341823719|24       |___|_123  |null               |
        |+--------+--------+----------+-------------------+---------+-------+----------------+---------+----------+-------------------+
        |only showing top 3 rows""".stripMargin
    // Adjust for null values being upper-case for Spark 3.5 or newer.
    if (sc.version.replace(".", "").toInt >= 350) {
      expectedResult = expectedResult.replace("null", "NULL")
    }
    checkResult(expectedResult, baos.toString)

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT
         |( CAST( "SQ_0"."TESTBYTE" AS VARCHAR ) ) AS "SQ_1_COL_0",
         |( CASE "SQ_0"."TESTBOOL" WHEN TRUE THEN \\'true\\' WHEN FALSE THEN \\'false\\' ELSE null END ) AS "SQ_1_COL_1",
         |( CAST( "SQ_0"."TESTDATE" AS VARCHAR ) ) AS "SQ_1_COL_2",
         |( CAST( "SQ_0"."TESTDOUBLE" AS VARCHAR ) ) AS "SQ_1_COL_3",
         |( CAST( "SQ_0"."TESTFLOAT" AS VARCHAR ) ) AS "SQ_1_COL_4",
         |( CAST( "SQ_0"."TESTINT" AS VARCHAR ) ) AS "SQ_1_COL_5",
         |( CAST( "SQ_0"."TESTLONG" AS VARCHAR ) ) AS "SQ_1_COL_6",
         |( CAST( "SQ_0"."TESTSHORT" AS VARCHAR ) ) AS "SQ_1_COL_7",
         |( "SQ_0"."TESTSTRING") AS "SQ_1_COL_8",
         |( CAST( "SQ_0"."TESTTIMESTAMP" AS VARCHAR ) ) AS "SQ_1_COL_9"
         | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS") AS "SQ_0" )
         | AS "SQ_1" LIMIT 4""".stripMargin,
      s"""SELECT * FROM ( SELECT
         |( CASE WHEN "SQ_0"."TESTBYTE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTBYTE" AS VARCHAR ) END ) AS "SQ_1_COL_0",
         |( CASE WHEN "SQ_0"."TESTBOOL" IS NULL THEN \\'NULL\\' ELSE CASE "SQ_0"."TESTBOOL" WHEN TRUE THEN \\'true\\' WHEN FALSE THEN \\'false\\' ELSE null END END ) AS "SQ_1_COL_1",
         |( CASE WHEN "SQ_0"."TESTDATE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTDATE" AS VARCHAR ) END ) AS" SQ_1_COL_2",
         |( CASE WHEN "SQ_0"."TESTDOUBLE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTDOUBLE" AS VARCHAR ) END ) AS "SQ_1_COL_3",
         |( CASE WHEN "SQ_0"."TESTFLOAT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTFLOAT" AS VARCHAR ) END ) AS "SQ_1_COL_4",
         |( CASE WHEN "SQ_0"."TESTINT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTINT" AS VARCHAR ) END ) AS "SQ_1_COL_5",
         |( CASE WHEN "SQ_0"."TESTLONG" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTLONG" AS VARCHAR ) END ) AS "SQ_1_COL_6",
         |( CASE WHEN "SQ_0"."TESTSHORT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTSHORT" AS VARCHAR ) END ) AS "SQ_1_COL_7",
         |( CASE WHEN "SQ_0"."TESTSTRING" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTSTRING" AS VARCHAR ) END ) AS "SQ_1_COL_8",
         |( CASE WHEN "SQ_0"."TESTTIMESTAMP" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTTIMESTAMP" AS VARCHAR ) END ) AS "SQ_1_COL_9"
         | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
         | AS "SQ_1" LIMIT 4""".stripMargin
    )
  }

  test ("show(20, 10)", P0Test, P1Test) {
    val baos = new ByteArrayOutputStream()
    Console.withOut(baos) {
      sqlContext.sql("""SELECT * FROM test_table""").show(20, 10)
    }
    var expectedResult =
      s"""+--------+--------+----------+----------+---------+-------+----------+---------+----------+-------------+
         ||testbyte|testbool|  testdate|testdouble|testfloat|testint|  testlong|testshort|teststring|testtimestamp|
         |+--------+--------+----------+----------+---------+-------+----------+---------+----------+-------------+
         ||    null|    null|      null|      null|     null|   null|      null|     null|      null|         null|
         ||       0|    null|2015-07-03|         0|       -1|4141214|1239012...|     null|         f|   2015-07...|
         ||       0|   false|      null|-123415...|   100000|   null|1239012...|       24|  ___|_123|         null|
         ||       1|   false|2015-07-02|         0|        0|     42|1239012...|      -13|      asdf|   2015-07...|
         ||       1|    true|2015-07-01|1234152...|        1|     42|1239012...|       23|Unicode...|   2015-07...|
         |+--------+--------+----------+----------+---------+-------+----------+---------+----------+-------------+
         |
         |""".stripMargin
    // Adjust for null values being upper-case for Spark 3.5 or newer.
    if (sc.version.replace(".", "").toInt >= 350) {
      expectedResult = expectedResult.replace("null", "NULL")
    }
    checkResult(expectedResult, baos.toString)

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT
         |( CAST( "SQ_0"."TESTBYTE" AS VARCHAR ) ) AS "SQ_1_COL_0",
         |( CASE "SQ_0"."TESTBOOL" WHEN TRUE THEN \\'true\\' WHEN FALSE THEN \\'false\\' ELSE null END ) AS "SQ_1_COL_1",
         |( CAST( "SQ_0"."TESTDATE" AS VARCHAR ) ) AS "SQ_1_COL_2",
         |( CAST( "SQ_0"."TESTDOUBLE" AS VARCHAR ) ) AS "SQ_1_COL_3",
         |( CAST( "SQ_0"."TESTFLOAT" AS VARCHAR ) ) AS "SQ_1_COL_4",
         |( CAST( "SQ_0"."TESTINT" AS VARCHAR ) ) AS "SQ_1_COL_5",
         |( CAST( "SQ_0"."TESTLONG" AS VARCHAR ) ) AS "SQ_1_COL_6",
         |( CAST( "SQ_0"."TESTSHORT" AS VARCHAR ) ) AS "SQ_1_COL_7",
         |( "SQ_0"."TESTSTRING") AS "SQ_1_COL_8",
         |( CAST( "SQ_0"."TESTTIMESTAMP" AS VARCHAR ) ) AS "SQ_1_COL_9"
         | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS") AS "SQ_0" )
         | AS "SQ_1" LIMIT 21""".stripMargin,
      s"""SELECT * FROM ( SELECT
         |( CASE WHEN "SQ_0"."TESTBYTE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTBYTE" AS VARCHAR ) END ) AS "SQ_1_COL_0",
         |( CASE WHEN "SQ_0"."TESTBOOL" IS NULL THEN \\'NULL\\' ELSE CASE "SQ_0"."TESTBOOL" WHEN TRUE THEN \\'true\\' WHEN FALSE THEN \\'false\\' ELSE null END END ) AS "SQ_1_COL_1",
         |( CASE WHEN "SQ_0"."TESTDATE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTDATE" AS VARCHAR ) END ) AS" SQ_1_COL_2",
         |( CASE WHEN "SQ_0"."TESTDOUBLE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTDOUBLE" AS VARCHAR ) END ) AS "SQ_1_COL_3",
         |( CASE WHEN "SQ_0"."TESTFLOAT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTFLOAT" AS VARCHAR ) END ) AS "SQ_1_COL_4",
         |( CASE WHEN "SQ_0"."TESTINT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTINT" AS VARCHAR ) END ) AS "SQ_1_COL_5",
         |( CASE WHEN "SQ_0"."TESTLONG" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTLONG" AS VARCHAR ) END ) AS "SQ_1_COL_6",
         |( CASE WHEN "SQ_0"."TESTSHORT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTSHORT" AS VARCHAR ) END ) AS "SQ_1_COL_7",
         |( CASE WHEN "SQ_0"."TESTSTRING" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTSTRING" AS VARCHAR ) END ) AS "SQ_1_COL_8",
         |( CASE WHEN "SQ_0"."TESTTIMESTAMP" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTTIMESTAMP" AS VARCHAR ) END ) AS "SQ_1_COL_9"
         | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
         | AS "SQ_1" LIMIT 21""".stripMargin
    )
  }

  test ("show(20, 10, true)", P0Test, P1Test) {
    val baos = new ByteArrayOutputStream()
    Console.withOut(baos) {
      sqlContext.sql("""SELECT * FROM test_table""").show(20, 10, true)
    }
    var expectedResult =
      s"""-RECORD 0-------------------${""}
         | testbyte      | null       ${""}
         | testbool      | null       ${""}
         | testdate      | null       ${""}
         | testdouble    | null       ${""}
         | testfloat     | null       ${""}
         | testint       | null       ${""}
         | testlong      | null       ${""}
         | testshort     | null       ${""}
         | teststring    | null       ${""}
         | testtimestamp | null       ${""}
         |-RECORD 1-------------------${""}
         | testbyte      | 0          ${""}
         | testbool      | null       ${""}
         | testdate      | 2015-07-03 ${""}
         | testdouble    | 0          ${""}
         | testfloat     | -1         ${""}
         | testint       | 4141214    ${""}
         | testlong      | 1239012... ${""}
         | testshort     | null       ${""}
         | teststring    | f          ${""}
         | testtimestamp | 2015-07... ${""}
         |-RECORD 2-------------------${""}
         | testbyte      | 0          ${""}
         | testbool      | false      ${""}
         | testdate      | null       ${""}
         | testdouble    | -123415... ${""}
         | testfloat     | 100000     ${""}
         | testint       | null       ${""}
         | testlong      | 1239012... ${""}
         | testshort     | 24         ${""}
         | teststring    | ___|_123   ${""}
         | testtimestamp | null       ${""}
         |-RECORD 3-------------------${""}
         | testbyte      | 1          ${""}
         | testbool      | false      ${""}
         | testdate      | 2015-07-02 ${""}
         | testdouble    | 0          ${""}
         | testfloat     | 0          ${""}
         | testint       | 42         ${""}
         | testlong      | 1239012... ${""}
         | testshort     | -13        ${""}
         | teststring    | asdf       ${""}
         | testtimestamp | 2015-07... ${""}
         |-RECORD 4-------------------${""}
         | testbyte      | 1          ${""}
         | testbool      | true       ${""}
         | testdate      | 2015-07-01 ${""}
         | testdouble    | 1234152... ${""}
         | testfloat     | 1          ${""}
         | testint       | 42         ${""}
         | testlong      | 1239012... ${""}
         | testshort     | 23         ${""}
         | teststring    | Unicode... ${""}
         | testtimestamp | 2015-07... ${""}""".stripMargin
    // Adjust for null values being upper-case for Spark 3.5 or newer.
    if (sc.version.replace(".", "").toInt >= 350) {
      expectedResult = expectedResult.replace("null", "NULL")
    }
    checkResult(expectedResult, baos.toString)

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT
         |( CAST( "SQ_0"."TESTBYTE" AS VARCHAR ) ) AS "SQ_1_COL_0",
         |( CASE "SQ_0"."TESTBOOL" WHEN TRUE THEN \\'true\\' WHEN FALSE THEN \\'false\\' ELSE null END ) AS "SQ_1_COL_1",
         |( CAST( "SQ_0"."TESTDATE" AS VARCHAR ) ) AS "SQ_1_COL_2",
         |( CAST( "SQ_0"."TESTDOUBLE" AS VARCHAR ) ) AS "SQ_1_COL_3",
         |( CAST( "SQ_0"."TESTFLOAT" AS VARCHAR ) ) AS "SQ_1_COL_4",
         |( CAST( "SQ_0"."TESTINT" AS VARCHAR ) ) AS "SQ_1_COL_5",
         |( CAST( "SQ_0"."TESTLONG" AS VARCHAR ) ) AS "SQ_1_COL_6",
         |( CAST( "SQ_0"."TESTSHORT" AS VARCHAR ) ) AS "SQ_1_COL_7",
         |( "SQ_0"."TESTSTRING") AS "SQ_1_COL_8",
         |( CAST( "SQ_0"."TESTTIMESTAMP" AS VARCHAR ) ) AS "SQ_1_COL_9"
         | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS") AS "SQ_0" )
         | AS "SQ_1" LIMIT 21""".stripMargin,
      s"""SELECT * FROM ( SELECT
         |( CASE WHEN "SQ_0"."TESTBYTE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTBYTE" AS VARCHAR ) END ) AS "SQ_1_COL_0",
         |( CASE WHEN "SQ_0"."TESTBOOL" IS NULL THEN \\'NULL\\' ELSE CASE "SQ_0"."TESTBOOL" WHEN TRUE THEN \\'true\\' WHEN FALSE THEN \\'false\\' ELSE null END END ) AS "SQ_1_COL_1",
         |( CASE WHEN "SQ_0"."TESTDATE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTDATE" AS VARCHAR ) END ) AS" SQ_1_COL_2",
         |( CASE WHEN "SQ_0"."TESTDOUBLE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTDOUBLE" AS VARCHAR ) END ) AS "SQ_1_COL_3",
         |( CASE WHEN "SQ_0"."TESTFLOAT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTFLOAT" AS VARCHAR ) END ) AS "SQ_1_COL_4",
         |( CASE WHEN "SQ_0"."TESTINT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTINT" AS VARCHAR ) END ) AS "SQ_1_COL_5",
         |( CASE WHEN "SQ_0"."TESTLONG" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTLONG" AS VARCHAR ) END ) AS "SQ_1_COL_6",
         |( CASE WHEN "SQ_0"."TESTSHORT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTSHORT" AS VARCHAR ) END ) AS "SQ_1_COL_7",
         |( CASE WHEN "SQ_0"."TESTSTRING" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTSTRING" AS VARCHAR ) END ) AS "SQ_1_COL_8",
         |( CASE WHEN "SQ_0"."TESTTIMESTAMP" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTTIMESTAMP" AS VARCHAR ) END ) AS "SQ_1_COL_9"
         | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
         | AS "SQ_1" LIMIT 21""".stripMargin
    )
  }

  test ("show(20, 10, false)", P0Test, P1Test) {
    val baos = new ByteArrayOutputStream()
    Console.withOut(baos) {
      sqlContext.sql("""SELECT * FROM test_table""").show(20, 10, false)
    }
    var expectedResult =
      s"""+--------+--------+----------+----------+---------+-------+----------+---------+----------+-------------+
         ||testbyte|testbool|  testdate|testdouble|testfloat|testint|  testlong|testshort|teststring|testtimestamp|
         |+--------+--------+----------+----------+---------+-------+----------+---------+----------+-------------+
         ||    null|    null|      null|      null|     null|   null|      null|     null|      null|         null|
         ||       0|    null|2015-07-03|         0|       -1|4141214|1239012...|     null|         f|   2015-07...|
         ||       0|   false|      null|-123415...|   100000|   null|1239012...|       24|  ___|_123|         null|
         ||       1|   false|2015-07-02|         0|        0|     42|1239012...|      -13|      asdf|   2015-07...|
         ||       1|    true|2015-07-01|1234152...|        1|     42|1239012...|       23|Unicode...|   2015-07...|
         |+--------+--------+----------+----------+---------+-------+----------+---------+----------+-------------+""".stripMargin
    // Adjust for null values being upper-case for Spark 3.5 or newer.
    if (sc.version.replace(".", "").toInt >= 350) {
      expectedResult = expectedResult.replace("null", "NULL")
    }
    checkResult(expectedResult, baos.toString)

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT
         |( CAST( "SQ_0"."TESTBYTE" AS VARCHAR ) ) AS "SQ_1_COL_0",
         |( CASE "SQ_0"."TESTBOOL" WHEN TRUE THEN \\'true\\' WHEN FALSE THEN \\'false\\' ELSE null END ) AS "SQ_1_COL_1",
         |( CAST( "SQ_0"."TESTDATE" AS VARCHAR ) ) AS "SQ_1_COL_2",
         |( CAST( "SQ_0"."TESTDOUBLE" AS VARCHAR ) ) AS "SQ_1_COL_3",
         |( CAST( "SQ_0"."TESTFLOAT" AS VARCHAR ) ) AS "SQ_1_COL_4",
         |( CAST( "SQ_0"."TESTINT" AS VARCHAR ) ) AS "SQ_1_COL_5",
         |( CAST( "SQ_0"."TESTLONG" AS VARCHAR ) ) AS "SQ_1_COL_6",
         |( CAST( "SQ_0"."TESTSHORT" AS VARCHAR ) ) AS "SQ_1_COL_7",
         |( "SQ_0"."TESTSTRING") AS "SQ_1_COL_8",
         |( CAST( "SQ_0"."TESTTIMESTAMP" AS VARCHAR ) ) AS "SQ_1_COL_9"
         | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS") AS "SQ_0" )
         | AS "SQ_1" LIMIT 21""".stripMargin,
      s"""SELECT * FROM ( SELECT
         |( CASE WHEN "SQ_0"."TESTBYTE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTBYTE" AS VARCHAR ) END ) AS "SQ_1_COL_0",
         |( CASE WHEN "SQ_0"."TESTBOOL" IS NULL THEN \\'NULL\\' ELSE CASE "SQ_0"."TESTBOOL" WHEN TRUE THEN \\'true\\' WHEN FALSE THEN \\'false\\' ELSE null END END ) AS "SQ_1_COL_1",
         |( CASE WHEN "SQ_0"."TESTDATE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTDATE" AS VARCHAR ) END ) AS" SQ_1_COL_2",
         |( CASE WHEN "SQ_0"."TESTDOUBLE" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTDOUBLE" AS VARCHAR ) END ) AS "SQ_1_COL_3",
         |( CASE WHEN "SQ_0"."TESTFLOAT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTFLOAT" AS VARCHAR ) END ) AS "SQ_1_COL_4",
         |( CASE WHEN "SQ_0"."TESTINT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTINT" AS VARCHAR ) END ) AS "SQ_1_COL_5",
         |( CASE WHEN "SQ_0"."TESTLONG" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTLONG" AS VARCHAR ) END ) AS "SQ_1_COL_6",
         |( CASE WHEN "SQ_0"."TESTSHORT" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTSHORT" AS VARCHAR ) END ) AS "SQ_1_COL_7",
         |( CASE WHEN "SQ_0"."TESTSTRING" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTSTRING" AS VARCHAR ) END ) AS "SQ_1_COL_8",
         |( CASE WHEN "SQ_0"."TESTTIMESTAMP" IS NULL THEN \\'NULL\\' ELSE CAST ( "SQ_0"."TESTTIMESTAMP" AS VARCHAR ) END ) AS "SQ_1_COL_9"
         | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
         | AS "SQ_1" LIMIT 21""".stripMargin
    )
  }
}

class TextPushdownShowSuite extends PushdownOnlyShowSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "true"
}

class ParquetPushdownShowSuite extends PushdownOnlyShowSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "true"
}

class TextNoCachePushdownShowSuite extends TextPushdownShowSuite {
  override protected val s3_result_cache = "false"
}

class ParquetNoCachePushdownShowSuite extends ParquetPushdownShowSuite {
  override protected val s3_result_cache = "false"
}
