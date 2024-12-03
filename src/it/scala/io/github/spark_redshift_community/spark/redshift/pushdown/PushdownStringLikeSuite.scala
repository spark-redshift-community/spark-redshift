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

abstract class PushdownStringLikeSuite extends StringIntegrationPushdownSuiteBase {

  test("Like tests", P1Test) {
    // Column names
    val columns = List("testfixedstring", "testvarstring")
    // (id, pattern, result)
    val paramTuples = List(
      (0, "%", null),
      (1, "%Hello World", true),
      (1, "Hello World%", true),
      (1, "%Hello World%", true),
      (1, "Hello%World", true),
      (1, "%Hello%World%", true),
      (1, "______World", true),
      (1, "______W0rld", false),
      (1, "%World", true),
      (1, "Hello%", true),
      (1, "Hel%rld", true),
      (2, "Controls\t \b%", true),
      (3, "Specials/%%", true),
      (4, "%Byte_Chars%", true),
      (5, "%", true),
      (5, "_", false),
      (6, "  Hello_World%", true),
      (7, "  \t__Foo%", true),
      (8, "  /%Foo%/%", true),
      (9, "  %i%", true)
    )

    columns.par.foreach(column => {
      paramTuples.foreach(paramTuple => {
        val id = paramTuple._1
        val pattern = paramTuple._2
        val result = paramTuple._3

        // Test normal input strings.
        checkAnswer(
          sqlContext.sql(
            s"""SELECT LIKE($column, '$pattern') FROM test_table WHERE testid=$id""".stripMargin),
          Seq(Row(result)))

        // Test input strings as nested expressions.
        checkAnswer(
          sqlContext.sql(
            s"""SELECT LIKE((SELECT $column FROM test_table WHERE testid=$id), '$pattern')
               | FROM test_table WHERE testid=$id""".stripMargin),
          Seq(Row(result)))

        // Don't bother checking the SQL statement as Spark can optimize these queries
        // using several different variations. We will check the push-down SQL in other tests.
      })
    })
  }

  test("Boolean Like Tests", P1Test) {
    // Column names
    val columns = List("testfixedstring", "testvarstring")
    // (id, pattern, result)
    val patterns = List(
      "%Hello World",
      "Hello World%",
      "%Hello World%"
    )

    val expectedSqlPattern = List(
      """CONCAT(\'%\', \'Hello World\')""",
      """CONCAT(\'Hello World\', \'%\')""",
      """CONCAT(\'%\', CONCAT(\'Hello World\', \'%\'))"""
    )

    columns.par.foreach(column => {
      patterns.zip(expectedSqlPattern).foreach({case (pattern, sqlPattern) =>
        checkAnswer(
          sqlContext.sql(
            s"""SELECT LIKE($column, '$pattern') FROM test_table
               | WHERE testid=1""".stripMargin),
          Seq(Row(true)))

        checkSqlStatement(
          s"""SELECT ( ( CAST ( "SQ_1"."${column.toUpperCase}" AS VARCHAR
             | ) LIKE $sqlPattern ) ) AS "SQ_2_COL_0" FROM ( SELECT *
             | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
             | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
             | ( "SQ_0"."TESTID" = 1 ) ) ) AS "SQ_1"""".stripMargin)
      })
    })
  }

  test("Boolean Split Tests", P1Test) {
    val columns = List("testfixedstring", "testvarstring")
    columns.par.foreach(column => {
      checkAnswer(
        sqlContext.sql(
          s"""SELECT LIKE($column, 'Hello%World') FROM test_table
             | WHERE testid=1""".stripMargin),
        Seq(Row(true)))

      checkSqlStatement(
        s"""SELECT ( ( ( LENGTH ( "SQ_1"."${column.toUpperCase}" ) >= 10 ) AND (
           | ( CAST ( "SQ_1"."${column.toUpperCase}" AS VARCHAR )
           | LIKE CONCAT(\\'Hello\\', \\'%\\') ) AND
           | ( CAST ( "SQ_1"."${column.toUpperCase}" AS VARCHAR )
           | LIKE CONCAT(\\'%\\', \\'World\\' ) ) ) ) )
           | AS "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table AS
           | "RCQ_ALIAS" ) AS "SQ_0" WHERE ( ( "SQ_0"."TESTID"
           | IS NOT NULL ) AND ( "SQ_0"."TESTID" = 1 ) ) ) AS "SQ_1"""".stripMargin)
    })
  }

  test("String Like tests", P1Test) {
    val columns = List("testfixedstring", "testvarstring")
    columns.par.foreach(column => {
      checkAnswer(
        sqlContext.sql(
          s"""SELECT LIKE($column, '%Hello%World%') FROM test_table
             | WHERE testid=1""".stripMargin),
        Seq(Row(true)))

      checkSqlStatement(
        s"""SELECT ( CAST ( "SQ_1"."${column.toUpperCase}" AS VARCHAR )
           | LIKE \\'%Hello%World%\\' ) AS "SQ_2_COL_0" FROM ( SELECT *
           | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           | AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL ) AND
           | ( "SQ_0"."TESTID" = 1 ) ) ) AS "SQ_1"""".stripMargin)
    })
  }
}

class TextPushdownStringLikeSuite extends PushdownStringLikeSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "true"
}

class ParquetPushdownStringLikeSuite extends PushdownStringLikeSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "true"
}

class TextNoPushdownStringLikeSuite extends PushdownStringLikeSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "false"
}

class ParquetNoPushdownStringLikeSuite extends PushdownStringLikeSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}

class TextNoCachePushdownStringLikeSuite
  extends TextPushdownStringLikeSuite {
  override protected val s3_result_cache = "false"
}

class ParquetNoCachePushdownStringLikeSuite
  extends ParquetPushdownStringLikeSuite {
  override protected val s3_result_cache = "false"
}
