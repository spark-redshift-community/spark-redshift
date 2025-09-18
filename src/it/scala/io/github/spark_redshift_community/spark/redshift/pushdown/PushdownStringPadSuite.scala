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

abstract class PushdownStringPadSuite extends StringIntegrationPushdownSuiteBase {

  test("Left Pad without pad", P1Test) {
    // Column names
    val columns = List("testfixedstring", "testvarstring")
    // (id, length, result)
    val paramTuples = List(
      (0, 3, null),
      (0, -3, null),
      (1, 13, "  Hello World"),
      (1, 5, "Hello"),
      (1, -5, "")
    )

    columns.par.foreach(column => {
      paramTuples.foreach(paramTuple => {
        val id = paramTuple._1
        val length = paramTuple._2
        val result = paramTuple._3

        checkAnswer(
          sqlContext.sql(
            s"""SELECT LPAD($column, $length) FROM test_table
              | WHERE testid=${id}""".stripMargin),
          Seq(Row(result)))

        checkSqlStatement(
          s"""SELECT ( LPAD ( "SQ_1"."${column.toUpperCase}" , $length , \\' \\' ) ) AS
             | "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table AS
             | "RCQ_ALIAS" ) AS "SQ_0" WHERE ( ( "SQ_0"."TESTID"
             | IS NOT NULL ) AND ( "SQ_0"."TESTID" = $id ) ) ) AS
             | "SQ_1"""".stripMargin)
      })
    })
  }

  test("Left Pad with pad", P1Test) {
    // Column
    val columns = List("testfixedstring", "testvarstring")
    // (id, length, pad, result)
    val paramTuples = List(
      (0, 3, "A", null),
      (1, 13, "A", "AAHello World"),
      (1, 15, "AB", "ABABHello World"),
      (1, 16, "AB", "ABABAHello World"),
      (1, 16, "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "ABCDEHello World"),
      (1, 15, "\t\r\n\"", "\t\r\n\"Hello World"),
      (1, 16, "/%", "/%/%/Hello World"),
      (1, 19, "樂A趣", "樂A趣樂A趣樂AHello World"),
      (5, 9, "ABC", "ABCABCABC")
    )
    columns.par.foreach(column => {
      paramTuples.foreach(paramTuple => {
        val id = paramTuple._1
        val length = paramTuple._2
        val pad = paramTuple._3
        val result = paramTuple._4
        checkAnswer(
          sqlContext.sql(
            s"""SELECT LPAD($column, $length, '$pad') FROM test_table
              | WHERE testid=$id""".stripMargin),
          Seq(Row(result)))

        checkSqlStatement(
          s"""SELECT ( LPAD ( "SQ_1"."${column.toUpperCase}" , $length , \\'$pad\\' ) ) AS
             | "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table AS
             | "RCQ_ALIAS" ) AS "SQ_0" WHERE ( ( "SQ_0"."TESTID"
             | IS NOT NULL ) AND ( "SQ_0"."TESTID" = $id ) ) ) AS
             | "SQ_1"""".stripMargin)
      })
    })
  }

  test("Right Pad without pad", P1Test) {
    // Column names
    val columns = List(/* "testfixedstring", */ "testvarstring")
    // (id, length, result)
    val paramTuples = List(
      (0, 3, null),
      (0, -3, null),
      (1, 13, "Hello World  "),
      (1, 5, "Hello"),
      (1, -5, "")
    )

    columns.par.foreach(column => {
      paramTuples.foreach(paramTuple => {
        val id = paramTuple._1
        val length = paramTuple._2
        val result = paramTuple._3

        checkAnswer(
          sqlContext.sql(
            s"""SELECT RPAD($column, $length) FROM test_table
               | WHERE testid=${id}""".stripMargin),
          Seq(Row(result)))

        checkSqlStatement(
          s"""SELECT ( RPAD ( "SQ_1"."${column.toUpperCase}" , $length , \\' \\' ) ) AS
             | "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table AS
             | "RCQ_ALIAS" ) AS "SQ_0" WHERE ( ( "SQ_0"."TESTID"
             | IS NOT NULL ) AND ( "SQ_0"."TESTID" = $id ) ) ) AS
             | "SQ_1"""".stripMargin)
      })
    })
  }

  test("Right Pad with pad", P1Test) {
    // Column
    val columns = List("testfixedstring", "testvarstring")
    // (id, length, pad, result)
    val paramTuples = List(
      (0, 3, "A", null),
      (1, 13, "A", "Hello WorldAA"),
      (1, 15, "AB", "Hello WorldABAB"),
      (1, 16, "AB", "Hello WorldABABA"),
      (1, 16, "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "Hello WorldABCDE"),
      (1, 15, "\t\r\n\"", "Hello World\t\r\n\""),
      (1, 16, "/%", "Hello World/%/%/"),
      (1, 19, "樂A趣", "Hello World樂A趣樂A趣樂A"),
      (5, 9, "ABC", "ABCABCABC")
    )
    columns.par.foreach(column => {
      paramTuples.foreach(paramTuple => {
        val id = paramTuple._1
        val length = paramTuple._2
        val pad = paramTuple._3
        val result = paramTuple._4
        checkAnswer(
          sqlContext.sql(
            s"""SELECT RPAD($column, $length, '$pad') FROM test_table
               | WHERE testid=$id""".stripMargin),
          Seq(Row(result)))

        checkSqlStatement(
          s"""SELECT ( RPAD ( "SQ_1"."${column.toUpperCase}" , $length , \\'$pad\\' ) ) AS
             | "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table AS
             | "RCQ_ALIAS" ) AS "SQ_0" WHERE ( ( "SQ_0"."TESTID"
             | IS NOT NULL ) AND ( "SQ_0"."TESTID" = $id ) ) ) AS
             | "SQ_1"""".stripMargin)
      })
    })
  }
}

class TextPushdownStringPadSuite extends PushdownStringPadSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "true"
}

class ParquetPushdownStringPadSuite extends PushdownStringPadSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "true"
}

class TextNoPushdownStringPadSuite extends PushdownStringPadSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "false"
}

class ParquetNoPushdownStringPadSuite extends PushdownStringPadSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}

class TextNoCachePushdownStringPadSuite
  extends TextPushdownStringPadSuite {
  override protected val s3_result_cache = "false"
}

class ParquetNoCachePushdownStringPadSuite
  extends ParquetPushdownStringPadSuite {
  override protected val s3_result_cache = "false"
}
