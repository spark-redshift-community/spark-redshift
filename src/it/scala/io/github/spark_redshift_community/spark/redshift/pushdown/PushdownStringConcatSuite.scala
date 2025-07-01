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

abstract class PushdownStringConcatSuite extends StringIntegrationPushdownSuiteBase {

  test("Concat same column", P1Test) {
    // (id, column, result)
    val paramTuples = List(
      (0, "testfixedstring", null),
      (0, "testvarstring", null),
      (1, "testfixedstring", "Hello WorldHello World"),
      (1, "testvarstring", "Hello WorldHello World"),
      (2, "testfixedstring", "Controls\t \b\n\r\f\\'\"Controls\t \b\n\r\f\\'\""),
      (2, "testvarstring", "Controls\t \b\n\r\f\\'\"Controls\t \b\n\r\f\\'\""),
      (3, "testfixedstring", "Specials/%Specials/%"),
      (3, "testvarstring", "Specials/%Specials/%"),
      (4, "testfixedstring", "Singl_Byte_CharsSingl_Byte_Chars" ),
      (4, "testvarstring", "Multi樂Byte趣CharsMulti樂Byte趣Chars"),
      (5, "testfixedstring", ""),
      (5, "testvarstring", ""),
      (6, "testfixedstring", "  Hello World  Hello World"),
      (6, "testvarstring", "  Hello World    Hello World  "),
      (7, "testfixedstring", "  \t\b\nFoo\r\f\\'\"  \t\b\nFoo\r\f\\'\""),
      (7, "testvarstring", "  \t\b\nFoo\r\f\\'\"    \t\b\nFoo\r\f\\'\"  "),
      (8, "testfixedstring", "  /%Foo%/  /%Foo%/"),
      (8, "testvarstring", "  /%Foo%/    /%Foo%/  "),
      (9, "testfixedstring", "  _Single_  _Single_"),
      (9, "testvarstring", "  樂Multi趣    樂Multi趣  ")
    )

    paramTuples.par.foreach(paramTuple => {
      val id = paramTuple._1
      val column = paramTuple._2
      val result = paramTuple._3

      checkAnswer(
        sqlContext.sql(
          s"""SELECT CONCAT($column, $column) FROM test_table WHERE testid=$id""".stripMargin),
        Seq(Row(result)))

      checkSqlStatement(
        s"""SELECT ( CONCAT (
          | "SQ_1"."${column.toUpperCase}" , "SQ_1"."${column.toUpperCase}" ) )
          | AS "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table AS
          | "RCQ_ALIAS" ) AS "SQ_0" WHERE ( ( "SQ_0"."TESTID"
          | IS NOT NULL ) AND ( "SQ_0"."TESTID" = $id ) ) ) AS "SQ_1"""".stripMargin)

    })
  }

  test("Concat different columns", P1Test) {
    // (column1, column2)
    var columnTuples = List(("testfixedstring", "testvarstring"))
    // (id, result)
    val paramTuples = List(
      (0, null),
      (1, "Hello WorldHello World"),
      (2, "Controls\t \b\n\r\f\\'\"Controls\t \b\n\r\f\\'\""),
      (3, "Specials/%Specials/%"),
      (4, "Singl_Byte_CharsMulti樂Byte趣Chars"),
      (5, ""),
      (6, "  Hello World  Hello World  "),
      (7, "  \t\b\nFoo\r\f\\'\"  \t\b\nFoo\r\f\\'\"  "),
      (8, "  /%Foo%/  /%Foo%/  "),
      (9, "  _Single_  樂Multi趣  ")
    )

    columnTuples.par.foreach(columnTuple => {
      val column1 = columnTuple._1
      val column2 = columnTuple._2

      paramTuples.foreach(paramTuple => {
        val id = paramTuple._1
        val result = paramTuple._2

        checkAnswer(
          sqlContext.sql(
            s"""SELECT CONCAT($column1, $column2) FROM test_table WHERE testid=$id""".stripMargin),
          Seq(Row(result)))

        checkSqlStatement(
          s"""SELECT ( CONCAT (
             | "SQ_1"."${column1.toUpperCase}" , "SQ_1"."${column2.toUpperCase}" ) )
             | AS "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table AS
             | "RCQ_ALIAS" ) AS "SQ_0" WHERE ( ( "SQ_0"."TESTID"
             | IS NOT NULL ) AND ( "SQ_0"."TESTID" = $id ) ) ) AS "SQ_1"""".stripMargin)

      })
    })
  }

  test("Concat different columns and literal", P1Test) {
    // (column1, column2)
    var columnTuples = List(("testfixedstring", "testvarstring"))
    // (id, literal, result)
    val paramTuples = List(
      (1, "", "Hello WorldHello World"),
      (1, "Hi!", "Hello WorldHello WorldHi!"),
      (1, " \t\b\n\f\"", "Hello WorldHello World \t\b\n\f\""),
      (1, "/%", "Hello WorldHello World/%"),
      (1, "樂趣", "Hello WorldHello World樂趣")
    )

    columnTuples.par.foreach(columnTuple => {
      val column1 = columnTuple._1
      val column2 = columnTuple._2

      paramTuples.foreach(paramTuple => {
        val id = paramTuple._1
        val literal = paramTuple._2
        val result = paramTuple._3

        checkAnswer(
          sqlContext.sql(
            s"""SELECT CONCAT($column1, $column2, '$literal') FROM test_table
              | WHERE testid=$id""".stripMargin),
          Seq(Row(result)))

        checkSqlStatement(
          s"""SELECT ( CONCAT ( "SQ_1"."${column1.toUpperCase}" ,
             | CONCAT ( "SQ_1"."${column2.toUpperCase}" , \\'$literal\\' ) ) )
             | AS "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table AS
             | "RCQ_ALIAS" ) AS "SQ_0" WHERE ( ( "SQ_0"."TESTID" IS NOT NULL )
             | AND ( "SQ_0"."TESTID" = $id ) ) ) AS "SQ_1"""".stripMargin)

      })
    })
  }
}

class TextPushdownStringConcatSuite extends PushdownStringConcatSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "true"
}

class ParquetPushdownStringConcatSuite extends PushdownStringConcatSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "true"
}

class TextNoPushdownStringConcatSuite extends PushdownStringConcatSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "false"
}

class ParquetNoPushdownStringConcatSuite extends PushdownStringConcatSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}

class TextNoCachePushdownStringConcatSuite
  extends TextPushdownStringConcatSuite {
  override protected val s3_result_cache = "false"
}

class ParquetNoCachePushdownStringConcatSuite
  extends ParquetPushdownStringConcatSuite {
  override protected val s3_result_cache = "false"
}
