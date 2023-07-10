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
import org.scalatest.DoNotDiscover

abstract class PushdownStringTranslateSuite extends StringIntegrationPushdownSuiteBase {

  test("Translate Single-Byte Characters") {
    // Column names
    val columns = List("testfixedstring", "testvarstring")
    // (id, from, to, result)
    val paramTuples = List(
      (0, "ABC", "DEF", null),
      (1, "loz", "LOZ", "HeLLO WOrLd"),
      (1, "loz", "", "He Wrd"),
      (1, "HZW", "hz", "hello orld"),
      (2, "\t \b\n\r\f\"", "", "Controls\\'"),
      (3, "/%", "%/", "Specials%/"),
      (5, "ABCabc", "abcABC", ""),
      (6, " ", "", "HelloWorld"),
      (6, "o ", "/", "Hell/W/rld"),
      (6, "o ", "\"", "Hell\"W\"rld"),
      (7, "\t\b\n\r\f ", "", "Foo\\\'\""),
      (8, "/%o ", "_!\n", "_!F\n\n!_")
    )

    columns.foreach(column => {
      paramTuples.foreach(paramTuple => {
        val id = paramTuple._1
        val from = paramTuple._2
        val to = paramTuple._3
        val result = paramTuple._4

        checkAnswer(
          sqlContext.sql(
            s"""SELECT TRANSLATE($column, '$from', '$to') FROM test_table
               | WHERE testid=${id}""".stripMargin),
          Seq(Row(result)))

        checkSqlStatement(
          s"""SELECT ( TRANSLATE ( "SUBQUERY_1"."${column.toUpperCase}" ,
             | \\'$from\\' , \\'$to\\' ) ) AS
             | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table AS
             | "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID"
             | IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = $id ) ) ) AS
             | "SUBQUERY_1"""".stripMargin)
      })
    })
  }

 test("Translate Multi-Byte Characters") {
    // Column names
    val columns = List("testvarstring")
    // (id, from, to, result)
    val paramTuples = List(
      (4, "樂趣", "", "MultiByteChars"),
      (4, "樂i趣s", "趣I樂S", "MultI趣Byte樂CharS"),
      (6, " ", "樂", "樂樂Hello樂World樂樂"),
      (9, "樂趣 ", "趣樂", "趣Multi樂")
    )

    columns.foreach(column => {
      paramTuples.foreach(paramTuple => {
        val id = paramTuple._1
        val from = paramTuple._2
        val to = paramTuple._3
        val result = paramTuple._4

        checkAnswer(
          sqlContext.sql(
            s"""SELECT TRANSLATE($column, '$from', '$to') FROM test_table
               | WHERE testid=${id}""".stripMargin),
          Seq(Row(result)))

        checkSqlStatement(
          s"""SELECT ( TRANSLATE ( "SUBQUERY_1"."${column.toUpperCase}" ,
             | \\'$from\\' , \\'$to\\' ) ) AS
             | "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table AS
             | "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID"
             | IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = $id ) ) ) AS
             | "SUBQUERY_1"""".stripMargin)
      })
    })
  }
}

class DefaultStringTranslatePushdownSuite extends PushdownStringTranslateSuite {
  override protected val s3format: String = "DEFAULT"
  override protected val auto_pushdown: String = "true"
}

class ParquetStringTranslatePushdownSuite extends PushdownStringTranslateSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "true"
}

class DefaultNoPushdownStringTranslateSuite extends PushdownStringTranslateSuite {
  override protected val s3format: String = "DEFAULT"
  override protected val auto_pushdown: String = "false"
}

class ParquetNoPushdownStringTranslateSuite extends PushdownStringTranslateSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}
