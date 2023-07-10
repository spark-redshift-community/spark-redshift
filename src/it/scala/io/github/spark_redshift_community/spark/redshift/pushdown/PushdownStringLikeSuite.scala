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

abstract class PushdownStringLikeSuite extends StringIntegrationPushdownSuiteBase {

  test("Like tests") {
    // Column names
    val columns = List("testfixedstring", "testvarstring")
    // (id, pattern, result)
    val paramTuples = List(
      (0, "%", null),
      (1, "Hello World%", true),
      (1, "______World%", true),
      (1, "______W0rld%", false),
      (1, "%World%", true),
      (1, "Hello%", true),
      (1, "Hel%rld%", true),
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

    columns.foreach(column => {
      paramTuples.foreach(paramTuple => {
        val id = paramTuple._1
        val pattern = paramTuple._2
        val result = paramTuple._3

        checkAnswer(
          sqlContext.sql(
            s"""SELECT LIKE($column, '$pattern') FROM test_table WHERE testid=$id""".stripMargin),
          Seq(Row(result)))

        checkSqlStatement(
          s"""SELECT ( "SUBQUERY_1"."${column.toUpperCase}" LIKE \\'$pattern\\' )
             | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
             | $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
             | AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND
             | ( "SUBQUERY_0"."TESTID" = $id ) ) ) AS "SUBQUERY_1"""".stripMargin)
      })
    })
  }
}

@DoNotDiscover // Disable until the following SIM is fixed: [Redshift-7150]
class DefaultStringLikePushdownSuite extends PushdownStringLikeSuite {
  override protected val s3format: String = "DEFAULT"
  override protected val auto_pushdown: String = "true"
}

@DoNotDiscover // Disable until the following SIM is fixed: [Redshift-7150]
class ParquetStringLikePushdownSuite extends PushdownStringLikeSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "true"
}

class DefaultNoPushdownStringLikeSuite extends PushdownStringLikeSuite {
  override protected val s3format: String = "DEFAULT"
  override protected val auto_pushdown: String = "false"
}

class ParquetNoPushdownStringLikeSuite extends PushdownStringLikeSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}
