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

abstract class StringSelectCorrectnessSuite extends StringIntegrationPushdownSuiteBase {

  test("Select StringType Column") {
    // (id, column, result)
    val paramTuples = List(
      (0, "testfixedstring", null),
      (0, "testvarstring", null),
      (1, "testfixedstring", "Hello World"),
      (1, "testvarstring", "Hello World"),
      (2, "testfixedstring", "Controls\t \b\n\r\f\\'\""),
      (2, "testvarstring", "Controls\t \b\n\r\f\\'\""),
      (3, "testfixedstring", "Specials/%"),
      (3, "testvarstring", "Specials/%"),
      (4, "testfixedstring", "Singl_Byte_Chars" ),
      (4, "testvarstring", "Multi樂Byte趣Chars"),
      (5, "testfixedstring", ""),
      (5, "testvarstring", ""),
      (6, "testfixedstring", "  Hello World"),
      (6, "testvarstring", "  Hello World  "),
      (7, "testfixedstring", "  \t\b\nFoo\r\f\\'\""),
      (7, "testvarstring", "  \t\b\nFoo\r\f\\'\"  "),
      (8, "testfixedstring", "  /%Foo%/"),
      (8, "testvarstring", "  /%Foo%/  "),
      (9, "testfixedstring", "  _Single_"),
      (9, "testvarstring", "  樂Multi趣  ")
    )

    paramTuples.foreach(paramTuple => {
      val id = paramTuple._1
      val column = paramTuple._2
      val result = paramTuple._3

      checkAnswer(
        sqlContext.sql(
          s"""SELECT $column FROM test_table WHERE testid=$id""".stripMargin),
        Seq(Row(result)))
    })
  }
}

class TextStringSelectCorrectnessSuite extends StringSelectCorrectnessSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "true"
}

class ParquetStringSelectCorrectnessSuite extends StringSelectCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "true"
}

class TextNoPushdownStringSelectCorrectnessSuite extends StringSelectCorrectnessSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "false"
}

class ParquetNoPushdownStringSelectCorrectnessSuite extends StringSelectCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}

class TextPushdownNoCacheStringSelectCorrectnessSuite
  extends TextStringSelectCorrectnessSuite {
  override protected val s3_result_cache = "false"
}

class ParquetPushdownNoCacheStringSelectCorrectnessSuite
  extends ParquetStringSelectCorrectnessSuite {
  override protected val s3_result_cache = "false"
}
