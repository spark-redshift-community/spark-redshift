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

abstract class PushdownStringCorrectnessSuite extends StringSubstringCorrectnessSuite
                                              with StringAsciiCorrectnessSuite
                                              with StringLengthCorrectnessSuite
                                              with StringTrimCorrectnessSuite
                                              with StringLowerCorrectnessSuite
                                              with StringUpperCorrectnessSuite {
  // Define trim tests
  test("Test Trim statements against correctness dataset") {
    val cases = Seq(
    testTrim00,
    testTrim01,
    testTrim02,
    testTrim03,
    testTrim04,
    testTrim05,
    testTrim06,
    testTrim07,
    testTrim08,
    testTrim09,
    testTrim10,
    testTrim11,
    testTrim12,
    testTrim13,
    testTrim14,
    testTrim15,
    testTrim16,
    testTrim17,
    testTrim18,
    testTrim19,
    testTrim20,
    testTrim21,
    testTrim22,
    testTrim23,
    testTrim24,
    testTrim25,
    testTrim26,
    testTrim27,
    testTrim28,
    testTrim29,
    testTrim30,
    testTrim31,
    testTrim32,
    testTrim33,
    testTrim34,
    testTrim35,
    testTrim36)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // Define length tests
  test("Test Length statements against correctness dataset") {
    val cases = Seq(
    testLength00,
    testLength01,
    testLength02,
    testLength03,
    testLength04,
    testLength05,
    testLength06,
    testLength07,
    testLength08,
    testLength09,
    testLength10,
    testLength11,
    testLength12,
    testLength13)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // Define ascii tests
  test("Test Ascii statements against correctness dataset") {
    val cases = Seq(
    testAscii00,
    testAscii01,
    testAscii02,
    testAscii03,
    testAscii04,
    testAscii05,
    testAscii06,
    testAscii07,
    testAscii08,
    testAscii09)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // Define upper tests
  test("Test Upper statements against correctness dataset") {
    val cases = Seq(
    testUpper00,
    testUpper01,
    testUpper02,
    testUpper03,
    testUpper04,
    testUpper05,
    testUpper06,
    testUpper07,
    testUpper08,
    testUpper09,
    testUpper10,
    testUpper11)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // Define lower tests
  test("Test Lower statements against correctness dataset") {
    val cases = Seq(
    testLower00,
    testLower01,
    testLower02,
    testLower03,
    testLower04,
    testLower05,
    testLower06,
    testLower07,
    testLower08,
    testLower09,
    testLower10,
    testLower11)

    cases.par.foreach { doTest(sqlContext, _) }
  }

  // Define substring tests
  test("Test Substring statements against correctness dataset") {
    val cases = Seq(
    testSubstr00,
    testSubstr01,
    testSubstr02,
    testSubstr03,
    testSubstr04,
    testSubstr05,
    testSubstr06,
    testSubstr07,
    testSubstr08,
    testSubstr09,
    testSubstr10,
    testSubstr11,
    testSubstr12,
    testSubstr13,
    testSubstr14,
    testSubstr15,
    testSubstr16,
    testSubstr17,
    testSubstr18,
    testSubstr19,
    testSubstr20,
    testSubstr21,
    testSubstr22,
    testSubstr23,
    testSubstr24,
    testSubstr25,
    testSubstr26,
    testSubstr27,
    testSubstr28,
    testSubstr29,
    testSubstr30,
    testSubstr31,
    testSubstr32,
    testSubstr33,
    testSubstr34,
    testSubstr35,
    testSubstr36,
    testSubstr37,
    testSubstr38,
    testSubstr39,
    testSubstr40,
    testSubstr41,
    testSubstr42,
    testSubstr43,
    testSubstr44,
    testSubstr45,
    testSubstr46)

    cases.par.foreach { doTest(sqlContext, _) }
  }
}

class TextPushdownStringCorrectnessSuite extends PushdownStringCorrectnessSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "true"
}

class ParquetPushdownStringCorrectnessSuite extends PushdownStringCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "true"
}

class TextNoPushdownStringCorrectnessSuite extends PushdownStringCorrectnessSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "false"
}

class ParquetNoPushdownStringCorrectnessSuite extends PushdownStringCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}

class TextPushdownNoCacheStringCorrectnessSuite
  extends TextPushdownStringCorrectnessSuite {
  override protected val s3_result_cache = "false"
}

class ParquetPushdownNoCacheStringCorrectnessSuite
  extends ParquetPushdownStringCorrectnessSuite {
  override protected val s3_result_cache = "false"
}
