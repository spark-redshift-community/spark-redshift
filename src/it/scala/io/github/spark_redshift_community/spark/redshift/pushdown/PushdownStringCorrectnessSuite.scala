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
    doTest(sqlContext, testTrim00)
    doTest(sqlContext, testTrim01)
    doTest(sqlContext, testTrim02)
    doTest(sqlContext, testTrim03)
    doTest(sqlContext, testTrim04)
    doTest(sqlContext, testTrim05)
    doTest(sqlContext, testTrim06)
    doTest(sqlContext, testTrim07)
    doTest(sqlContext, testTrim08)
    doTest(sqlContext, testTrim09)
    doTest(sqlContext, testTrim10)
    doTest(sqlContext, testTrim11)
    doTest(sqlContext, testTrim12)
    doTest(sqlContext, testTrim13)
    doTest(sqlContext, testTrim14)
    doTest(sqlContext, testTrim15)
    doTest(sqlContext, testTrim16)
    doTest(sqlContext, testTrim17)
    doTest(sqlContext, testTrim18)
    doTest(sqlContext, testTrim19)
    doTest(sqlContext, testTrim20)
    doTest(sqlContext, testTrim21)
    doTest(sqlContext, testTrim22)
    doTest(sqlContext, testTrim23)
    doTest(sqlContext, testTrim24)
    doTest(sqlContext, testTrim25)
    doTest(sqlContext, testTrim26)
    doTest(sqlContext, testTrim27)
    doTest(sqlContext, testTrim28)
    doTest(sqlContext, testTrim29)
    doTest(sqlContext, testTrim30)
    doTest(sqlContext, testTrim31)
    doTest(sqlContext, testTrim32)
    doTest(sqlContext, testTrim33)
    doTest(sqlContext, testTrim34)
    doTest(sqlContext, testTrim35)
    doTest(sqlContext, testTrim36)
  }

  // Define length tests
  test("Test Length statements against correctness dataset") {
    doTest(sqlContext, testLength00)
    doTest(sqlContext, testLength01)
    doTest(sqlContext, testLength02)
    doTest(sqlContext, testLength03)
    doTest(sqlContext, testLength04)
    doTest(sqlContext, testLength05)
    doTest(sqlContext, testLength06)
    doTest(sqlContext, testLength07)
    doTest(sqlContext, testLength08)
    doTest(sqlContext, testLength09)
    doTest(sqlContext, testLength10)
    doTest(sqlContext, testLength11)
    doTest(sqlContext, testLength12)
    doTest(sqlContext, testLength13)
  }

  // Define ascii tests
  test("Test Ascii statements against correctness dataset") {
    doTest(sqlContext, testAscii00)
    doTest(sqlContext, testAscii01)
    doTest(sqlContext, testAscii02)
    doTest(sqlContext, testAscii03)
    doTest(sqlContext, testAscii04)
    doTest(sqlContext, testAscii05)
    doTest(sqlContext, testAscii06)
    doTest(sqlContext, testAscii07)
    doTest(sqlContext, testAscii08)
    doTest(sqlContext, testAscii09)
  }

  // Define upper tests
  test("Test Upper statements against correctness dataset") {
    doTest(sqlContext, testUpper00)
    doTest(sqlContext, testUpper01)
    doTest(sqlContext, testUpper02)
    doTest(sqlContext, testUpper03)
    doTest(sqlContext, testUpper04)
    doTest(sqlContext, testUpper05)
    doTest(sqlContext, testUpper06)
    doTest(sqlContext, testUpper07)
    doTest(sqlContext, testUpper08)
    doTest(sqlContext, testUpper09)
    doTest(sqlContext, testUpper10)
    doTest(sqlContext, testUpper11)
  }

  // Define lower tests
  test("Test Lower statements against correctness dataset") {
    doTest(sqlContext, testLower00)
    doTest(sqlContext, testLower01)
    doTest(sqlContext, testLower02)
    doTest(sqlContext, testLower03)
    doTest(sqlContext, testLower04)
    doTest(sqlContext, testLower05)
    doTest(sqlContext, testLower06)
    doTest(sqlContext, testLower07)
    doTest(sqlContext, testLower08)
    doTest(sqlContext, testLower09)
    doTest(sqlContext, testLower10)
    doTest(sqlContext, testLower11)
  }

  // Define substring tests
  test("Test Substring statements against correctness dataset") {
    doTest(sqlContext, testSubstr00)
    doTest(sqlContext, testSubstr01)
    doTest(sqlContext, testSubstr02)
    doTest(sqlContext, testSubstr03)
    doTest(sqlContext, testSubstr04)
    doTest(sqlContext, testSubstr05)
    doTest(sqlContext, testSubstr06)
    doTest(sqlContext, testSubstr07)
    doTest(sqlContext, testSubstr08)
    doTest(sqlContext, testSubstr09)
    doTest(sqlContext, testSubstr10)
    doTest(sqlContext, testSubstr11)
    doTest(sqlContext, testSubstr12)
    doTest(sqlContext, testSubstr13)
    doTest(sqlContext, testSubstr14)
    doTest(sqlContext, testSubstr15)
    doTest(sqlContext, testSubstr16)
    doTest(sqlContext, testSubstr17)
    doTest(sqlContext, testSubstr18)
    doTest(sqlContext, testSubstr19)
    doTest(sqlContext, testSubstr20)
    doTest(sqlContext, testSubstr21)
    doTest(sqlContext, testSubstr22)
    doTest(sqlContext, testSubstr23)
    doTest(sqlContext, testSubstr24)
    doTest(sqlContext, testSubstr25)
    doTest(sqlContext, testSubstr26)
    doTest(sqlContext, testSubstr27)
    doTest(sqlContext, testSubstr28)
    doTest(sqlContext, testSubstr29)
    doTest(sqlContext, testSubstr30)
    doTest(sqlContext, testSubstr31)
    doTest(sqlContext, testSubstr32)
    doTest(sqlContext, testSubstr33)
    doTest(sqlContext, testSubstr34)
    doTest(sqlContext, testSubstr35)
    doTest(sqlContext, testSubstr36)
    doTest(sqlContext, testSubstr37)
    doTest(sqlContext, testSubstr38)
    doTest(sqlContext, testSubstr39)
    doTest(sqlContext, testSubstr40)
    doTest(sqlContext, testSubstr41)
    doTest(sqlContext, testSubstr42)
    doTest(sqlContext, testSubstr43)
    doTest(sqlContext, testSubstr44)
    doTest(sqlContext, testSubstr45)
    doTest(sqlContext, testSubstr46)
  }
}

class DefaultPushdownStringCorrectnessSuite extends PushdownStringCorrectnessSuite {
  override protected val s3format: String = "DEFAULT"
  override protected val auto_pushdown: String = "true"
}

class ParquetPushdownStringCorrectnessSuite extends PushdownStringCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "true"
}

class DefaultNoPushdownStringCorrectnessSuite extends PushdownStringCorrectnessSuite {
  override protected val s3format: String = "DEFAULT"
  override protected val auto_pushdown: String = "false"
}

class ParquetNoPushdownStringCorrectnessSuite extends PushdownStringCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}
