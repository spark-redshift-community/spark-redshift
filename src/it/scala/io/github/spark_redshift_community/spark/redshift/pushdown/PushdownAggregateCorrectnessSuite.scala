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

import org.scalatest.DoNotDiscover


abstract class PushdownAggregateCorrectnessSuite extends AggregateCountCorrectnessSuite
                                                 with AggregateMaxCorrectnessSuite {
  override protected val preloaded_data: String = "true"
  override def setTestTableName(): String = """"PUBLIC"."all_shapes_dist_all_sort_compound_12col""""

  // define a test for count(*)
  test("Test COUNT(*) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount00)
    doTest(sqlContext, testCount01)
  }

  // define a test for count(short)
  test("Test COUNT(short) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount10)
    doTest(sqlContext, testCount10_2)
    doTest(sqlContext, testCount11)
    doTest(sqlContext, testCount12)
    doTest(sqlContext, testCount13)
    doTest(sqlContext, testCount14)
    doTest(sqlContext, testCount15)
    doTest(sqlContext, testCount16)
    // group by
    doTest(sqlContext, testCount17)
    doTest(sqlContext, testCount17_2)
  }

  // define a test for count(int)
  test("Test COUNT(int) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount20)
    doTest(sqlContext, testCount20_2)
    doTest(sqlContext, testCount21)
    doTest(sqlContext, testCount21_2)
    doTest(sqlContext, testCount22)
    doTest(sqlContext, testCount22_2)
    doTest(sqlContext, testCount23)
    doTest(sqlContext, testCount23_2)
    doTest(sqlContext, testCount24)
    doTest(sqlContext, testCount25)
    doTest(sqlContext, testCount26)
    // group by
    doTest(sqlContext, testCount27)
    doTest(sqlContext, testCount27_2)
  }

  // define a test for count(long)
  test("Test COUNT(long) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount30)
    doTest(sqlContext, testCount31)
    doTest(sqlContext, testCount32)
    doTest(sqlContext, testCount33)
    doTest(sqlContext, testCount34)
    doTest(sqlContext, testCount35)
    doTest(sqlContext, testCount36)
    // group by
    doTest(sqlContext, testCount37)
    doTest(sqlContext, testCount37_2)
  }

  // define a test for count(decimal)
  test("Test COUNT(decimal) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount40)
    doTest(sqlContext, testCount41)
    doTest(sqlContext, testCount42)
    doTest(sqlContext, testCount43)
    doTest(sqlContext, testCount44)
    doTest(sqlContext, testCount45)
    doTest(sqlContext, testCount46)
    doTest(sqlContext, testCount47)
    doTest(sqlContext, testCount48)
    // group by
    doTest(sqlContext, testCount49)
  }

  // define a test for count(float)
  test("Test COUNT(float) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount50)
    doTest(sqlContext, testCount51)
    doTest(sqlContext, testCount52)
    doTest(sqlContext, testCount53)
    doTest(sqlContext, testCount54)
    doTest(sqlContext, testCount55)
    doTest(sqlContext, testCount56)
    doTest(sqlContext, testCount57)
    // group by
    doTest(sqlContext, testCount58)
  }

  // define a test for count(boolean)
  test("Test COUNT(boolean) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount60)
    doTest(sqlContext, testCount61)
    doTest(sqlContext, testCount62)
    // group by
    doTest(sqlContext, testCount63)
    doTest(sqlContext, testCount63_2)
    doTest(sqlContext, testCount64)
    doTest(sqlContext, testCount64_2)
  }

  // define a test for count(char)
  test("Test COUNT(char) aggregation statements against correctness dataset") {
    // doTest(sqlContext, testCount70)
    // doTest(sqlContext, testCount71)
    // doTest(sqlContext, testCount72)
    // doTest(sqlContext, testCount73)
    // group by
    doTest(sqlContext, testCount74)
  }

  // define a test for count(varchar)
  test("Test COUNT(varchar) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount80)
    doTest(sqlContext, testCount81)
    doTest(sqlContext, testCount82)
    doTest(sqlContext, testCount83)
    // group by
    doTest(sqlContext, testCount84)
  }

  // define a test for count(date)
  test("Test COUNT(date) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount90)
    doTest(sqlContext, testCount91)
    doTest(sqlContext, testCount92)
    doTest(sqlContext, testCount93)
    doTest(sqlContext, testCount94)
    doTest(sqlContext, testCount95)
    doTest(sqlContext, testCount96)
    // group by
    doTest(sqlContext, testCount97)
  }

  // define a test for count(timestamp)
  test("Test COUNT(timestamp) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount100)
    doTest(sqlContext, testCount101)
    doTest(sqlContext, testCount102)
    doTest(sqlContext, testCount103)
    doTest(sqlContext, testCount104)
    doTest(sqlContext, testCount105)
    doTest(sqlContext, testCount106)
    // group by
    doTest(sqlContext, testCount107)
  }

  // define a test for count(timestamptz)
  test("Test COUNT(timestamptz) aggregation statements against correctness dataset") {
    doTest(sqlContext, testCount110)
    doTest(sqlContext, testCount111)
    doTest(sqlContext, testCount112)
    doTest(sqlContext, testCount113)
    doTest(sqlContext, testCount114)
    doTest(sqlContext, testCount115)
    doTest(sqlContext, testCount116)
  }

  // define a test for max(short)
  test("Test MAX(short) aggregation statements against correctness dataset") {
    doTest(sqlContext, testMax00)
    doTest(sqlContext, testMax01)
    doTest(sqlContext, testMax02)
    doTest(sqlContext, testMax03)
    doTest(sqlContext, testMax04)
    doTest(sqlContext, testMax05)
    doTest(sqlContext, testMax06)
    // group by
    doTest(sqlContext, testMax07)
  }
}

// Plz comment out this tag when you have set up test framework and want to run this against it
@DoNotDiscover
class DefaultPushdownAggregateCorrectnessSuite extends PushdownAggregateCorrectnessSuite {
  override protected val s3format: String = "DEFAULT"
  // Timestamptz column is not handled correctly in parquet format as time zone information is not
  // unloaded.
  test("Test COUNT(timestamptz) aggregation statements against correctness dataset with group by") {
    // group by
    doTest(sqlContext, testCount117)
  }
}

@DoNotDiscover
class ParquetPushdownAggregateCorrectnessSuite extends PushdownAggregateCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
}

@DoNotDiscover
class DefaultNoPushdownAggregateCorrectnessSuite extends PushdownAggregateCorrectnessSuite {
  override protected val auto_pushdown: String = "false"
  override protected val s3format: String = "DEFAULT"
  // define a test for count(timestamptz) with group by
  test("Test COUNT(timestamptz) aggregation statements against correctness dataset with group by") {
    // group by
    doTest(sqlContext, testCount117)
  }
}