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

package io.github.spark_redshift_community.spark.redshift.test
import io.github.spark_redshift_community.spark.redshift.ComparableVersion


import org.scalatest.funsuite.AnyFunSuite

class ComparableVersionsSuite extends AnyFunSuite {

  test("ComparableVersion - one part") {
    val version = ComparableVersion("3")

    assert(version.lessThanOrEqualTo("4"))
    assert(version.lessThanOrEqualTo("3"))
    assert(!version.lessThanOrEqualTo("2"))
    assert(version.lessThanOrEqualTo("4.0"))
    assert(version.lessThanOrEqualTo("3.0"))
    assert(!version.lessThanOrEqualTo("2.9"))
    assert(version.lessThanOrEqualTo("3.0.1"))
    assert(version.lessThanOrEqualTo("3.0.0"))
    assert(!version.lessThanOrEqualTo("2.9.9"))

    assert(version.lessThan("4"))
    assert(!version.lessThan("3"))
    assert(!version.lessThan("2"))
    assert(version.lessThan("4.0"))
    assert(version.lessThan("3.1"))
    assert(!version.lessThan("3.0"))
    assert(!version.lessThan("2.9"))
    assert(version.lessThan("3.0.1"))
    assert(!version.lessThan("3.0.0"))
    assert(!version.lessThan("2.9.9"))

    assert(version.equalTo("3"))
    assert(!version.equalTo("4"))
    assert(version.equalTo("3.0"))
    assert(!version.equalTo("3.1"))
    assert(version.equalTo("3.0.0"))
    assert(!version.equalTo("3.0.1"))

    assert(version.greaterThan("2"))
    assert(!version.greaterThan("3"))
    assert(!version.greaterThan("4"))
    assert(version.greaterThan("2.9"))
    assert(!version.greaterThan("3.0"))
    assert(!version.greaterThan("3.1"))
    assert(version.greaterThan("2.9.9"))
    assert(!version.greaterThan("3.0.0"))

    assert(version.greaterThanOrEqualTo("2"))
    assert(version.greaterThanOrEqualTo("3"))
    assert(version.greaterThanOrEqualTo("2.9"))
    assert(version.greaterThanOrEqualTo("3.0"))
    assert(!version.greaterThanOrEqualTo("3.1"))
    assert(version.greaterThanOrEqualTo("2.9.9"))
    assert(version.greaterThanOrEqualTo("3.0.0"))
    assert(!version.greaterThanOrEqualTo("3.0.1"))
  }

  test("ComparableVersion - two part") {
    val version = ComparableVersion("3.2")

    assert(version.lessThanOrEqualTo("4"))
    assert(!version.lessThanOrEqualTo("3"))
    assert(!version.lessThanOrEqualTo("2"))
    assert(version.lessThanOrEqualTo("4.0"))
    assert(version.lessThanOrEqualTo("3.2"))
    assert(!version.lessThanOrEqualTo("3.1"))
    assert(version.lessThanOrEqualTo("3.2.1"))
    assert(version.lessThanOrEqualTo("3.2.0"))
    assert(!version.lessThanOrEqualTo("3.1.9"))

    assert(version.lessThan("4"))
    assert(!version.lessThan("3"))
    assert(!version.lessThan("2"))
    assert(version.lessThan("4.0"))
    assert(version.lessThan("3.3"))
    assert(!version.lessThan("3.2"))
    assert(version.lessThan("3.2.1"))
    assert(!version.lessThan("3.2.0"))

    assert(version.equalTo("3.2"))
    assert(!version.equalTo("3.3"))
    assert(version.equalTo("3.2.0"))
    assert(!version.equalTo("3.2.1"))

    assert(version.greaterThan("2"))
    assert(version.greaterThan("3"))
    assert(!version.greaterThan("4"))
    assert(version.greaterThan("3.1"))
    assert(!version.greaterThan("3.2"))
    assert(!version.greaterThan("3.3"))
    assert(version.greaterThan("3.1.9"))
    assert(!version.greaterThan("3.2.0"))

    assert(version.greaterThanOrEqualTo("2"))
    assert(version.greaterThanOrEqualTo("3"))
    assert(version.greaterThanOrEqualTo("3.1"))
    assert(version.greaterThanOrEqualTo("3.2"))
    assert(!version.greaterThanOrEqualTo("3.3"))
    assert(version.greaterThanOrEqualTo("3.1.9"))
    assert(version.greaterThanOrEqualTo("3.2.0"))
    assert(!version.greaterThanOrEqualTo("3.2.1"))
  }

  test("ComparableVersion - three part") {
    val version = ComparableVersion("3.2.1")

    assert(version.lessThanOrEqualTo("4"))
    assert(!version.lessThanOrEqualTo("3"))
    assert(version.lessThanOrEqualTo("3.3"))
    assert(!version.lessThanOrEqualTo("3.2"))
    assert(version.lessThanOrEqualTo("3.2.2"))
    assert(version.lessThanOrEqualTo("3.2.1"))

    assert(version.lessThan("4"))
    assert(!version.lessThan("3"))
    assert(version.lessThan("3.3"))
    assert(!version.lessThan("3.2"))
    assert(version.lessThan("3.2.2"))
    assert(!version.lessThan("3.2.1"))

    assert(version.equalTo("3.2.1"))
    assert(!version.equalTo("4"))
    assert(!version.equalTo("3"))

    assert(version.greaterThanOrEqualTo("3"))
    assert(!version.greaterThanOrEqualTo("4"))
    assert(version.greaterThanOrEqualTo("3.2"))
    assert(!version.greaterThanOrEqualTo("3.3"))
    assert(version.greaterThanOrEqualTo("3.2.0"))
    assert(version.greaterThanOrEqualTo("3.2.1"))
    assert(!version.greaterThanOrEqualTo("3.2.2"))

    assert(version.greaterThan("3"))
    assert(!version.greaterThan("4"))
    assert(version.greaterThan("3.2"))
    assert(!version.greaterThan("3.3"))
    assert(version.greaterThan("3.2.0"))
    assert(!version.greaterThan("3.2.1"))
    assert(!version.greaterThan("3.2.2"))
  }
}
