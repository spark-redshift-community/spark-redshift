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
package io.github.spark_redshift_community.spark.redshift

import com.fasterxml.jackson.core.Version

private[redshift] case class ComparableVersion(strVersion: String) {
  private val version: Version = parseVersion(strVersion)

  private def parseVersion(strVersion: String): Version = {
    assert(strVersion != null && strVersion.nonEmpty)
    val versionComponents = strVersion.split('.')
    new Version(
      if (versionComponents.length > 0) versionComponents(0).toInt else 0,
      if (versionComponents.length > 1) versionComponents(1).toInt else 0,
      if (versionComponents.length > 2) versionComponents(2).toInt else 0,
      null, null, null)
  }

  def lessThan(strOtherVersion: String): Boolean = {
    val otherVersion = parseVersion(strOtherVersion)
    version.compareTo(otherVersion) < 0
  }

  def lessThanOrEqualTo(strOtherVersion: String): Boolean = {
    val otherVersion = parseVersion(strOtherVersion)
    version.compareTo(otherVersion) <= 0
  }

  def greaterThan(strOtherVersion: String): Boolean = {
    val otherVersion = parseVersion(strOtherVersion)
    version.compareTo(otherVersion) > 0
  }

  def greaterThanOrEqualTo(strOtherVersion: String): Boolean = {
    val otherVersion = parseVersion(strOtherVersion)
    version.compareTo(otherVersion) >= 0
  }

  def equalTo(strOtherVersion: String): Boolean = {
    val otherVersion = parseVersion(strOtherVersion)
    version.compareTo(otherVersion) == 0
  }
}
