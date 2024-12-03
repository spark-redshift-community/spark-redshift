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

import io.github.spark_redshift_community.spark.redshift.data.JDBCWrapper
import org.mockito.Mockito.verify
import org.scalatestplus.mockito.MockitoSugar.mock
import org.slf4j.Logger

class QueryGroupIntegrationSuite extends IntegrationSuiteBase {
  test("getConnectorWithQueryGroup returns a working connection when setting query group fails") {
    // This test is only valid for JDBC-based connections
    if (redshiftWrapper.isInstanceOf[JDBCWrapper]) {
      val invalidQueryGroup = "'"
      val params: Map[String, String] = defaultOptions() + ("dbtable" -> "fake_table")
      val mergedParams = Parameters.mergeParameters(params)
      val conn = TestJdbcWrapper.getConnectorWithQueryGroup(mergedParams, invalidQueryGroup)
      verify(TestJdbcWrapper.getLogger).debug("Unable to set query group: " +
        "Unterminated string literal started at position 21 in SQL set query_group to '''. Expected  char")
      try {
        val results = TestJdbcWrapper.executeQueryInterruptibly(conn, "select 1")
        assert(results.next())
        assert(results.getInt(1) == 1)
        assert(!results.next())
      } finally {
        conn.close()
      }
    }
  }
}

private object TestJdbcWrapper extends JDBCWrapper {
  override protected val log = mock[Logger]
  def getLogger: Logger = log
}
