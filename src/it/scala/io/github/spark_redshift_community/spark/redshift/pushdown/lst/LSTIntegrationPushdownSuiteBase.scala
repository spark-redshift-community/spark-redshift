/*
 * Copyright (c) Microsoft Corporation.
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package io.github.spark_redshift_community.spark.redshift.pushdown.lst.test

import software.amazon.awssdk.utils.IoUtils
import io.github.spark_redshift_community.spark.redshift.pushdown.test.IntegrationPushdownSuiteBase

class LSTIntegrationPushdownSuiteBase extends IntegrationPushdownSuiteBase{

  // a list of all the tables used for the LST dataset testing
  protected val tableNames: List[String] = List(
    "catalog_returns",
    "catalog_sales",
    "date_dim",
    "inventory",
    "store_returns",
    "web_returns",
    "web_sales"
  )

  // drops the tables necessary for running the TPC-DS correctness suite
  def tableCleanUpHelper(stmt: String): Unit = {
    for ( tpcds_table <- tableNames) {
      redshiftWrapper.executeUpdate(conn, s"${stmt} $tpcds_table")
      redshiftWrapper.executeUpdate(conn, s"${stmt} ${tpcds_table}_copy")
    }
  }

  // creates and populates the tables necessary for running the TPC-DS correctness suite
  def tableSetUpHelper(filename_prefix: String): Unit = {

    // for each of the defined tables, we want to run both the create and load SQL
    for ( lst_table <- tableNames) {
        val create_stmt = IoUtils.toUtf8String(
          getClass().getClassLoader().getResourceAsStream(
            s"lst/${filename_prefix}_${lst_table}.sql")
        )

        val create_stmt_copy = s"CREATE TABLE IF NOT EXISTS ${lst_table}_copy (LIKE ${lst_table});"

        redshiftWrapper.executeUpdate(conn, create_stmt)
        redshiftWrapper.executeUpdate(conn, create_stmt_copy)
      }
  }

  // drops any danging tables from previous LST test runs then re-creates
  override def beforeAll(): Unit = {
    super.beforeAll()
    tableCleanUpHelper("drop table if exists")
    tableSetUpHelper("1_create")
  }

  // drops any danging tables from previous LST test runs
  override def afterAll(): Unit = {
    try {
      tableCleanUpHelper("drop table if exists")
    } finally {
      super.afterAll()
    }
  }

  // truncates existing tables from previous LST test case then re-loads
  override def beforeEach(): Unit = {
    super.beforeEach()
    try {
    tableCleanUpHelper("truncate")} catch {
      case _ : Exception => tableSetUpHelper("1_create")
    }
    tableSetUpHelper("2_load")
  }
}
