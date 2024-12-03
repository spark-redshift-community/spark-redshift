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
package io.github.spark_redshift_community.spark.redshift.pushdown.lst

import io.github.spark_redshift_community.spark.redshift.pushdown.TestCase
import org.apache.spark.sql.Row

class SmallWritesCorrectnessSuite extends LSTIntegrationPushdownSuiteBase {
  // These tests cannot disable pushdown since insert happens in pushdown
  override protected val auto_pushdown: String = "true"
  // These tests cannot use cache since they check the result changing
  override val s3_result_cache: String = "false"

  val testMerge1: TestCase = TestCase(
    s"""MERGE INTO web_returns_copy t
       |USING web_returns s
       |ON t.wr_order_number = s.wr_order_number AND t.wr_item_sk = s.wr_item_sk
       |WHEN NOT MATCHED THEN INSERT *""".stripMargin,
    Seq(Row()),
    /* This needs to be adjusted after extending the DML MERGE */
    s"""MERGE INTO "PUBLIC"."web_returns_copy" USING ( SELECT * FROM
       | "PUBLIC"."web_returns" AS "RS_CONNECTOR_QUERY_ALIAS" )
       |  AS "SUBQUERY_0"ON (
       |    (
       |        "PUBLIC"."WEB_RETURNS_COPY"."WR_ORDER_NUMBER" = "SUBQUERY_0"."WR_ORDER_NUMBER"
       |    )
       |    AND (
       |        "PUBLIC"."WEB_RETURNS_COPY"."WR_ITEM_SK" = "SUBQUERY_0"."WR_ITEM_SK"
       |    )
       |)
       |WHEN MATCHED THEN
       |UPDATE
       |SET
       |    "WR_RETURNED_DATE_SK" = "PUBLIC"."WEB_RETURNS_COPY"."WR_RETURNED_DATE_SK"
       |    WHEN NOT MATCHED THEN
       |INSERT
       |    (
       |        "WR_RETURNED_DATE_SK",
       |        "WR_RETURNED_TIME_SK",
       |        "WR_ITEM_SK",
       |        "WR_REFUNDED_CUSTOMER_SK",
       |        "WR_REFUNDED_CDEMO_SK",
       |        "WR_REFUNDED_HDEMO_SK",
       |        "WR_REFUNDED_ADDR_SK",
       |        "WR_RETURNING_CUSTOMER_SK",
       |        "WR_RETURNING_CDEMO_SK",
       |        "WR_RETURNING_HDEMO_SK",
       |        "WR_RETURNING_ADDR_SK",
       |        "WR_WEB_PAGE_SK",
       |        "WR_REASON_SK",
       |        "WR_ORDER_NUMBER",
       |        "WR_RETURN_QUANTITY",
       |        "WR_RETURN_AMT",
       |        "WR_RETURN_TAX",
       |        "WR_RETURN_AMT_INC_TAX",
       |        "WR_FEE",
       |        "WR_RETURN_SHIP_COST",
       |        "WR_REFUNDED_CASH",
       |        "WR_REVERSED_CHARGE",
       |        "WR_ACCOUNT_CREDIT",
       |        "WR_NET_LOSS"
       |    )
       |VALUES
       |    (
       |        "SUBQUERY_0"."WR_RETURNED_DATE_SK",
       |        "SUBQUERY_0"."WR_RETURNED_TIME_SK",
       |        "SUBQUERY_0"."WR_ITEM_SK",
       |        "SUBQUERY_0"."WR_REFUNDED_CUSTOMER_SK",
       |        "SUBQUERY_0"."WR_REFUNDED_CDEMO_SK",
       |        "SUBQUERY_0"."WR_REFUNDED_HDEMO_SK",
       |        "SUBQUERY_0"."WR_REFUNDED_ADDR_SK",
       |        "SUBQUERY_0"."WR_RETURNING_CUSTOMER_SK",
       |        "SUBQUERY_0"."WR_RETURNING_CDEMO_SK",
       |        "SUBQUERY_0"."WR_RETURNING_HDEMO_SK",
       |        "SUBQUERY_0"."WR_RETURNING_ADDR_SK",
       |        "SUBQUERY_0"."WR_WEB_PAGE_SK",
       |        "SUBQUERY_0"."WR_REASON_SK",
       |        "SUBQUERY_0"."WR_ORDER_NUMBER",
       |        "SUBQUERY_0"."WR_RETURN_QUANTITY",
       |        "SUBQUERY_0"."WR_RETURN_AMT",
       |        "SUBQUERY_0"."WR_RETURN_TAX",
       |        "SUBQUERY_0"."WR_RETURN_AMT_INC_TAX",
       |        "SUBQUERY_0"."WR_FEE",
       |        "SUBQUERY_0"."WR_RETURN_SHIP_COST",
       |        "SUBQUERY_0"."WR_REFUNDED_CASH",
       |        "SUBQUERY_0"."WR_REVERSED_CHARGE",
       |        "SUBQUERY_0"."WR_ACCOUNT_CREDIT",
       |        "SUBQUERY_0"."WR_NET_LOSS"
       |    )""".stripMargin.replaceAll("\\s", ""))

  test("1. Merge Single Insert") {
    read
      .option("dbtable", s"web_returns")
      .load()
      .createOrReplaceTempView(s"web_returns")
    read
      .option("dbtable", s"web_returns_copy")
      .load()
      .createOrReplaceTempView(s"web_returns_copy")
    checkAnswer(
      sqlContext.sql("SELECT COUNT(*) FROM web_returns_copy"),
      Seq(Row(0)))
    doTest(sqlContext, testMerge1)
    val cnt = sqlContext.sql(s"SELECT COUNT(*) FROM web_returns").collect().head.getLong(0)
    checkAnswer(
      sqlContext.sql("SELECT COUNT(*) FROM web_returns_copy"),
      Seq(Row(cnt)))
  }

  val testMerge2: TestCase = TestCase(
    s"""MERGE INTO web_returns_copy t
       |USING web_returns s
       |ON t.wr_order_number = s.wr_order_number AND t.wr_item_sk = s.wr_item_sk
       |WHEN NOT MATCHED AND s.wr_item_sk % 2 = 0 THEN INSERT *
       |WHEN NOT MATCHED THEN INSERT *""".stripMargin,
    Seq(Row()),
    s"""MERGE INTO "PUBLIC"."web_returns_copy" USING ( SELECT * FROM "PUBLIC"."web_returns"
       | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ON (
       |    (
       |        "PUBLIC"."WEB_RETURNS_COPY"."WR_ORDER_NUMBER" = "SUBQUERY_0"."WR_ORDER_NUMBER"
       |    )
       |    AND (
       |        "PUBLIC"."WEB_RETURNS_COPY"."WR_ITEM_SK" = "SUBQUERY_0"."WR_ITEM_SK"
       |    )
       |)
       |WHEN MATCHED THEN
       |UPDATE
       |SET
       |    "WR_RETURNED_DATE_SK" = "PUBLIC"."WEB_RETURNS_COPY"."WR_RETURNED_DATE_SK"
       |WHEN NOT MATCHED THEN
       |INSERT
       |    (
       |        "WR_RETURNED_DATE_SK",
       |        "WR_RETURNED_TIME_SK",
       |        "WR_ITEM_SK",
       |        "WR_REFUNDED_CUSTOMER_SK",
       |        "WR_REFUNDED_CDEMO_SK",
       |        "WR_REFUNDED_HDEMO_SK",
       |        "WR_REFUNDED_ADDR_SK",
       |        "WR_RETURNING_CUSTOMER_SK",
       |        "WR_RETURNING_CDEMO_SK",
       |        "WR_RETURNING_HDEMO_SK",
       |        "WR_RETURNING_ADDR_SK",
       |        "WR_WEB_PAGE_SK",
       |        "WR_REASON_SK",
       |        "WR_ORDER_NUMBER",
       |        "WR_RETURN_QUANTITY",
       |        "WR_RETURN_AMT",
       |        "WR_RETURN_TAX",
       |        "WR_RETURN_AMT_INC_TAX",
       |        "WR_FEE",
       |        "WR_RETURN_SHIP_COST",
       |        "WR_REFUNDED_CASH",
       |        "WR_REVERSED_CHARGE",
       |        "WR_ACCOUNT_CREDIT",
       |        "WR_NET_LOSS"
       |    )
       |VALUES
       |    (
       |        "SUBQUERY_0"."WR_RETURNED_DATE_SK",
       |        "SUBQUERY_0"."WR_RETURNED_TIME_SK",
       |        "SUBQUERY_0"."WR_ITEM_SK",
       |        "SUBQUERY_0"."WR_REFUNDED_CUSTOMER_SK",
       |        "SUBQUERY_0"."WR_REFUNDED_CDEMO_SK",
       |        "SUBQUERY_0"."WR_REFUNDED_HDEMO_SK",
       |        "SUBQUERY_0"."WR_REFUNDED_ADDR_SK",
       |        "SUBQUERY_0"."WR_RETURNING_CUSTOMER_SK",
       |        "SUBQUERY_0"."WR_RETURNING_CDEMO_SK",
       |        "SUBQUERY_0"."WR_RETURNING_HDEMO_SK",
       |        "SUBQUERY_0"."WR_RETURNING_ADDR_SK",
       |        "SUBQUERY_0"."WR_WEB_PAGE_SK",
       |        "SUBQUERY_0"."WR_REASON_SK",
       |        "SUBQUERY_0"."WR_ORDER_NUMBER",
       |        "SUBQUERY_0"."WR_RETURN_QUANTITY",
       |        "SUBQUERY_0"."WR_RETURN_AMT",
       |        "SUBQUERY_0"."WR_RETURN_TAX",
       |        "SUBQUERY_0"."WR_RETURN_AMT_INC_TAX",
       |        "SUBQUERY_0"."WR_FEE",
       |        "SUBQUERY_0"."WR_RETURN_SHIP_COST",
       |        "SUBQUERY_0"."WR_REFUNDED_CASH",
       |        "SUBQUERY_0"."WR_REVERSED_CHARGE",
       |        "SUBQUERY_0"."WR_ACCOUNT_CREDIT",
       |        "SUBQUERY_0"."WR_NET_LOSS"
       |    )""".stripMargin.replaceAll("\\s", ""))
  test("2. Merge Multiple Inserts") {
    read
      .option("dbtable", s"web_returns")
      .load()
      .createOrReplaceTempView(s"web_returns")
    read
      .option("dbtable", s"web_returns_copy")
      .load()
      .createOrReplaceTempView(s"web_returns_copy")
    checkAnswer(
      sqlContext.sql("SELECT COUNT(*) FROM web_returns_copy"),
      Seq(Row(0)))
    doTest(sqlContext, testMerge2)
    val pre = sqlContext.sql(s"SELECT COUNT(*) FROM web_returns").collect().head.getLong(0)
    val post = sqlContext.sql(s"SELECT COUNT(*) FROM web_returns_copy").collect().head.getLong(0)
    checkAnswer(
      sqlContext.sql("SELECT COUNT(*) FROM web_returns_copy"),
      Seq(Row(20)))
  }

  val testMerge3: TestCase = TestCase(
    s"""MERGE INTO web_returns_copy t
       |USING web_returns s
       |ON t.wr_order_number = s.wr_order_number AND t.wr_item_sk = s.wr_item_sk
       |WHEN MATCHED THEN DELETE""".stripMargin,
    Seq(Row()),
    s"""DELETE FROM
       |  "PUBLIC"."web_returns_copy" USING ( SELECT * FROM "PUBLIC"."web_returns"
       | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_1"
       | WHERE
       |  (
       |    (
       |      "PUBLIC"."WEB_RETURNS_COPY"."WR_ORDER_NUMBER" = "SUBQUERY_1"."WR_ORDER_NUMBER"
       |    )
       |    AND (
       |      "PUBLIC"."WEB_RETURNS_COPY"."WR_ITEM_SK" = "SUBQUERY_1"."WR_ITEM_SK"
       |    )
       |  )""".stripMargin.replaceAll("\\s", ""))
  test("3. Merge Delete") {
    read
      .option("dbtable", s"web_returns")
      .load()
      .createOrReplaceTempView(s"web_returns")
    read
      .option("dbtable", s"web_returns_copy")
      .load()
      .createOrReplaceTempView(s"web_returns_copy")
    val pre = sqlContext.sql(s"SELECT COUNT(*) FROM web_returns_copy").collect().head.getLong(0)
    sqlContext.sql("INSERT INTO web_returns_copy SELECT * FROM web_returns;")
    checkAnswer(
      sqlContext.sql("SELECT COUNT(*) FROM web_returns_copy"),
      Seq(Row(pre + 20)))
    doTest(sqlContext, testMerge3)
    checkAnswer(
      sqlContext.sql("SELECT COUNT(*) FROM web_returns_copy"),
      Seq(Row(pre)))
  }

  val testMerge4: TestCase = TestCase(
    s"""MERGE INTO web_returns_copy t
       |USING web_returns s
       |ON t.wr_order_number = s.wr_order_number AND t.wr_item_sk = s.wr_item_sk
       |WHEN MATCHED THEN UPDATE SET *
       |WHEN NOT MATCHED THEN INSERT *""".stripMargin,
    Seq(Row()),
    s"""MERGE INTO "PUBLIC"."web_returns_copy" USING ( SELECT * FROM "PUBLIC"."web_returns"
       | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | ON (
       |    (
       |        "PUBLIC"."WEB_RETURNS_COPY"."WR_ORDER_NUMBER" = "SUBQUERY_0"."WR_ORDER_NUMBER"
       |    )
       |    AND (
       |        "PUBLIC"."WEB_RETURNS_COPY"."WR_ITEM_SK" = "SUBQUERY_0"."WR_ITEM_SK"
       |    )
       |)
       |WHEN MATCHED THEN
       |UPDATE
       |SET
       |    "WR_RETURNED_DATE_SK" = "SUBQUERY_0"."WR_RETURNED_DATE_SK",
       |    "WR_RETURNED_TIME_SK" = "SUBQUERY_0"."WR_RETURNED_TIME_SK",
       |    "WR_ITEM_SK" = "SUBQUERY_0"."WR_ITEM_SK",
       |    "WR_REFUNDED_CUSTOMER_SK" = "SUBQUERY_0"."WR_REFUNDED_CUSTOMER_SK",
       |    "WR_REFUNDED_CDEMO_SK" = "SUBQUERY_0"."WR_REFUNDED_CDEMO_SK",
       |    "WR_REFUNDED_HDEMO_SK" = "SUBQUERY_0"."WR_REFUNDED_HDEMO_SK",
       |    "WR_REFUNDED_ADDR_SK" = "SUBQUERY_0"."WR_REFUNDED_ADDR_SK",
       |    "WR_RETURNING_CUSTOMER_SK" = "SUBQUERY_0"."WR_RETURNING_CUSTOMER_SK",
       |    "WR_RETURNING_CDEMO_SK" = "SUBQUERY_0"."WR_RETURNING_CDEMO_SK",
       |    "WR_RETURNING_HDEMO_SK" = "SUBQUERY_0"."WR_RETURNING_HDEMO_SK",
       |    "WR_RETURNING_ADDR_SK" = "SUBQUERY_0"."WR_RETURNING_ADDR_SK",
       |    "WR_WEB_PAGE_SK" = "SUBQUERY_0"."WR_WEB_PAGE_SK",
       |    "WR_REASON_SK" = "SUBQUERY_0"."WR_REASON_SK",
       |    "WR_ORDER_NUMBER" = "SUBQUERY_0"."WR_ORDER_NUMBER",
       |    "WR_RETURN_QUANTITY" = "SUBQUERY_0"."WR_RETURN_QUANTITY",
       |    "WR_RETURN_AMT" = "SUBQUERY_0"."WR_RETURN_AMT",
       |    "WR_RETURN_TAX" = "SUBQUERY_0"."WR_RETURN_TAX",
       |    "WR_RETURN_AMT_INC_TAX" = "SUBQUERY_0"."WR_RETURN_AMT_INC_TAX",
       |    "WR_FEE" = "SUBQUERY_0"."WR_FEE",
       |    "WR_RETURN_SHIP_COST" = "SUBQUERY_0"."WR_RETURN_SHIP_COST",
       |    "WR_REFUNDED_CASH" = "SUBQUERY_0"."WR_REFUNDED_CASH",
       |    "WR_REVERSED_CHARGE" = "SUBQUERY_0"."WR_REVERSED_CHARGE",
       |    "WR_ACCOUNT_CREDIT" = "SUBQUERY_0"."WR_ACCOUNT_CREDIT",
       |    "WR_NET_LOSS" = "SUBQUERY_0"."WR_NET_LOSS"
       |    WHEN NOT MATCHED THEN
       |INSERT
       |    (
       |        "WR_RETURNED_DATE_SK",
       |        "WR_RETURNED_TIME_SK",
       |        "WR_ITEM_SK",
       |        "WR_REFUNDED_CUSTOMER_SK",
       |        "WR_REFUNDED_CDEMO_SK",
       |        "WR_REFUNDED_HDEMO_SK",
       |        "WR_REFUNDED_ADDR_SK",
       |        "WR_RETURNING_CUSTOMER_SK",
       |        "WR_RETURNING_CDEMO_SK",
       |        "WR_RETURNING_HDEMO_SK",
       |        "WR_RETURNING_ADDR_SK",
       |        "WR_WEB_PAGE_SK",
       |        "WR_REASON_SK",
       |        "WR_ORDER_NUMBER",
       |        "WR_RETURN_QUANTITY",
       |        "WR_RETURN_AMT",
       |        "WR_RETURN_TAX",
       |        "WR_RETURN_AMT_INC_TAX",
       |        "WR_FEE",
       |        "WR_RETURN_SHIP_COST",
       |        "WR_REFUNDED_CASH",
       |        "WR_REVERSED_CHARGE",
       |        "WR_ACCOUNT_CREDIT",
       |        "WR_NET_LOSS"
       |    )
       |VALUES
       |    (
       |        "SUBQUERY_0"."WR_RETURNED_DATE_SK",
       |        "SUBQUERY_0"."WR_RETURNED_TIME_SK",
       |        "SUBQUERY_0"."WR_ITEM_SK",
       |        "SUBQUERY_0"."WR_REFUNDED_CUSTOMER_SK",
       |        "SUBQUERY_0"."WR_REFUNDED_CDEMO_SK",
       |        "SUBQUERY_0"."WR_REFUNDED_HDEMO_SK",
       |        "SUBQUERY_0"."WR_REFUNDED_ADDR_SK",
       |        "SUBQUERY_0"."WR_RETURNING_CUSTOMER_SK",
       |        "SUBQUERY_0"."WR_RETURNING_CDEMO_SK",
       |        "SUBQUERY_0"."WR_RETURNING_HDEMO_SK",
       |        "SUBQUERY_0"."WR_RETURNING_ADDR_SK",
       |        "SUBQUERY_0"."WR_WEB_PAGE_SK",
       |        "SUBQUERY_0"."WR_REASON_SK",
       |        "SUBQUERY_0"."WR_ORDER_NUMBER",
       |        "SUBQUERY_0"."WR_RETURN_QUANTITY",
       |        "SUBQUERY_0"."WR_RETURN_AMT",
       |        "SUBQUERY_0"."WR_RETURN_TAX",
       |        "SUBQUERY_0"."WR_RETURN_AMT_INC_TAX",
       |        "SUBQUERY_0"."WR_FEE",
       |        "SUBQUERY_0"."WR_RETURN_SHIP_COST",
       |        "SUBQUERY_0"."WR_REFUNDED_CASH",
       |        "SUBQUERY_0"."WR_REVERSED_CHARGE",
       |        "SUBQUERY_0"."WR_ACCOUNT_CREDIT",
       |        "SUBQUERY_0"."WR_NET_LOSS"
       |    )""".stripMargin.replaceAll("\\s", ""))
  test("4. Merge Upsert") {
    read
      .option("dbtable", s"web_returns")
      .load()
      .createOrReplaceTempView(s"web_returns")
    read
      .option("dbtable", s"web_returns_copy")
      .load()
      .createOrReplaceTempView(s"web_returns_copy")
    sqlContext.sql("INSERT INTO web_returns_copy " +
      "SELECT * FROM web_returns ORDER BY wr_order_number DESC LIMIT 2;")
    val query =
      """SELECT count(*)
        |FROM web_returns_copy s
        |INNER JOIN web_returns t
        |ON s.wr_order_number = t.wr_order_number
        |AND s.wr_returned_date_sk = t.wr_returned_date_sk""".stripMargin
    // web_returns_copy has only 2 row and they exist in web_returns
    assert(2 === sqlContext.sql(query).collect().head.getLong(0))
    // change the id of one of them (in web_returns_copy) so it does not exist in web_returns
    sqlContext.sql(
      """
        |UPDATE web_returns_copy
        |SET wr_order_number = wr_order_number + 1
        |WHERE wr_order_number = (SELECT MAX(wr_order_number) FROM web_returns_copy)""".stripMargin)
    // web_returns_copy has only 2 rows and one of them  does NOT exist in web_returns
    assert(1 === sqlContext.sql(query).collect().head.getLong(0))
    doTest(sqlContext, testMerge4)
    // Now web_returns copy has:
    // 1 row not in web_returns
    // 1 existing row updated to be like web_returns
    // 19 new rows from web_returns
    // total 21 rows, 20 of which are available in web_returns
    assert(21 === sqlContext.sql("SELECT COUNT(*) FROM web_returns_copy").collect().head.getLong(0))
    assert(20 === sqlContext.sql(query).collect().head.getLong(0))
  }

  val testInsert1: TestCase = TestCase(
    s"""INSERT INTO
       |  store_returns_copy
       |SELECT
       |  s.*
       |FROM
       |  store_returns AS s
       |  INNER JOIN store_returns_copy AS t ON t.sr_ticket_number = s.sr_ticket_number
       |  AND t.sr_item_sk = s.sr_item_sk;""".stripMargin,
    Seq(Row()),
    s"""INSERT INTO
       |  "PUBLIC"."store_returns_copy"
       |SELECT
       |  ("SUBQUERY_3"."SUBQUERY_3_COL_0") AS "SUBQUERY_4_COL_0",
       |  ("SUBQUERY_3"."SUBQUERY_3_COL_1") AS "SUBQUERY_4_COL_1",
       |  ("SUBQUERY_3"."SUBQUERY_3_COL_2") AS "SUBQUERY_4_COL_2",
       |  ("SUBQUERY_3"."SUBQUERY_3_COL_3") AS "SUBQUERY_4_COL_3",
       |  ("SUBQUERY_3"."SUBQUERY_3_COL_4") AS "SUBQUERY_4_COL_4",
       |  ("SUBQUERY_3"."SUBQUERY_3_COL_5") AS "SUBQUERY_4_COL_5",
       |  ("SUBQUERY_3"."SUBQUERY_3_COL_6") AS "SUBQUERY_4_COL_6",
       |  ("SUBQUERY_3"."SUBQUERY_3_COL_7") AS "SUBQUERY_4_COL_7",
       |  ("SUBQUERY_3"."SUBQUERY_3_COL_8") AS "SUBQUERY_4_COL_8",
       |  ("SUBQUERY_3"."SUBQUERY_3_COL_9") AS "SUBQUERY_4_COL_9",
       |  ("SUBQUERY_3"."SUBQUERY_3_COL_10") AS "SUBQUERY_4_COL_10",
       |  ("SUBQUERY_3"."SUBQUERY_3_COL_11") AS "SUBQUERY_4_COL_11",
       |  ("SUBQUERY_3"."SUBQUERY_3_COL_12") AS "SUBQUERY_4_COL_12",
       |  ("SUBQUERY_3"."SUBQUERY_3_COL_13") AS "SUBQUERY_4_COL_13",
       |  ("SUBQUERY_3"."SUBQUERY_3_COL_14") AS "SUBQUERY_4_COL_14",
       |  ("SUBQUERY_3"."SUBQUERY_3_COL_15") AS "SUBQUERY_4_COL_15",
       |  ("SUBQUERY_3"."SUBQUERY_3_COL_16") AS "SUBQUERY_4_COL_16",
       |  ("SUBQUERY_3"."SUBQUERY_3_COL_17") AS "SUBQUERY_4_COL_17",
       |  ("SUBQUERY_3"."SUBQUERY_3_COL_18") AS "SUBQUERY_4_COL_18",
       |  ("SUBQUERY_3"."SUBQUERY_3_COL_19") AS "SUBQUERY_4_COL_19"
       |FROM
       |  (
       |    SELECT
       |      ("SUBQUERY_1"."SR_RETURNED_DATE_SK") AS "SUBQUERY_3_COL_0",
       |      ("SUBQUERY_1"."SR_RETURN_TIME_SK") AS "SUBQUERY_3_COL_1",
       |      ("SUBQUERY_1"."SR_ITEM_SK") AS "SUBQUERY_3_COL_2",
       |      ("SUBQUERY_1"."SR_CUSTOMER_SK") AS "SUBQUERY_3_COL_3",
       |      ("SUBQUERY_1"."SR_CDEMO_SK") AS "SUBQUERY_3_COL_4",
       |      ("SUBQUERY_1"."SR_HDEMO_SK") AS "SUBQUERY_3_COL_5",
       |      ("SUBQUERY_1"."SR_ADDR_SK") AS "SUBQUERY_3_COL_6",
       |      ("SUBQUERY_1"."SR_STORE_SK") AS "SUBQUERY_3_COL_7",
       |      ("SUBQUERY_1"."SR_REASON_SK") AS "SUBQUERY_3_COL_8",
       |      ("SUBQUERY_1"."SR_TICKET_NUMBER") AS "SUBQUERY_3_COL_9",
       |      ("SUBQUERY_1"."SR_RETURN_QUANTITY") AS "SUBQUERY_3_COL_10",
       |      ("SUBQUERY_1"."SR_RETURN_AMT") AS "SUBQUERY_3_COL_11",
       |      ("SUBQUERY_1"."SR_RETURN_TAX") AS "SUBQUERY_3_COL_12",
       |      ("SUBQUERY_1"."SR_RETURN_AMT_INC_TAX") AS "SUBQUERY_3_COL_13",
       |      ("SUBQUERY_1"."SR_FEE") AS "SUBQUERY_3_COL_14",
       |      ("SUBQUERY_1"."SR_RETURN_SHIP_COST") AS "SUBQUERY_3_COL_15",
       |      ("SUBQUERY_1"."SR_REFUNDED_CASH") AS "SUBQUERY_3_COL_16",
       |      ("SUBQUERY_1"."SR_REVERSED_CHARGE") AS "SUBQUERY_3_COL_17",
       |      ("SUBQUERY_1"."SR_STORE_CREDIT") AS "SUBQUERY_3_COL_18",
       |      ("SUBQUERY_1"."SR_NET_LOSS") AS "SUBQUERY_3_COL_19",
       |      ("SUBQUERY_2"."SR_RETURNED_DATE_SK") AS "SUBQUERY_3_COL_20",
       |      ("SUBQUERY_2"."SR_RETURN_TIME_SK") AS "SUBQUERY_3_COL_21",
       |      ("SUBQUERY_2"."SR_ITEM_SK") AS "SUBQUERY_3_COL_22",
       |      ("SUBQUERY_2"."SR_CUSTOMER_SK") AS "SUBQUERY_3_COL_23",
       |      ("SUBQUERY_2"."SR_CDEMO_SK") AS "SUBQUERY_3_COL_24",
       |      ("SUBQUERY_2"."SR_HDEMO_SK") AS "SUBQUERY_3_COL_25",
       |      ("SUBQUERY_2"."SR_ADDR_SK") AS "SUBQUERY_3_COL_26",
       |      ("SUBQUERY_2"."SR_STORE_SK") AS "SUBQUERY_3_COL_27",
       |      ("SUBQUERY_2"."SR_REASON_SK") AS "SUBQUERY_3_COL_28",
       |      ("SUBQUERY_2"."SR_TICKET_NUMBER") AS "SUBQUERY_3_COL_29",
       |      ("SUBQUERY_2"."SR_RETURN_QUANTITY") AS "SUBQUERY_3_COL_30",
       |      ("SUBQUERY_2"."SR_RETURN_AMT") AS "SUBQUERY_3_COL_31",
       |      ("SUBQUERY_2"."SR_RETURN_TAX") AS "SUBQUERY_3_COL_32",
       |      ("SUBQUERY_2"."SR_RETURN_AMT_INC_TAX") AS "SUBQUERY_3_COL_33",
       |      ("SUBQUERY_2"."SR_FEE") AS "SUBQUERY_3_COL_34",
       |      ("SUBQUERY_2"."SR_RETURN_SHIP_COST") AS "SUBQUERY_3_COL_35",
       |      ("SUBQUERY_2"."SR_REFUNDED_CASH") AS "SUBQUERY_3_COL_36",
       |      ("SUBQUERY_2"."SR_REVERSED_CHARGE") AS "SUBQUERY_3_COL_37",
       |      ("SUBQUERY_2"."SR_STORE_CREDIT") AS "SUBQUERY_3_COL_38",
       |      ("SUBQUERY_2"."SR_NET_LOSS") AS "SUBQUERY_3_COL_39"
       |    FROM
       |      (
       |        SELECT
       |          *
       |        FROM
       |          "PUBLIC"."store_returns" AS "RS_CONNECTOR_QUERY_ALIAS"
       |      ) AS "SUBQUERY_1"
       |      INNER JOIN (
       |        SELECT
       |          *
       |        FROM
       |          "PUBLIC"."store_returns_copy" AS "RS_CONNECTOR_QUERY_ALIAS"
       |      ) AS "SUBQUERY_2" ON (
       |        (
       |          "SUBQUERY_2"."SR_TICKET_NUMBER" = "SUBQUERY_1"."SR_TICKET_NUMBER"
       |        )
       |        AND (
       |          "SUBQUERY_2"."SR_ITEM_SK" = "SUBQUERY_1"."SR_ITEM_SK"
       |        )
       |      )
       |  ) AS "SUBQUERY_3"""".stripMargin)
  test("5. Insert") {
    read
      .option("dbtable", s"store_returns")
      .load()
      .createOrReplaceTempView(s"store_returns")
    read
      .option("dbtable", s"store_returns_copy")
      .load()
      .createOrReplaceTempView(s"store_returns_copy")
    sqlContext.sql("INSERT INTO store_returns_copy SELECT * FROM store_returns LIMIT 2;")
    // Alter one of them so we can have one match and one not matched.
    assert(2 === sqlContext.sql("SELECT COUNT(*) FROM store_returns_copy").collect().head.getLong(0))
    doTest(sqlContext, testInsert1)
    // we just duplicated the store_returns_copy rows
    assert(2 + 2 === sqlContext.sql("SELECT COUNT(*) FROM store_returns_copy").collect().head.getLong(0))
    assert(2 ===
      sqlContext.sql(
        "SELECT COUNT(DISTINCT sr_item_sk) FROM store_returns_copy").collect().head.getLong(0))
  }

  val testDelete1: TestCase = TestCase(
    s"""DELETE FROM web_sales_copy
       |WHERE ws_order_number IN (
       |    SELECT s.ws_order_number
       |    FROM web_sales_copy AS t
       |    INNER JOIN web_sales AS s
       |        ON t.ws_order_number = s.ws_order_number
       |        AND t.ws_item_sk = s.ws_item_sk
       |);""".stripMargin,
    Seq(Row()),
    s"""DELETE FROM
       |  "PUBLIC"."web_sales_copy"
       |WHERE
       |  ("PUBLIC"."WEB_SALES_COPY"."WS_ORDER_NUMBER") IN (
       |    SELECT
       |      ("SUBQUERY_4"."SUBQUERY_4_COL_3") AS "SUBQUERY_5_COL_0"
       |    FROM
       |      (
       |        SELECT
       |          ("SUBQUERY_1"."SUBQUERY_1_COL_0") AS "SUBQUERY_4_COL_0",
       |          ("SUBQUERY_1"."SUBQUERY_1_COL_1") AS "SUBQUERY_4_COL_1",
       |          ("SUBQUERY_3"."SUBQUERY_3_COL_0") AS "SUBQUERY_4_COL_2",
       |          ("SUBQUERY_3"."SUBQUERY_3_COL_1") AS "SUBQUERY_4_COL_3"
       |        FROM
       |          (
       |            SELECT
       |              ("SUBQUERY_0"."WS_ITEM_SK") AS "SUBQUERY_1_COL_0",
       |              ("SUBQUERY_0"."WS_ORDER_NUMBER") AS "SUBQUERY_1_COL_1"
       |            FROM
       |              (
       |                SELECT
       |                  *
       |                FROM
       |                  "PUBLIC"."web_sales_copy" AS "RS_CONNECTOR_QUERY_ALIAS"
       |              ) AS "SUBQUERY_0"
       |          ) AS "SUBQUERY_1"
       |          INNER JOIN (
       |            SELECT
       |              ("SUBQUERY_2"."WS_ITEM_SK") AS "SUBQUERY_3_COL_0",
       |              ("SUBQUERY_2"."WS_ORDER_NUMBER") AS "SUBQUERY_3_COL_1"
       |            FROM
       |              (
       |                SELECT
       |                  *
       |                FROM
       |                  "PUBLIC"."web_sales" AS "RS_CONNECTOR_QUERY_ALIAS"
       |              ) AS "SUBQUERY_2"
       |          ) AS "SUBQUERY_3" ON (
       |            (
       |              "SUBQUERY_1"."SUBQUERY_1_COL_1" = "SUBQUERY_3"."SUBQUERY_3_COL_1"
       |            )
       |            AND (
       |              "SUBQUERY_1"."SUBQUERY_1_COL_0" = "SUBQUERY_3"."SUBQUERY_3_COL_0"
       |            )
       |          )
       |      ) AS "SUBQUERY_4"
       |  )""".stripMargin)

  test("6. LST small-writes DELETE from subquery with inner join") {
    read
      .option("dbtable", s"web_sales")
      .load()
      .createOrReplaceTempView(s"web_sales")
    read
      .option("dbtable", s"web_sales_copy")
      .load()
      .createOrReplaceTempView(s"web_sales_copy")
    sqlContext.sql(
      """INSERT INTO web_sales_copy
        | SELECT * FROM (
        |     SELECT *
        |     FROM web_sales
        |     ORDER BY ws_item_sk
        |     LIMIT 2)""".stripMargin)
    val query =
      """SELECT count(*)
        |FROM web_sales_copy s
        |INNER JOIN web_sales t
        |ON s.ws_item_sk = t.ws_item_sk
        |AND s.ws_order_number = t.ws_order_number""".stripMargin
    // web_sales_copy has only 2 row and they exist in web_sales
    assert(2 === sqlContext.sql(query).collect().head.getLong(0))
    // change the id of one of them (in web_sales_copy) so it does not exist in web_sales
    sqlContext.sql(
      """
        |UPDATE web_sales_copy
        |SET ws_item_sk = ws_item_sk+1
        |WHERE ws_item_sk = (SELECT MAX(ws_item_sk) FROM web_sales_copy);""".stripMargin)
    // web_sales_copy has only 2 rows and one of them(by inv_item_sk)
    // does not exist in web_sales, so the above query returns only one row
    // and that is the one which will be (reinserted) to increase the web_sales_copy row count to 3
    assert(1 === sqlContext.sql(query).collect().head.getLong(0))
    // Now do the delete
    doTest(sqlContext, testDelete1)
    // web_sales_copy is left with one row that does not exist in web_sales
    assert(1 === sqlContext.sql("SELECT COUNT(*) FROM web_sales_copy").collect().head.getLong(0))
  }

  val testUpdate1: TestCase = TestCase(
    s"""UPDATE web_returns_copy
       |SET
       |    wr_return_quantity = wr_return_quantity + 100,
       |    wr_return_amt = wr_return_amt + 100,
       |    wr_net_loss = wr_net_loss + 100
       |WHERE (wr_order_number, wr_item_sk) IN (
       |    SELECT wr_order_number, wr_item_sk FROM web_returns
       |);""".stripMargin,
    Seq(Row()),
    /* This needs to be adjusted after extending the DML MERGE */
    s"""UPDATE
       |    "PUBLIC"."web_returns_copy" AS "RT_CONNECTOR_QUERY_ALIAS"
       |SET
       |    "WR_RETURN_QUANTITY" = (
       |        "RT_CONNECTOR_QUERY_ALIAS"."WR_RETURN_QUANTITY" + 100
       |    ),
       |    "WR_RETURN_AMT" = CAST (
       |        (
       |            "RT_CONNECTOR_QUERY_ALIAS"."WR_RETURN_AMT" + 100
       |        ) AS DECIMAL(7, 2)
       |    ),
       |    "WR_NET_LOSS" = CAST (
       |        (
       |            "RT_CONNECTOR_QUERY_ALIAS"."WR_NET_LOSS" + 100
       |        ) AS DECIMAL(7, 2)
       |    )
       |WHERE
       |    (
       |        "RT_CONNECTOR_QUERY_ALIAS"."WR_ORDER_NUMBER",
       |        "RT_CONNECTOR_QUERY_ALIAS"."WR_ITEM_SK"
       |    ) IN (
       |        SELECT
       |            ("SUBQUERY_0"."WR_ORDER_NUMBER") AS "SUBQUERY_1_COL_0",
       |            ("SUBQUERY_0"."WR_ITEM_SK") AS "SUBQUERY_1_COL_1"
       |        FROM
       |            (
       |                SELECT
       |                    *
       |                FROM
       |                    "PUBLIC"."web_returns" AS "RS_CONNECTOR_QUERY_ALIAS"
       |            ) AS "SUBQUERY_0"
       |    )""".stripMargin.replaceAll("\\s", ""))

  /*
  The Spark plan utilizes named_struct (CreateNamedStruct) operators
  which are currently unsupported. This sub-query pushdown is also missing
  for the Delete-1 LST-Bench workload query. Once added, it will address both failures.
   */
  test("7. LST small-writes UPDATE matches the subquery") {
    read
      .option("dbtable", s"web_returns")
      .load()
      .createOrReplaceTempView(s"web_returns")
    read
      .option("dbtable", s"web_returns_copy")
      .load()
      .createOrReplaceTempView(s"web_returns_copy")
    val impactRowCnt = 2
    sqlContext.sql(s"INSERT INTO web_returns_copy SELECT * FROM web_returns")
    val query = """SELECT COUNT(t.wr_order_number)
                  |FROM web_returns as t, web_returns_copy as s
                  |WHERE t.wr_return_quantity = s.wr_return_quantity
                  |AND t.wr_return_amt = s.wr_return_amt
                  |AND t.wr_net_loss = s.wr_net_loss
                  |AND t.wr_order_number = s.wr_order_number
                  |AND t.wr_item_sk = s.wr_item_sk
                  |""".stripMargin
    assert(20 === sqlContext.sql(query).collect().head.getLong(0))
    doTest(sqlContext, testUpdate1)
    assert(0 === sqlContext.sql(query).collect().head.getLong(0))
  }
}
