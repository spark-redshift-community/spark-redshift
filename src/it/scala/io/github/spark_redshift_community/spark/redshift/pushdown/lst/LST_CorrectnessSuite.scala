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

import io.github.spark_redshift_community.spark.redshift.pushdown.test.{ TestCase }
import org.apache.spark.sql.Row

class LST_CorrectnessSuite extends LSTIntegrationPushdownSuiteBase {
  // These tests cannot disable pushdown since delete happens in pushdown
  override protected val auto_pushdown: String = "true"
  // These tests cannot use cache since they check the result changing
  override val s3_result_cache: String = "false"

  val testDelete1: TestCase = TestCase(
    s"""DELETE
       |FROM
       |    catalog_sales
       |WHERE
       |    cs_sold_date_sk >=(
       |        SELECT
       |            MIN( d_date_sk )
       |        FROM
       |            date_dim
       |        WHERE
       |            d_date BETWEEN '2024-01-01' AND '2024-01-05'
       |    )
       |    AND cs_sold_date_sk <=(
       |        SELECT
       |            MAX( d_date_sk )
       |        FROM
       |            date_dim
       |        WHERE
       |            d_date BETWEEN '2024-01-01' AND '2024-01-05'
       |    );""".stripMargin,
    Seq(Row()),
    """DELETE FROM "PUBLIC"."catalog_sales" WHERE ( ( "PUBLIC"."CATALOG_SALES"."CS_SOLD_DATE_SK" >=
      | ( SELECT ( "SQ_4_COL_0" . "MIN(D_DATE_SK)" ::INTEGER ) FROM ( SELECT (
      | OBJECT( 'min(d_date_sk)' , "SQ_3"."SQ_3_COL_0" , 'max(d_date_sk)' ,
      | "SQ_3"."SQ_3_COL_1" ) ) AS "SQ_4_COL_0" FROM ( SELECT ( MIN (
      | "SQ_2"."SQ_2_COL_0" ) ) AS "SQ_3_COL_0" , ( MAX (
      | "SQ_2"."SQ_2_COL_0" ) ) AS "SQ_3_COL_1" FROM ( SELECT (
      | "SQ_1"."D_DATE_SK" ) AS "SQ_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
      | "PUBLIC"."date_dim" AS "RCQ_ALIAS" ) AS "SQ_0" WHERE ( (
      | "SQ_0"."D_DATE" IS NOT NULL ) AND ( ( "SQ_0"."D_DATE" >= DATEADD(day, 19723 ,
      | TO_DATE('1970-01-01', 'YYYY-MM-DD')) ) AND ( "SQ_0"."D_DATE" <= DATEADD(day, 19727 ,
      | TO_DATE('1970-01-01', 'YYYY-MM-DD')) ) ) ) ) AS "SQ_1" ) AS "SQ_2" LIMIT 1 )
      | AS "SQ_3" ) ) ) AND ( "PUBLIC"."CATALOG_SALES"."CS_SOLD_DATE_SK" <= ( SELECT (
      | "SQ_4_COL_0" . "MAX(D_DATE_SK)" ::INTEGER ) FROM ( SELECT ( OBJECT( 'min(d_date_sk)'
      | , "SQ_3"."SQ_3_COL_0" , 'max(d_date_sk)' , "SQ_3"."SQ_3_COL_1" ) )
      | AS "SQ_4_COL_0" FROM ( SELECT ( MIN ( "SQ_2"."SQ_2_COL_0" ) ) AS
      | "SQ_3_COL_0" , ( MAX ( "SQ_2"."SQ_2_COL_0" ) ) AS "SQ_3_COL_1"
      | FROM ( SELECT ( "SQ_1"."D_DATE_SK" ) AS "SQ_2_COL_0" FROM ( SELECT * FROM (
      | SELECT * FROM "PUBLIC"."date_dim" AS "RCQ_ALIAS" ) AS "SQ_0" WHERE (
      | ( "SQ_0"."D_DATE" IS NOT NULL ) AND ( ( "SQ_0"."D_DATE" >= DATEADD(day, 19723 ,
      | TO_DATE('1970-01-01', 'YYYY-MM-DD')) ) AND ( "SQ_0"."D_DATE" <= DATEADD(day, 19727 ,
      | TO_DATE('1970-01-01', 'YYYY-MM-DD')) ) ) ) ) AS "SQ_1" ) AS "SQ_2" LIMIT 1 )
      | AS "SQ_3" ) ) ) )""".stripMargin)

  test("LST-Delete-1") {
    read.option("dbtable", s"catalog_sales").load()
      .createOrReplaceTempView(s"catalog_sales")
    read.option("dbtable", s"catalog_returns").load()
      .createOrReplaceTempView(s"catalog_returns")
    read.option("dbtable", s"date_dim").load()
      .createOrReplaceTempView(s"date_dim")
    checkAnswer(sqlContext.sql("select count(*) from catalog_sales"), Seq(Row(20)))
    doTest(sqlContext, testDelete1)
    // rows with cs_sold_date_sk between 1 and 10 should be deleted
    checkAnswer(
      sqlContext.sql("select cs_sold_date_sk from catalog_sales order by cs_sold_date_sk desc"),
      Seq(
        Row(20230907),
        Row(20230906),
        Row(20230905),
        Row(20230904),
        Row(20230903),
        Row(20230902),
        Row(20230901),
        Row(20230831),
        Row(20230830),
        Row(20230829),
        Row(10),
        Row(9),
        Row(8),
        Row(7),
        Row(6)))
  }

  val testDelete2: TestCase = TestCase(
    """DELETE FROM catalog_returns
      | WHERE
      |    cr_order_number IN(
      |        SELECT
      |            cs_order_number
      |        FROM
      |            catalog_sales,
      |            date_dim
      |        WHERE
      |            cs_sold_date_sk = d_date_sk
      |            AND d_date BETWEEN '2024-01-01' AND '2024-01-10'
      |    );""".stripMargin,
    Seq(Row()),
    s"""DELETE FROM
       |  "PUBLIC"."catalog_returns"
       |WHERE
       |  ("PUBLIC"."CATALOG_RETURNS"."CR_ORDER_NUMBER") IN (
       |  SELECT
       |    ("SQ_6"."SQ_6_COL_0") AS "SQ_7_COL_0"
       |  FROM
       |    (
       |    SELECT
       |      ("SQ_2"."SQ_2_COL_0") AS "SQ_6_COL_0",
       |      ("SQ_2"."SQ_2_COL_1") AS "SQ_6_COL_1",
       |      ("SQ_5"."SQ_5_COL_0") AS "SQ_6_COL_2"
       |    FROM
       |      (
       |      SELECT
       |        ("SQ_1"."CS_ORDER_NUMBER") AS "SQ_2_COL_0",
       |        ("SQ_1"."CS_SOLD_DATE_SK") AS "SQ_2_COL_1"
       |      FROM
       |        (
       |        SELECT
       |          *
       |        FROM
       |          (
       |          SELECT
       |            *
       |          FROM
       |            "PUBLIC"."catalog_sales" AS "RCQ_ALIAS"
       |          ) AS "SQ_0"
       |        WHERE
       |          ("SQ_0"."CS_SOLD_DATE_SK" IS NOT NULL)
       |        ) AS "SQ_1"
       |      ) AS "SQ_2"
       |      INNER JOIN (
       |      SELECT
       |        ("SQ_4"."D_DATE_SK") AS "SQ_5_COL_0"
       |      FROM
       |        (
       |        SELECT
       |          *
       |        FROM
       |          (
       |          SELECT
       |            *
       |          FROM
       |            "PUBLIC"."date_dim" AS "RCQ_ALIAS"
       |          ) AS "SQ_3"
       |        WHERE
       |          (
       |          (
       |            ("SQ_3"."D_DATE" IS NOT NULL)
       |            AND (
       |            (
       |              "SQ_3"."D_DATE" >= DATEADD(
       |              day,
       |              19723,
       |              TO_DATE('1970-01-01', 'YYYY-MM-DD')
       |              )
       |            )
       |            AND (
       |              "SQ_3"."D_DATE" <= DATEADD(day, 19732, TO_DATE('1970-01-01', 'YYYY-MM-DD'))
       |            )
       |            )
       |          )
       |          AND (
       |            "SQ_3"."D_DATE_SK" IS NOT NULL
       |          )
       |          )
       |        ) AS "SQ_4"
       |      ) AS "SQ_5" ON (
       |      "SQ_2"."SQ_2_COL_1" = "SQ_5"."SQ_5_COL_0"
       |      )
       |    ) AS "SQ_6"
       |  )""".stripMargin)

  test("LST-Delete-2") {
    read.option("dbtable", s"catalog_sales").load()
      .createOrReplaceTempView(s"catalog_sales")
    read.option("dbtable", s"catalog_returns").load()
      .createOrReplaceTempView(s"catalog_returns")
    read.option("dbtable", s"date_dim").load()
      .createOrReplaceTempView(s"date_dim")
    checkAnswer(sqlContext.sql("select count(*) from catalog_returns"), Seq(Row(20)))
    doTest(sqlContext, testDelete2)
    // rows with cr_order_numbers 1 through 10 should be deleted
    checkAnswer(
      sqlContext.sql("select cr_order_number from catalog_returns order by cr_order_number desc"),
      Seq(
        Row(1234567909),
        Row(1234567908),
        Row(1234567907),
        Row(1234567906),
        Row(1234567905),
        Row(1234567904),
        Row(1234567903),
        Row(1234567902),
        Row(1234567901),
        Row(1234567900)))
  }

  val testInsert1: TestCase = TestCase(
    s"""INSERT
       |    INTO
       |        inventory_copy SELECT
       |            INV_ITEM_SK,
       |            INV_WAREHOUSE_SK,
       |            INV_QUANTITY_ON_HAND,
       |            INV_DATE_SK
       |        FROM
       |            inventory;""".stripMargin,
    Seq(Row()),
    s"""INSERT INTO "PUBLIC"."inventory_copy"
       |SELECT ( Cast ("SQ_14"."sq_14_col_0" AS INTEGER) ) AS "SQ_15_COL_0",
       |       ( Cast ("SQ_14"."sq_14_col_1" AS INTEGER) ) AS "SQ_15_COL_1",
       |       ( Cast ("SQ_14"."sq_14_col_2" AS INTEGER) ) AS "SQ_15_COL_2",
       |       ( Cast ("SQ_14"."sq_14_col_3" AS INTEGER) ) AS "SQ_15_COL_3"
       |FROM   (SELECT ( "SQ_13"."inv_item_sk" )          AS "SQ_14_COL_0",
       |               ( "SQ_13"."inv_warehouse_sk" )     AS "SQ_14_COL_1",
       |               ( "SQ_13"."inv_quantity_on_hand" ) AS "SQ_14_COL_2",
       |               ( "SQ_13"."inv_date_sk" )          AS "SQ_14_COL_3"
       |        FROM   (SELECT *
       |                FROM   "PUBLIC"."inventory" AS "RCQ_ALIAS") AS "SQ_13") AS
       |       "SQ_14""""
      .stripMargin,
    s"""INSERT INTO "PUBLIC"."inventory_copy"
       |SELECT ( Cast ("SQ_2"."sq_2_col_0" AS INTEGER) ) AS "SQ_3_COL_0",
       |       ( Cast ("SQ_2"."sq_2_col_1" AS INTEGER) ) AS "SQ_3_COL_1",
       |       ( Cast ("SQ_2"."sq_2_col_2" AS INTEGER) ) AS "SQ_3_COL_2",
       |       ( Cast ("SQ_2"."sq_2_col_3" AS INTEGER) ) AS "SQ_3_COL_3"
       |FROM   (SELECT ( "SQ_1"."inv_item_sk" )          AS "SQ_2_COL_0",
       |               ( "SQ_1"."inv_warehouse_sk" )     AS "SQ_2_COL_1",
       |               ( "SQ_1"."inv_quantity_on_hand" ) AS "SQ_2_COL_2",
       |               ( "SQ_1"."inv_date_sk" )          AS "SQ_2_COL_3"
       |        FROM   (SELECT *
       |                FROM   "PUBLIC"."inventory" AS "RCQ_ALIAS") AS "SQ_1") AS "SQ_2""""
      .stripMargin)

  test("LST-Insert-1") {
    read
      .option("dbtable", s"inventory")
      .load()
      .createOrReplaceTempView(s"inventory")
    read
      .option("dbtable", s"inventory_copy")
      .load()
      .createOrReplaceTempView(s"inventory_copy")
    checkAnswer(sqlContext.sql("select count(*) from inventory_copy"), Seq(Row(0)))
    doTest(sqlContext, testInsert1)
    checkAnswer(sqlContext.sql("select count(*) from inventory_copy"), Seq(Row(20)))
    val source = sqlContext.sql("SELECT * FROM inventory ORDER BY" +
      " inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand,inv_date_sk").collect().toSeq
    val target = sqlContext.sql("SELECT * FROM inventory_copy ORDER BY" +
      " inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand,inv_date_sk").collect().toSeq
    assert(source === target)
  }

  val testMerge1: TestCase = TestCase(
    s"""MERGE INTO catalog_returns USING(
       |    SELECT
       |        cs_order_number
       |    FROM
       |        catalog_sales,
       |        date_dim
       |    WHERE
       |        cs_sold_date_sk = d_date_sk
       |        AND d_date BETWEEN '2024-01-01'
       |        AND '2024-01-21'
       |) SOURCE ON cr_order_number = cs_order_number
       |WHEN MATCHED THEN DELETE;""".stripMargin,
    Seq(Row()),
    s"""DELETE FROM
       |  "PUBLIC"."catalog_returns" USING (
       |    SELECT
       |      ("SQ_7"."SQ_7_COL_0") AS "SQ_8_COL_0"
       |    FROM
       |      (
       |        SELECT
       |          ("SQ_3"."SQ_3_COL_0") AS "SQ_7_COL_0",
       |          ("SQ_3"."SQ_3_COL_1") AS "SQ_7_COL_1",
       |          ("SQ_6"."SQ_6_COL_0") AS "SQ_7_COL_2"
       |        FROM
       |          (
       |            SELECT
       |              ("SQ_2"."CS_ORDER_NUMBER") AS "SQ_3_COL_0",
       |              ("SQ_2"."CS_SOLD_DATE_SK") AS "SQ_3_COL_1"
       |            FROM
       |              (
       |                SELECT
       |                  *
       |                FROM
       |                  (
       |                    SELECT
       |                      *
       |                    FROM
       |                      "PUBLIC"."catalog_sales" AS "RCQ_ALIAS"
       |                  ) AS "SQ_1"
       |                WHERE
       |                  ("SQ_1"."CS_SOLD_DATE_SK" IS NOT NULL)
       |              ) AS "SQ_2"
       |          ) AS "SQ_3"
       |          INNER JOIN (
       |            SELECT
       |              ("SQ_5"."D_DATE_SK") AS "SQ_6_COL_0"
       |            FROM
       |              (
       |                SELECT
       |                  *
       |                FROM
       |                  (
       |                    SELECT
       |                      *
       |                    FROM
       |                      "PUBLIC"."date_dim" AS "RCQ_ALIAS"
       |                  ) AS "SQ_4"
       |                WHERE
       |                  (
       |                    (
       |                      ("SQ_4"."D_DATE" IS NOT NULL)
       |                      AND (
       |                        (
       |                          "SQ_4"."D_DATE" >= DATEADD(day, 19723, TO_DATE('1970-01-01', 'YYYY-MM-DD'))
       |                        )
       |                        AND (
       |                          "SQ_4"."D_DATE" <= DATEADD(day, 19743, TO_DATE('1970-01-01', 'YYYY-MM-DD'))
       |                        )
       |                      )
       |                    )
       |                    AND ("SQ_4"."D_DATE_SK" IS NOT NULL)
       |                  )
       |              ) AS "SQ_5"
       |          ) AS "SQ_6" ON (
       |            "SQ_3"."SQ_3_COL_1" = "SQ_6"."SQ_6_COL_0"
       |          )
       |      ) AS "SQ_7"
       |  ) AS "SQ_8"
       |WHERE
       |  (
       |    "PUBLIC"."CATALOG_RETURNS"."CR_ORDER_NUMBER" = "SQ_8"."SQ_8_COL_0"
       |  )""".stripMargin)

  test("LST-Merge-1") {
    read
      .option("dbtable", s"date_dim")
      .load()
      .createOrReplaceTempView(s"date_dim")
    read
      .option("dbtable", s"catalog_sales")
      .load()
      .createOrReplaceTempView(s"catalog_sales")
    read
      .option("dbtable", s"catalog_returns")
      .load()
      .createOrReplaceTempView(s"catalog_returns")
    checkAnswer(
      sqlContext.sql("SELECT COUNT(*) FROM catalog_returns"),
      Seq(Row(20)))
    doTest(sqlContext, testMerge1)
    checkAnswer(
      sqlContext.sql("SELECT COUNT(*) FROM catalog_returns"),
      Seq(Row(10)))
  }

  val testMerge2: TestCase = TestCase(
    s"""MERGE INTO catalog_sales USING(
       |    SELECT
       |        *
       |    FROM
       |        (
       |            SELECT
       |                MIN(d_date_sk) AS min_date
       |            FROM
       |                date_dim
       |            WHERE
       |                d_date BETWEEN '2024-01-01'
       |                AND '2024-01-21'
       |        ) r
       |        JOIN(
       |            SELECT
       |                MAX(d_date_sk) AS max_date
       |            FROM
       |                date_dim
       |            WHERE
       |                d_date BETWEEN '2024-01-01'
       |                AND '2024-01-21'
       |        ) s
       |) SOURCE ON cs_sold_date_sk >= min_date
       |AND cs_sold_date_sk <= max_date
       |WHEN MATCHED THEN DELETE;""".stripMargin,
    Seq(Row()),
    s"""DELETE FROM
       |  "PUBLIC"."catalog_sales" USING (
       |    SELECT
       |      ("SQ_4"."SQ_4_COL_0") AS "SQ_9_COL_0",
       |      ("SQ_8"."SQ_8_COL_0") AS "SQ_9_COL_1"
       |    FROM
       |      (
       |        SELECT
       |          (MIN ("SQ_3"."SQ_3_COL_0")) AS "SQ_4_COL_0"
       |        FROM
       |          (
       |            SELECT
       |              ("SQ_2"."D_DATE_SK") AS "SQ_3_COL_0"
       |            FROM
       |              (
       |                SELECT
       |                  *
       |                FROM
       |                  (
       |                    SELECT
       |                      *
       |                    FROM
       |                      "PUBLIC"."date_dim" AS "RCQ_ALIAS"
       |                  ) AS "SQ_1"
       |                WHERE
       |                  (
       |                    ("SQ_1"."D_DATE" IS NOT NULL)
       |                    AND (
       |                      (
       |                        "SQ_1"."D_DATE" >= DATEADD(day, 19723, TO_DATE('1970-01-01', 'YYYY-MM-DD'))
       |                      )
       |                      AND (
       |                        "SQ_1"."D_DATE" <= DATEADD(day, 19743, TO_DATE('1970-01-01', 'YYYY-MM-DD'))
       |                      )
       |                    )
       |                  )
       |              ) AS "SQ_2"
       |          ) AS "SQ_3"
       |        LIMIT
       |          1
       |      ) AS "SQ_4"
       |      CROSS JOIN (
       |        SELECT
       |          (MAX ("SQ_7"."SQ_7_COL_0")) AS "SQ_8_COL_0"
       |        FROM
       |          (
       |            SELECT
       |              ("SQ_6"."D_DATE_SK") AS "SQ_7_COL_0"
       |            FROM
       |              (
       |                SELECT
       |                  *
       |                FROM
       |                  (
       |                    SELECT
       |                      *
       |                    FROM
       |                      "PUBLIC"."date_dim" AS "RCQ_ALIAS"
       |                  ) AS "SQ_5"
       |                WHERE
       |                  (
       |                    ("SQ_5"."D_DATE" IS NOT NULL)
       |                    AND (
       |                      (
       |                        "SQ_5"."D_DATE" >= DATEADD(day, 19723, TO_DATE('1970-01-01', 'YYYY-MM-DD'))
       |                      )
       |                      AND (
       |                        "SQ_5"."D_DATE" <= DATEADD(day, 19743, TO_DATE('1970-01-01', 'YYYY-MM-DD'))
       |                      )
       |                    )
       |                  )
       |              ) AS "SQ_6"
       |          ) AS "SQ_7"
       |        LIMIT
       |          1
       |      ) AS "SQ_8"
       |  ) AS "SQ_9"
       |WHERE
       |  (
       |    (
       |      "PUBLIC"."CATALOG_SALES"."CS_SOLD_DATE_SK" >= "SQ_9"."SQ_9_COL_0"
       |    )
       |    AND (
       |      "PUBLIC"."CATALOG_SALES"."CS_SOLD_DATE_SK" <= "SQ_9"."SQ_9_COL_1"
       |    )
       |  )""".stripMargin)

  test("LST-Merge-2") {
    read
      .option("dbtable", s"date_dim")
      .load()
      .createOrReplaceTempView(s"date_dim")
    read
      .option("dbtable", s"catalog_sales")
      .load()
      .createOrReplaceTempView(s"catalog_sales")
    checkAnswer(
      sqlContext.sql("SELECT COUNT(*) FROM catalog_sales"),
      Seq(Row(20)))
    doTest(sqlContext, testMerge2)
    checkAnswer(
      sqlContext.sql("SELECT COUNT(*) FROM catalog_sales"),
      Seq(Row(10)))
  }
}