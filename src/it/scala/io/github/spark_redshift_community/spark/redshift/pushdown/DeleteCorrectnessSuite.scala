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


class DeleteCorrectnessSuite extends IntegrationPushdownSuiteBase {
  // These tests cannot disable pushdown since delete happens in pushdown
  override protected val auto_pushdown: String = "true"
  // These tests cannot use cache since they check the result changing
  override val s3_result_cache: String = "false"

  test("Delete from removes all rows from table") {
    withTempRedshiftTable("deleteStar") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table ${tableName} (id int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)
      val ds = sqlContext.range(0, 10)
      write(ds).option("dbtable", tableName).mode("append").save()

      checkAnswer(sqlContext.sql(s"select count(*) from ${tableName}"), Seq(Row(10)))

      doTest(
        sqlContext,
        TestCase(
          s"delete from ${tableName}",
          Seq(Row()),
          s"""DELETE FROM "PUBLIC"."${tableName}" WHERE true"""
        )
      )

      checkAnswer(sqlContext.sql(s"select count(*) from ${tableName}"), Seq(Row(0)))
    }
  }

  test("delete from removes selected rows from table") {
    withTempRedshiftTable("deleteTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      checkAnswer(sqlContext.sql(s"select count(*) from ${tableName}"), Seq(Row(3)))

      doTest(
        sqlContext,
        TestCase(
          s"delete from $tableName where id = 2",
          Seq(Row()),
          s"""DELETE FROM "PUBLIC"."${tableName}"
             | WHERE ("PUBLIC"."${tableName}"."ID" = 2)""".stripMargin
        )
      )

      checkAnswer(
        sqlContext.sql(s"select id from ${tableName} order by id asc"),
        Seq(Row(1), Row(3))
      )
    }
  }

  test("delete from removes rows in table with an alias for tablename") {
    withTempRedshiftTable("deleteTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      checkAnswer(sqlContext.sql(s"select count(*) from ${tableName}"), Seq(Row(3)))

      doTest(
        sqlContext,
        TestCase(
          s"delete from $tableName as newTable where newTable.id = 1",
          Seq(Row()),
          s"""DELETE FROM "PUBLIC"."$tableName" WHERE
             | ( "PUBLIC"."$tableName"."ID" = 1 )""".stripMargin
        )
      )

      checkAnswer(
        sqlContext.sql(s"select id from ${tableName} order by id asc"),
        Seq(Row(2), Row(3))
      )
    }
  }

  test("Delete rows using a subquery IN") {
    withTempRedshiftTable("deleteTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE $tableName (id INT, value INT)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      checkAnswer(
        sqlContext.sql(s"select count(*) from $tableName"),
        Seq(Row(3))
      )
      val query = s"DELETE FROM $tableName WHERE id IN (SELECT id FROM $tableName WHERE value > 20)"
      sqlContext.sql(query)

      val testTableName = s""""PUBLIC"."${tableName}""""
      checkSqlStatement(
        s"""DELETE FROM $testTableName WHERE ( $testTableName."ID" ) IN
           | ( SELECT ( "SQ_1"."ID" ) AS "SQ_2_COL_0" FROM
           | ( SELECT * FROM ( SELECT * FROM $testTableName AS
           | "RCQ_ALIAS" ) AS "SQ_0" WHERE
           | ( ( "SQ_0"."VALUE" IS NOT NULL ) AND (
           | "SQ_0"."VALUE" > 20 ) ) ) AS "SQ_1" )""".stripMargin
      )
      // expect only row with id=3 to be deleted
      checkAnswer(
        sqlContext.sql(s"select id from $tableName where id <= 20 order by id asc"),
        Seq(Row(1), Row(2))
      )
    }
  }

  test("Delete rows using a subquery NOT IN") {
    withTempRedshiftTable("deleteTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE $tableName (id INT, value INT)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      checkAnswer(
        sqlContext.sql(s"select count(*) from $tableName"),
        Seq(Row(3))
      )
      val query = s"DELETE FROM $tableName WHERE id NOT" +
        s" IN (SELECT id FROM $tableName WHERE value > 20)"
      sqlContext.sql(query)

      val testTableName = s""""PUBLIC"."${tableName}""""
      checkSqlStatement(
        s"""DELETE FROM $testTableName WHERE NOT ( ( $testTableName."ID" ) IN (
           | SELECT ( "SQ_1"."ID" ) AS "SQ_2_COL_0" FROM (
           | SELECT * FROM ( SELECT * FROM $testTableName AS
           | "RCQ_ALIAS" ) AS "SQ_0" WHERE ( (
           | "SQ_0"."VALUE" IS NOT NULL ) AND (
           | "SQ_0"."VALUE" > 20 ) ) ) AS "SQ_1" ) )""".stripMargin
      )
      // expect row with id=1, id=2 to be deleted
      checkAnswer(
        sqlContext.sql(s"select id from $tableName"),
        Seq(Row(3))
      )
    }
  }

  test("Delete rows using a subquery IN with two values") {
    withTempRedshiftTable("deleteTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE $tableName (id INT, value INT, name VARCHAR(50))"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10, "a"), (2, 20, "b"), (3, 30, "c"))
      val schema = List("id", "value", "name")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      checkAnswer(
        sqlContext.sql(s"select count(*) from $tableName"),
        Seq(Row(3))
      )
      val query =
        s"""DELETE FROM $tableName
           |WHERE (id, value) IN
           | (SELECT id, value FROM $tableName WHERE id != 2 AND value > 20)
           |""".stripMargin
      sqlContext.sql(query)

      val testTableName = s""""PUBLIC"."${tableName}""""
      checkSqlStatement(
        s"""DELETE FROM $testTableName WHERE ( $testTableName."ID" , $testTableName."VALUE" ) IN
           |( SELECT ( "SQ_1"."ID" ) AS "SQ_2_COL_0" , ( "SQ_1"."VALUE" )
           |AS "SQ_2_COL_1" FROM ( SELECT * FROM ( SELECT * FROM $testTableName
           |AS "RCQ_ALIAS" ) AS "SQ_0" WHERE ( ( ( "SQ_0"."ID"
           |IS NOT NULL ) AND ( "SQ_0"."VALUE" IS NOT NULL ) ) AND
           |( ( "SQ_0"."ID" != 2 ) AND ( "SQ_0"."VALUE" > 20 ) ) ) )
           |AS "SQ_1" )""".stripMargin
      )
      // expect only row with id=3 to be deleted
      checkAnswer(
        sqlContext.sql(s"select id from $tableName order by id asc"),
        Seq(Row(1), Row(2))
      )
    }
  }

  test("Delete rows using a subquery NOT IN with two values") {
    withTempRedshiftTable("deleteTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE $tableName (id INT, value INT, name VARCHAR(50))"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10, "a"), (2, 20, "b"), (3, 30, "c"))
      val schema = List("id", "value", "name")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      checkAnswer(
        sqlContext.sql(s"select count(*) from $tableName"),
        Seq(Row(3))
      )
      val query =
        s"""DELETE FROM $tableName
           |WHERE (id, value) NOT IN
           | (SELECT id, value FROM $tableName WHERE id != 2 AND value > 20)
           |""".stripMargin
      sqlContext.sql(query)

      val testTableName = s""""PUBLIC"."${tableName}""""
      checkSqlStatement(
        s"""DELETE FROM $testTableName WHERE NOT ( ( $testTableName."ID" ,
           | $testTableName."VALUE" ) IN ( SELECT ( "SQ_1"."ID" ) AS
           | "SQ_2_COL_0" , ( "SQ_1"."VALUE" ) AS "SQ_2_COL_1"
           | FROM ( SELECT * FROM ( SELECT * FROM $testTableName AS
           | "RCQ_ALIAS" ) AS "SQ_0" WHERE ( ( (
           | "SQ_0"."ID" IS NOT NULL ) AND ( "SQ_0"."VALUE"
           | IS NOT NULL ) ) AND ( ( "SQ_0"."ID" != 2 ) AND
           | ( "SQ_0"."VALUE" > 20 ) ) ) ) AS "SQ_1" ) )""".stripMargin
      )
      // expect rows with id 1 and 2 to be deleted
      checkAnswer(
        sqlContext.sql(s"select id from $tableName order by id asc"),
        Seq(Row(3))
      )
    }
  }

  ignore("Delete rows using a nested subquery IN CASE") {
    withTempRedshiftTable("orders") { tableName =>

      val createQuery =
        s"""CREATE TABLE IF NOT EXISTS $tableName (
           |  order_id INT,
           |  name VARCHAR(10)
           |)""".stripMargin

      redshiftWrapper.executeUpdate(conn, createQuery)
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)
      val initialData = Seq((1, "car"), (11, "pen"), (111, "ring"))
      val schema = List("order_id", "name")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()
      checkAnswer(
        sqlContext.sql(s"select count(*) from $tableName"),
        Seq(Row(3))
      )
      val query =
        s"""
           |DELETE FROM $tableName
           |WHERE order_id IN (
           |  SELECT order_id
           |  FROM (
           |    SELECT order_id,
           |           CASE WHEN name LIKE 'car' THEN 1 ELSE 0 END AS delete_flag
           |    FROM $tableName
           |  )
           |  WHERE delete_flag = 1
           |)
         """.stripMargin
      /*
      The following fails possibly because of the missing equalnullsafe (<=>) operator
      */
      sqlContext.sql(query)


      val testTableName = s""""PUBLIC"."${tableName}""""
      checkSqlStatement(
        s"""DELETE FROM $testTableName TBD""".stripMargin
      )
      // expect row with id=1 to be deleted
      checkAnswer(
        sqlContext.sql(s"select id from $tableName where value != 'car' order by id asc"),
        Seq(Row(11), Row(111))
      )
    }
  }

  test("Delete rows using a nested subquery HAVING") {
    withTempRedshiftTable("orders") { tableName =>
      withTempRedshiftTable("order_details") { tableName2 =>
        redshiftWrapper.executeUpdate(conn,
          s"""CREATE TABLE IF NOT EXISTS $tableName (
             |  order_id INT,
             |  name VARCHAR(10)
             |)""".stripMargin
        )
        redshiftWrapper.executeUpdate(conn,
          s"""
             |CREATE TABLE IF NOT EXISTS $tableName2 (
             |  order_id INT,
             |  product_id VARCHAR(10),
             |  sales_amount INT
             |)
             |""".stripMargin
        )
        read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)
        read.option("dbtable", tableName2).load.createOrReplaceTempView(tableName2)

        val initialData = Seq((1, "car"), (11, "pen"), (111, "ring"))
        val initialData2 = Seq((1, "car", 1), (1, "car", 1), (1, "car", 1),
          (1, "car", 1), (11, "pen", 11), (111, "ring", 111))
        val schema = List("order_id", "name")
        val schema2 = List("order_id", "product_id", "sales_amount")
        val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
        val df2 = sqlContext.createDataFrame(initialData2).toDF(schema2: _*)
        write(df).option("dbtable", tableName).mode("append").save()
        write(df2).option("dbtable", tableName2).mode("append").save()

        checkAnswer(
          sqlContext.sql(s"select count(*) from $tableName"),
          Seq(Row(3))
        )


        val testTableName = s""""PUBLIC"."${tableName}""""
        val testTableName2 = s""""PUBLIC"."${tableName2}""""
        doTest(
          sqlContext,
          TestCase(
            s"""
             |DELETE FROM $tableName
             |WHERE order_id IN (
             |  SELECT order_id
             |  FROM $tableName2
             |  GROUP BY order_id
             |  HAVING COUNT(*) > 3
             |)
             |""".stripMargin,
            Seq(Row()),
            s"""DELETE FROM
               |  $testTableName
               |WHERE
               |  (
               |    $testTableName."ORDER_ID"
               |  ) IN (
               |    SELECT
               |      ("SQ_3"."SQ_2_COL_0") AS "SQ_4_COL_0"
               |    FROM
               |      (
               |        SELECT
               |          *
               |        FROM
               |          (
               |            SELECT
               |              ("SQ_1"."SQ_1_COL_0") AS "SQ_2_COL_0",
               |              (COUNT (1)) AS "SQ_2_COL_1"
               |            FROM
               |              (
               |                SELECT
               |                  ("SQ_0"."ORDER_ID") AS "SQ_1_COL_0"
               |                FROM
               |                  (
               |                    SELECT
               |                      *
               |                    FROM
               |                      $testTableName2 AS "RCQ_ALIAS"
               |                  ) AS "SQ_0"
               |              ) AS "SQ_1"
               |            GROUP BY
               |              "SQ_1"."SQ_1_COL_0"
               |          ) AS "SQ_2"
               |        WHERE
               |          ("SQ_2"."SQ_2_COL_1" > 3)
               |      ) AS "SQ_3"
               |  )""".stripMargin
          )
        )

        // expect the row with order_id=1 to be deleted
        checkAnswer(
          sqlContext.sql(
            s"""select order_id from $tableName order by order_id asc""".stripMargin),
          Seq(Row(11), Row(111))
        )
      }
    }
  }

  test("Delete rows using a nested subquery EXISTS") {
    withTempRedshiftTable("orders") { tableName =>
      withTempRedshiftTable("order_details") { tableName2 =>
        redshiftWrapper.executeUpdate(conn,
          s"""CREATE TABLE IF NOT EXISTS $tableName (
             |  order_id INT,
             |  name VARCHAR(10)
             |)""".stripMargin
        )
        redshiftWrapper.executeUpdate(conn,
          s"""
             |CREATE TABLE IF NOT EXISTS $tableName2 (
             |  order_id INT,
             |  product_id VARCHAR(10),
             |  sales_amount INT
             |)
             |""".stripMargin
        )
        read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)
        read.option("dbtable", tableName2).load.createOrReplaceTempView(tableName2)

        val initialData = Seq((1, "car"), (11, "pen"), (111, "ring"))
        val initialData2 = Seq((1, "car1", 1), (11, "pen11", 11), (111, "ring111", 111))
        val schema = List("order_id", "name")
        val schema2 = List("order_id", "product_id", "sales_amount")
        val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
        val df2 = sqlContext.createDataFrame(initialData2).toDF(schema2: _*)
        write(df).option("dbtable", tableName).mode("append").save()
        write(df2).option("dbtable", tableName2).mode("append").save()

        checkAnswer(
          sqlContext.sql(s"select count(*) from $tableName"),
          Seq(Row(3))
        )

        doTest(
          sqlContext,
          TestCase(
            s"""
             |DELETE FROM $tableName o
             |WHERE EXISTS (
             |  SELECT order_id
             |  FROM $tableName2 od
             |  WHERE o.order_id = od.order_id
             |  AND od.product_id = 'pen11'
             |)
             |""".stripMargin,
            Seq(Row()),
            s"""DELETE FROM "PUBLIC"."$tableName" WHERE EXISTS ( SELECT (
               | "SQ_1"."ORDER_ID" ) AS "SQ_2_COL_0" FROM ( SELECT * FROM (
               |  SELECT * FROM "PUBLIC"."$tableName2" AS
               |   "RCQ_ALIAS" ) AS "SQ_0" WHERE ( (
               |    "SQ_0"."PRODUCT_ID" IS NOT NULL ) AND (
               |     "SQ_0"."PRODUCT_ID" = 'pen11' ) ) ) AS "SQ_1" WHERE
               |      ( "PUBLIC"."$tableName"."ORDER_ID" = "SQ_2_COL_0"
               |       ) )""".stripMargin
          )
        )
        // expect the row with product_id='pen11' to be deleted
        checkAnswer(
          sqlContext.sql(s"""select order_id from $tableName order by order_id asc""".stripMargin),
          Seq(Row(1), Row(111))
        )
      }
    }
  }

  test("Delete rows using a nested subquery NOT EXISTS") {
    withTempRedshiftTable("orders") { tableName =>
      withTempRedshiftTable("order_details") { tableName2 =>
        redshiftWrapper.executeUpdate(conn,
          s"""CREATE TABLE IF NOT EXISTS $tableName (
             |  order_id INT,
             |  name VARCHAR(10)
             |)""".stripMargin
        )
        redshiftWrapper.executeUpdate(conn,
          s"""
             |CREATE TABLE IF NOT EXISTS $tableName2 (
             |  order_id INT,
             |  product_id VARCHAR(10),
             |  sales_amount INT
             |)
             |""".stripMargin
        )
        read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)
        read.option("dbtable", tableName2).load.createOrReplaceTempView(tableName2)

        val initialData = Seq((1, "car"), (11, "pen"), (111, "ring"))
        val initialData2 = Seq((1, "car1", 1), (11, "pen11", 11), (111, "ring111", 111))
        val schema = List("order_id", "name")
        val schema2 = List("order_id", "product_id", "sales_amount")
        val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
        val df2 = sqlContext.createDataFrame(initialData2).toDF(schema2: _*)
        write(df).option("dbtable", tableName).mode("append").save()
        write(df2).option("dbtable", tableName2).mode("append").save()

        checkAnswer(
          sqlContext.sql(s"select count(*) from $tableName"),
          Seq(Row(3))
        )

        doTest(
          sqlContext,
          TestCase(
            s"""
             |DELETE FROM $tableName o
             |WHERE NOT EXISTS (
             |  SELECT 1
             |  FROM $tableName2 od
             |  WHERE o.order_id = od.order_id
             |  AND od.product_id = 'pen11'
             |)
             |""".stripMargin,
            Seq(Row()),
            s"""DELETE FROM "PUBLIC"."$tableName" WHERE NOT ( EXISTS ( SELECT ( 1 ) AS
               | "SQ_2_COL_0" , ( "SQ_1"."ORDER_ID" ) AS "SQ_2_COL_1" FROM
               |  ( SELECT * FROM ( SELECT * FROM "PUBLIC"."$tableName2" AS "RCQ_ALIAS"
               |   ) AS "SQ_0" WHERE ( ( "SQ_0"."PRODUCT_ID" IS NOT NULL ) AND
               |    ( "SQ_0"."PRODUCT_ID" = 'pen11' ) ) ) AS "SQ_1" WHERE (
               |    "PUBLIC"."$tableName"."ORDER_ID" = "SQ_2_COL_1" ) ) )""".stripMargin
          )
        )
        // expect the row with product_id='car1' or 'pen111' to be deleted
        checkAnswer(
          sqlContext.sql(s"""select order_id from $tableName""".stripMargin),
          Seq(Row(11))
        )
      }
    }
  }

  ignore("Delete rows using WITH") {
    withTempRedshiftTable("deleteTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      checkAnswer(
        sqlContext.sql(s"select count(*) from $tableName"),
        Seq(Row(3))
      )
      sqlContext.sql(
        s"""
           |WITH t2 AS (SELECT 1 AS id UNION SELECT 100 AS id)
           |DELETE FROM $tableName
           |WHERE id IN (SELECT id FROM t2)
           |""".stripMargin)

      val testTableName = s""""PUBLIC"."${tableName}""""
      checkSqlStatement(
        s"""TBD""".stripMargin
      )

      // expect the row with id=1 to be deleted
      checkAnswer(
        sqlContext.sql(s"select id from $tableName"),
        Seq(Row(2), Row(3))
      )
    }
  }

  test("Negative test for Delete using 'query' option instead of 'dbtable'") {
    withTempRedshiftTable("deleteStar") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int)"
      )
      read.option("query", s"select * from $tableName").load.createOrReplaceTempView(tableName)
      val ds = sqlContext.range(0, 10)
      write(ds).option("dbtable", tableName).mode("append").save()

      assertThrows[Exception] {
        sqlContext.sql(s"delete from $tableName")
      }
    }
  }

  test("Test deleting for a value in a sub-query") {
    withTempRedshiftTable("deleteTarget") { targetTable =>
      withTempRedshiftTable("deleteSource") { sourceTable =>
        // Arrange
        redshiftWrapper.executeUpdate(conn, s"create table $targetTable (id int)")
        redshiftWrapper.executeUpdate(conn, s"create table $sourceTable (id int)")

        val targetDs = sqlContext.range(0, 10)
        write(targetDs).option("dbtable", s"$targetTable").mode("append").save()

        val sourceDs = sqlContext.range(3, 6)
        write(sourceDs).option("dbtable", s"$sourceTable").mode("append").save()

        read.option("dbtable", s"$targetTable").load.createOrReplaceTempView(s"$targetTable")
        read.option("dbtable", s"$sourceTable").load.createOrReplaceTempView(s"$sourceTable")
        checkAnswer(
          sqlContext.sql(s"select count(*) from $targetTable"),
          Seq(Row(10))
        )
        // Act
        doTest(
          sqlContext,
          TestCase(
            s"delete from $targetTable as newTable where newTable.id in" +
              s" (select id from $sourceTable where id > 3)",
            Seq(Row()),
            s"""DELETE FROM "PUBLIC"."$targetTable" WHERE
               | ( "PUBLIC"."$targetTable"."ID" ) IN ( SELECT * FROM ( SELECT * FROM
               |  "PUBLIC"."$sourceTable" AS "RCQ_ALIAS" ) AS
               |   "SQ_0" WHERE ( ( "SQ_0"."ID" IS NOT NULL ) AND (
               |    "SQ_0"."ID" > 3 ) ) )""".stripMargin
          )
        )

        // Assert
        checkAnswer(
          sqlContext.sql(s"select id from $targetTable"),
          Seq(Row(0), Row(1), Row(2), Row(3), Row(6), Row(7), Row(8), Row(9))
        )
      }
    }
  }

  test("Test deleting for a value not in a sub-query") {
    withTempRedshiftTable("deleteTarget") { targetTable =>
      withTempRedshiftTable("deleteSource") { sourceTable =>
        // Arrange
        redshiftWrapper.executeUpdate(conn, s"create table $targetTable (id int)")
        redshiftWrapper.executeUpdate(conn, s"create table $sourceTable (id int)")

        val targetDs = sqlContext.range(0, 10)
        write(targetDs).option("dbtable", s"$targetTable").mode("append").save()

        val sourceDs = sqlContext.range(3, 6)
        write(sourceDs).option("dbtable", s"$sourceTable").mode("append").save()

        read.option("dbtable", s"$targetTable").load.createOrReplaceTempView(s"$targetTable")
        read.option("dbtable", s"$sourceTable").load.createOrReplaceTempView(s"$sourceTable")
        checkAnswer(
          sqlContext.sql(s"select count(*) from $targetTable"),
          Seq(Row(10))
        )

        // Act
        doTest(
          sqlContext,
          TestCase(
            s"delete from $targetTable as newTable where newTable.id" +
              s" not in (select id from $sourceTable where id > 3)",
            Seq(Row()),
            s"""DELETE FROM "PUBLIC"."$targetTable" WHERE NOT (
               | ( "PUBLIC"."$targetTable"."ID" ) IN ( SELECT * FROM ( SELECT * FROM
               |  "PUBLIC"."$sourceTable" AS "RCQ_ALIAS" ) AS
               |   "SQ_0" WHERE ( ( "SQ_0"."ID" IS NOT NULL ) AND (
               |    "SQ_0"."ID" > 3 ) ) ))""".stripMargin
          )
        )

        // Assert
        checkAnswer(
          sqlContext.sql(s"select id from $targetTable"),
          Seq(Row(4), Row(5))
        )
      }
    }
  }

  test("Test where exist clause with nested join") {
    withTempRedshiftTable("deleteTable1") { table1 =>
      withTempRedshiftTable("deleteTable2") { table2 =>
        redshiftWrapper.executeUpdate(conn, s"create table $table1 (id int, name VARCHAR)" )
        redshiftWrapper.executeUpdate(conn, s"create table $table2 (id int, t1_id int, name VARCHAR)" )

        read.option("dbtable", table1).load.createOrReplaceTempView(table1)
        read.option("dbtable", table2).load.createOrReplaceTempView(table2)

        val table1Schema = List("id", "name")
        val table1Data = Seq((0, "zero"), (1, "one"), (2, "two"))
        val dfTable1 = sqlContext.createDataFrame(table1Data).toDF(table1Schema: _*)
        write(dfTable1).option("dbtable", table1).mode("append").save()

        val table2Schema = List("id", "t1_id", "name")
        val table2Data = Seq((10, 0, "ten"), (11, 7, "eleven"), (12, 2, "twelve"))
        val dfTable2 = sqlContext.createDataFrame(table2Data).toDF(table2Schema: _*)
        write(dfTable2).option("dbtable", table2).mode("append").save()

        val pre = sqlContext.sql(s"select * from $table1").count
        assert(pre == 3)

        sqlContext.sql(
          s"""DELETE
             | FROM $table1 t1
             | WHERE EXISTS (SELECT 1
             |               FROM $table2 t2
             |               WHERE t2.t1_id = t1.id)
             |""".stripMargin)

        checkSqlStatement(
          s"""DELETE FROM "PUBLIC"."$table1" WHERE EXISTS ( SELECT ( 1 ) AS "SQ_1_COL_0" ,
             |  ( "SQ_0"."T1_ID" ) AS "SQ_1_COL_1" FROM ( SELECT * FROM "PUBLIC"."$table2"
             |  AS "RCQ_ALIAS" ) AS "SQ_0"
             |  WHERE ( "SQ_1_COL_1" = "PUBLIC"."$table1"."ID" ) )"""
            .stripMargin
        )

        val post = sqlContext.sql(s"select * from $table1").collect().map(row => row.toSeq).toSeq
        val expected = Array(Array(1, "one"))
        post should contain theSameElementsAs expected
      }
    }
  }
}