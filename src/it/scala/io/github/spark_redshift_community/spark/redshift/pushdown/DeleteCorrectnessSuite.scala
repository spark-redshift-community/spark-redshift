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


trait DeleteCorrectnessSuite extends IntegrationPushdownSuiteBase {
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

      val pre = sqlContext.sql(s"select * from ${tableName}").count

      sqlContext.sql(s"delete from ${tableName}")

      val post = sqlContext.sql(s"select * from ${tableName}").count

      assert(pre == 10)
      assert(post == 0)
    }
  }

  test("delete from removes selected rows from table") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      val pre = sqlContext.sql(s"select * from $tableName").count()

      sqlContext.sql(s"delete from $tableName where id = 2")

      val testTableName = s""""PUBLIC"."${tableName}""""
      checkSqlStatement(
        s"""DELETE FROM $testTableName WHERE ($testTableName."ID" = 2)""".stripMargin
      )

      val post = sqlContext.sql(s"select * from $tableName").count()

      assert(pre == 3)
      assert(post == 2)
    }
  }

  test("delete from removes rows in table with an alias for tablename") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      val pre = sqlContext.sql(s"select * from $tableName").count()

      sqlContext.sql(s"delete from $tableName as newTable where newTable.id = 1")

      val post = sqlContext.sql(s"select * from $tableName").count()

      assert(pre == 3)
      assert(post == 2)
    }
  }

  test("Delete_rows_using_a_subquery_IN") {
    withTempRedshiftTable("updateTable") { tableName =>
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
        s"""DELETE FROM $testTableName WHERE $testTableName."ID" IN
           | ( SELECT ( "SUBQUERY_1"."ID" ) AS "SUBQUERY_2_COL_0" FROM
           | ( SELECT * FROM ( SELECT * FROM $testTableName AS
           | "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
           | ( ( "SUBQUERY_0"."VALUE" IS NOT NULL ) AND (
           | "SUBQUERY_0"."VALUE" > 20 ) ) ) AS "SUBQUERY_1" )""".stripMargin
      )
      // expect only row with id=3 to be deleted
      checkAnswer(
        sqlContext.sql(s"select id from $tableName where id <= 20 order by id asc"),
        Seq(Row(1), Row(2))
      )
    }
  }

  test("Delete_rows_using_a_subquery_NOT_IN") {
    withTempRedshiftTable("updateTable") { tableName =>
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
      val query = s"DELETE FROM $tableName WHERE id NOT IN (SELECT id FROM $tableName WHERE value > 20)"
      sqlContext.sql(query)

      val testTableName = s""""PUBLIC"."${tableName}""""
      checkSqlStatement(
        s"""DELETE FROM $testTableName WHERE NOT ( $testTableName."ID" IN (
           | SELECT ( "SUBQUERY_1"."ID" ) AS "SUBQUERY_2_COL_0" FROM (
           | SELECT * FROM ( SELECT * FROM $testTableName AS
           | "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE ( (
           | "SUBQUERY_0"."VALUE" IS NOT NULL ) AND (
           | "SUBQUERY_0"."VALUE" > 20 ) ) ) AS "SUBQUERY_1" ) )""".stripMargin
      )
      // expect row with id=1, id=2 to be deleted
      checkAnswer(
        sqlContext.sql(s"select id from $tableName where id NOT IN (SELECT id FROM $tableName WHERE value <= 20) order by id asc"),
        Seq(Row(3))
      )
    }
  }

  ignore("Delete_rows_using_a_nested_subquery_IN_CASE") {
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
      println(s"main query: $query")
      /*
      The following fails possibly because of the missing equalnullsafe (<=>) operator
      */
      sqlContext.sql(query)
      // expect row with id=1 to be deleted
      checkAnswer(
        sqlContext.sql(s"select id from $tableName where value != 'car' order by id asc"),
        Seq(Row(11), Row(111))
      )
    }
  }

  test("Delete_rows_using_a_nested_subquery_HAVING") {
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
        val initialData2 = Seq((1, "car", 1), (1, "car", 1), (1, "car", 1), (1, "car", 1), (11, "pen", 11),
          (111, "ring", 111))
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
        val query =
          s"""
             |DELETE FROM $tableName
             |WHERE order_id IN (
             |  SELECT order_id
             |  FROM $tableName2
             |  GROUP BY order_id
             |  HAVING COUNT(*) > 3
             |)
             |""".stripMargin
        sqlContext.sql(query)
        // expect the row with order_id=1 to be deleted
        checkAnswer(
          sqlContext.sql(
            s"""select order_id from $tableName where order_id in
               | ( select order_id from $tableName2 group by order_id
               | having count(*) <= 3) order by order_id asc""".stripMargin),
          Seq(Row(11), Row(111))
        )
      }
    }
  }

  ignore("Delete_rows_using_a_nested_subquery_EXISTS") {
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
        val query =
          s"""
             |DELETE FROM $tableName o
             |WHERE EXISTS (
             |  SELECT 1
             |  FROM $tableName2 od
             |  WHERE o.order_id = od.order_id
             |  AND od.product_id = 'pen11'
             |)
             |""".stripMargin
        sqlContext.sql(query)
        // expect the row with product_id='pen11' to be deleted
        checkAnswer(
          sqlContext.sql(s"""select order_id from $tableName, $tableName2 where
                    o.order_id = od.order_id AND AND od.product_id != 'pen11'
                   order by order_id asc""".stripMargin),
          Seq(Row(1), Row(111))
        )
      }
    }
  }

  ignore("Delete_rows_using_a_nested_subquery_NOT_EXISTS") {
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
        val query =
          s"""
             |DELETE FROM $tableName o
             |WHERE NOT EXISTS (
             |  SELECT 1
             |  FROM $tableName2 od
             |  WHERE o.order_id = od.order_id
             |  AND od.product_id = 'pen11'
             |)
             |""".stripMargin
        sqlContext.sql(query)
        // expect the row with product_id='car1' or 'pen111' to be deleted
        checkAnswer(
          sqlContext.sql(s"""select order_id from $tableName o where not exists(
                    select 1 from $tableName2 od where o.order_id = od.order_id
                    and od.product_id != 'pen11') order by o.order_id asc""".stripMargin),
          Seq(Row(1), Row(111))
        )
      }
    }
  }

  ignore("Delete_rows_using_WITH") {
    withTempRedshiftTable("updateTable") { tableName =>
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
        val pre = sqlContext.sql(s"select * from $targetTable").count()

        // Act
        sqlContext.sql(s"delete from $targetTable as newTable where newTable.id in (select id from $sourceTable where id > 3)")

        // Assert
        val post = sqlContext.sql(s"select * from $targetTable").count()
        assert(pre == 10)
        assert(post == 8)
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
        val pre = sqlContext.sql(s"select * from $targetTable").count()

        // Act
        sqlContext.sql(s"delete from $targetTable as newTable where newTable.id not in (select id from $sourceTable where id > 3)")

        // Assert
        val post = sqlContext.sql(s"select * from $targetTable").count()
        assert(pre == 10)
        assert(post == 2)
      }
    }
  }
}

class TextDeleteCorrectnessSuite extends DeleteCorrectnessSuite {
  override val s3format = "TEXT"
}

class ParquetDeleteCorrectnessSuite extends DeleteCorrectnessSuite {
  override val s3format = "PARQUET"
}
