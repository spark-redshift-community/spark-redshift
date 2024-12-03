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

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{AnalysisException, DataFrameReader, Dataset, Row}

class InsertCorrectnessSuite extends IntegrationPushdownSuiteBase {
  // These tests cannot disable pushdown since insert happens in pushdown
  override protected val auto_pushdown: String = "true"
  // These tests cannot use cache since they check the result changing
  override val s3_result_cache: String = "false"

  private val source_table: String = s"read_suite_test_table_$randomSuffix"

  override def beforeAll(): Unit = {
    super.beforeAll()
    redshiftWrapper.executeUpdate(conn, s"drop table if exists $source_table")
    createTestDataInRedshift(source_table)
  }

  override def afterAll(): Unit = {
    try {
      redshiftWrapper.executeUpdate(conn, s"drop table if exists $source_table")
    } finally {
      super.afterAll()
    }
  }

  test("Push down insert literal values with all columns into the table") {
    withTempRedshiftTable("insertTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE ${tableName} (a int, b int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)
      val pre = sqlContext.sql(s"SELECT * FROM ${tableName}").count

      sqlContext.sql(s"INSERT INTO TABLE ${tableName} VALUES (1, 100), (3, 2000)")

      checkSqlStatement(
        s"""INSERT INTO "PUBLIC"."$tableName"
           | SELECT ( CAST ( "SUBQUERY_1"."COL1" AS INTEGER ) ) AS "SUBQUERY_2_COL_0" ,
           | ( CAST ( "SUBQUERY_1"."COL2" AS INTEGER ) ) AS "SUBQUERY_2_COL_1"
           | FROM ( ( (SELECT 1  AS "col1", 100  AS "col2")
           | UNION ALL (SELECT 3  AS "col1", 2000  AS "col2") ) ) AS "SUBQUERY_1"""".stripMargin
      )

      val post = sqlContext.sql(s"SELECT * FROM ${tableName}").collect().map(row => row.toSeq).toSeq

      assert(pre == 0)
      val expected = Array(Array(1, 100), Array(3, 2000))
      post should contain theSameElementsAs expected
    }
  }

  test("Push down insert duplicated literal values into the table") {
    withTempRedshiftTable("insertTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE ${tableName} (a int, b int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)
      val pre = sqlContext.sql(s"SELECT * FROM ${tableName}").count

      sqlContext.sql(s"INSERT INTO TABLE ${tableName} VALUES (1, 100), (1, 100)")

      checkSqlStatement(
        s"""INSERT INTO "PUBLIC"."$tableName"
           | SELECT ( CAST ( "SUBQUERY_1"."COL1" AS INTEGER ) ) AS "SUBQUERY_2_COL_0" ,
           | ( CAST ( "SUBQUERY_1"."COL2" AS INTEGER ) ) AS "SUBQUERY_2_COL_1" FROM
           | ( ( (SELECT 1  AS "col1", 100  AS "col2") UNION ALL
           | (SELECT 1  AS "col1", 100  AS "col2") ) ) AS "SUBQUERY_1"""".stripMargin
      )

      val post = sqlContext.sql(s"SELECT * FROM ${tableName}").collect().map(row => row.toSeq).toSeq

      assert(pre == 0)
      val expected = Array(Array(1, 100), Array(1, 100))
      post should contain theSameElementsAs expected
    }
  }

  test("Push down insert literal values with all columns in an order") {
    withTempRedshiftTable("insertTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE ${tableName} (a int, b int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)
      val pre = sqlContext.sql(s"SELECT * FROM ${tableName}").count

      sqlContext.sql(s"INSERT INTO ${tableName} (b, a) VALUES (100, 1), (2000,3)")

      checkSqlStatement(
        s"""INSERT INTO "PUBLIC"."$tableName"
           | SELECT ( CAST ( "SUBQUERY_2"."SUBQUERY_2_COL_1" AS INTEGER ) ) AS "SUBQUERY_3_COL_0" ,
           | ( CAST ( "SUBQUERY_2"."SUBQUERY_2_COL_0" AS INTEGER ) ) AS "SUBQUERY_3_COL_1"
           | FROM ( SELECT ( "SUBQUERY_1"."COL1" ) AS "SUBQUERY_2_COL_0" , ( "SUBQUERY_1"."COL2" ) AS "SUBQUERY_2_COL_1"
           | FROM ( ( (SELECT 100  AS "col1", 1  AS "col2") UNION ALL
           | (SELECT 2000  AS "col1", 3  AS "col2") ) ) AS "SUBQUERY_1" ) AS "SUBQUERY_2""""
          .stripMargin
      )

      val post = sqlContext.sql(s"SELECT * FROM ${tableName}").collect().map(row => row.toSeq).toSeq

      assert(pre == 0)
      val expected = Array(Array(1, 100), Array(3, 2000))
      post should contain theSameElementsAs expected
    }
  }

  test("Push down insert literal values with part of columns into the table") {
    withTempRedshiftTable("insertTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE ${tableName} (a int, b int, c varchar(256))"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)
      val pre = sqlContext.sql(s"SELECT * FROM ${tableName}").count

      sqlContext.sql(s"INSERT INTO ${tableName} (a, c) VALUES (100, '1'), (2000, '2')")

      checkSqlStatement(
        s"""INSERT INTO "PUBLIC"."$tableName"
           | SELECT ( CAST ( "SUBQUERY_2"."SUBQUERY_2_COL_0" AS INTEGER ) ) AS "SUBQUERY_3_COL_0" ,
           | ( NULL ) AS "SUBQUERY_3_COL_1" , ( CAST ( "SUBQUERY_2"."SUBQUERY_2_COL_1" AS VARCHAR ) ) AS "SUBQUERY_3_COL_2"
           | FROM ( SELECT ( "SUBQUERY_1"."COL1" ) AS "SUBQUERY_2_COL_0" , ( "SUBQUERY_1"."COL2" ) AS "SUBQUERY_2_COL_1"
           | FROM ( ( (SELECT 100  AS "col1", '1'  AS "col2") UNION ALL
           | (SELECT 2000  AS "col1", '2'  AS "col2") ) ) AS "SUBQUERY_1" ) AS "SUBQUERY_2""""
          .stripMargin
      )

      val post = sqlContext.sql(s"select * from ${tableName}").collect().map(row => row.toSeq).toSeq

      assert(pre == 0)
      val expected = Array(Array(100, null, "1"), Array(2000, null, "2"))
      post should contain theSameElementsAs expected
    }
  }

  test("Push down insert null values into the table") {
    withTempRedshiftTable("insertTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE ${tableName} (a bigint, b int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)
      val pre = sqlContext.sql(s"SELECT * FROM ${tableName}").count

      sqlContext.sql(s"INSERT INTO ${tableName} VALUES (null, null), (null, null)")

      checkSqlStatement(
        s"""INSERT INTO "PUBLIC"."$tableName"
           | SELECT ( CAST ( "SUBQUERY_1"."COL1" AS BIGINT ) ) AS "SUBQUERY_2_COL_0" ,
           | ( CAST ( "SUBQUERY_1"."COL2" AS INTEGER ) ) AS "SUBQUERY_2_COL_1"
           | FROM ( ( (SELECT NULL AS "col1", NULL AS "col2") UNION ALL
           | (SELECT NULL AS "col1", NULL AS "col2") ) ) AS "SUBQUERY_1""""
          .stripMargin
      )

      val post = sqlContext.sql(s"select * from ${tableName}").collect().map(row => row.toSeq).toSeq

      assert(pre == 0)
      val expected = Array(Array(null, null), Array(null, null))
      post should contain theSameElementsAs expected
    }
  }

  test("Push down insert literals and null values into the table") {
    withTempRedshiftTable("insertTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE ${tableName} (a bigint, b int, c int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)
      val pre = sqlContext.sql(s"SELECT * FROM ${tableName}").count

      sqlContext.sql(s"INSERT INTO ${tableName} VALUES (1, null, 1000), (2, 2000, null)")

      checkSqlStatement(
        s"""INSERT INTO "PUBLIC"."$tableName"
           | SELECT ( CAST ( "SUBQUERY_1"."COL1" AS BIGINT ) ) AS "SUBQUERY_2_COL_0" ,
           | ( CAST ( "SUBQUERY_1"."COL2" AS INTEGER ) ) AS "SUBQUERY_2_COL_1" ,
           | ( CAST ( "SUBQUERY_1"."COL3" AS INTEGER ) ) AS "SUBQUERY_2_COL_2"
           | FROM ( ( (SELECT 1  AS "col1", NULL  AS "col2", 1000  AS "col3") UNION ALL
           | (SELECT 2  AS "col1", 2000  AS "col2", NULL  AS "col3") ) )
           | AS "SUBQUERY_1""""
          .stripMargin
      )

      val post = sqlContext.sql(s"select * from ${tableName}").collect().map(row => row.toSeq).toSeq

      assert(pre == 0)
      val expected = Array(Array(1, null, 1000), Array(2, 2000, null))
      post should contain theSameElementsAs expected
    }
  }

  test("Push down insert rows from reading another table") {
    withTempRedshiftTable("insertTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE ${tableName} (a int, b int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)
      read.option("dbtable", source_table).load().createOrReplaceTempView(source_table)

      val pre = sqlContext.sql(s"SELECT * FROM ${tableName}").count

      sqlContext.sql(s"INSERT INTO TABLE ${tableName} (a) SELECT testint FROM $source_table")

      checkSqlStatement(
        s"""INSERT INTO "PUBLIC"."$tableName"
           | SELECT ( "SUBQUERY_3"."SUBQUERY_3_COL_0" ) AS "SUBQUERY_4_COL_0" ,
           | ( NULL ) AS "SUBQUERY_4_COL_1" FROM ( SELECT ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) AS "SUBQUERY_3_COL_0"
           | FROM ( SELECT ( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT * FROM "PUBLIC"."$source_table"
           | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_1" ) AS "SUBQUERY_2" ) AS "SUBQUERY_3""""
          .stripMargin
      )

      val post = sqlContext.sql(s"select * from ${tableName}").collect().map(row => row.toSeq).toSeq

      assert(pre == 0)
      val expected = Array(Array(null, null), Array(4141214, null),
        Array(null, null), Array(42, null), Array(42, null))
      post should contain theSameElementsAs expected
    }
  }

  test("Push down insert with simple select expression") {
    withTempRedshiftTable("insertTargetTable") { tableName =>
      withTempRedshiftTable("insertSourceTable") { tableNameSource =>
        redshiftWrapper.executeUpdate(conn,
          s"create table ${tableNameSource} (id int, value int)"
        )
        redshiftWrapper.executeUpdate(conn,
          s"create table ${tableName} (id int, value int)"
        )
        read.option("dbtable", tableNameSource).load.createOrReplaceTempView(tableNameSource)
        read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

        val initialData = Seq((1, 10), (2, 20), (3, 30))
        val schema = List("id", "value")
        val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
        write(df).option("dbtable", tableNameSource).mode("append").save()

        val pre = sqlContext.sql(s"SELECT * FROM $tableName").count()

        sqlContext.sql(s"INSERT INTO ${tableName} " +
          s"SELECT * FROM $tableNameSource WHERE value IN (" +
          s"SELECT value FROM $tableNameSource WHERE value < 40)")

        checkSqlStatement(
          s"""INSERT INTO "PUBLIC"."$tableName"
             | SELECT ( "SUBQUERY_2"."ID" ) AS "SUBQUERY_3_COL_0" , ( "SUBQUERY_2"."VALUE" ) AS "SUBQUERY_3_COL_1"
             | FROM ( SELECT * FROM ( SELECT * FROM "PUBLIC"."$tableNameSource" AS
             | "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_1" WHERE "SUBQUERY_1"."VALUE" IN
             | ( SELECT ( "SUBQUERY_1"."VALUE" ) AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM
             | "PUBLIC"."$tableNameSource" AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
             | WHERE ( "SUBQUERY_0"."VALUE" < 40 ) ) AS "SUBQUERY_1" ) ) AS "SUBQUERY_2""""
            .stripMargin
        )

        val post = sqlContext.sql(s"SELECT * FROM $tableName").collect().map(row => row.toSeq).toSeq

        assert(pre == 0)
        val expected = Array(Array(1, 10), Array(2, 20), Array(3, 30))
        post should contain theSameElementsAs expected
      }
    }
  }

  test("Push down insert with simple select scalar sub-query") {
    withTempRedshiftTable("insertTargetTable") { tableName =>
      withTempRedshiftTable("insertSourceTable") { tableNameSource =>
        redshiftWrapper.executeUpdate(conn,
          s"create table ${tableNameSource} (id int, value int, num int)"
        )
        redshiftWrapper.executeUpdate(conn,
          s"create table ${tableName} (id int, value int, num int)"
        )
        read.option("dbtable", tableNameSource).load.createOrReplaceTempView(tableNameSource)
        read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

        val initialData = Seq((1, 10, 1000), (200, 20, 2000), (3, 30, 3000))
        val schema = List("id", "value", "num")
        val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
        write(df).option("dbtable", tableNameSource).mode("append").save()

        val pre = sqlContext.sql(s"SELECT * FROM $tableName").count()

        sqlContext.sql(
          s"""INSERT INTO ${tableName}
             |SELECT id, value, (SELECT MAX(src_sub.num) FROM
             |$tableNameSource as src_sub) AS num FROM $tableNameSource AS src""".stripMargin)

        checkSqlStatement(
          s"""INSERT INTO "PUBLIC"."$tableName"
             | SELECT ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) AS "SUBQUERY_3_COL_0" ,
             | ( "SUBQUERY_2"."SUBQUERY_2_COL_1" ) AS "SUBQUERY_3_COL_1" ,
             | ( CAST ( "SUBQUERY_2"."SUBQUERY_2_COL_2" AS INTEGER ) )
             | AS "SUBQUERY_3_COL_2" FROM ( SELECT ( "SUBQUERY_1"."ID" ) AS "SUBQUERY_2_COL_0" ,
             | ( "SUBQUERY_1"."VALUE" ) AS "SUBQUERY_2_COL_1" ,
             | ( ( SELECT ( MAX ( "SUBQUERY_0"."NUM" ) ) AS "SUBQUERY_1_COL_0" FROM
             | ( SELECT * FROM "PUBLIC"."$tableNameSource" AS "RS_CONNECTOR_QUERY_ALIAS" )
             | AS "SUBQUERY_0" LIMIT 1 ) ) AS "SUBQUERY_2_COL_2" FROM ( SELECT *
             | FROM "PUBLIC"."$tableNameSource" AS "RS_CONNECTOR_QUERY_ALIAS" )
             |  AS "SUBQUERY_1" ) AS "SUBQUERY_2""""
            .stripMargin
        )

        val post = sqlContext.sql(s"SELECT * FROM $tableName").collect().map(row => row.toSeq).toSeq

        assert(pre == 0)
        val expected = Array(Array(1, 10, 3000), Array(200, 20, 3000), Array(3, 30, 3000))
        post should contain theSameElementsAs expected
      }
    }
  }

  test("Push down insert with select columns in an order") {
    withTempRedshiftTable("insertTargetTable") { tableName =>
      withTempRedshiftTable("insertSourceTable") { tableNameSource =>
        redshiftWrapper.executeUpdate(conn,
          s"create table ${tableNameSource} (id int, value int, quantity int)"
        )
        redshiftWrapper.executeUpdate(conn,
          s"create table ${tableName} (id int, value int, quantity int)"
        )
        read.option("dbtable", tableNameSource).load.createOrReplaceTempView(tableNameSource)
        read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

        val initialData = Seq((1, 10, 100), (2, 20, 200), (3, 30, 300))
        val schema = List("id", "value", "quantity")
        val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
        write(df).option("dbtable", tableNameSource).mode("append").save()

        val pre = sqlContext.sql(s"SELECT * FROM $tableName").count()

        sqlContext.sql(s"INSERT INTO ${tableName} " +
          s"(quantity, id) SELECT quantity, id FROM $tableNameSource")

        checkSqlStatement(
          s"""INSERT INTO "PUBLIC"."$tableName"
             | SELECT ( "SUBQUERY_3"."SUBQUERY_3_COL_1" ) AS "SUBQUERY_4_COL_0" , ( NULL ) AS "SUBQUERY_4_COL_1" ,
             | ( "SUBQUERY_3"."SUBQUERY_3_COL_0" ) AS "SUBQUERY_4_COL_2" FROM ( SELECT ( "SUBQUERY_2"."SUBQUERY_2_COL_0" )
             | AS "SUBQUERY_3_COL_0" , ( "SUBQUERY_2"."SUBQUERY_2_COL_1" ) AS "SUBQUERY_3_COL_1"
             | FROM ( SELECT ( "SUBQUERY_1"."QUANTITY" ) AS "SUBQUERY_2_COL_0" , ( "SUBQUERY_1"."ID" )
             | AS "SUBQUERY_2_COL_1" FROM ( SELECT * FROM "PUBLIC"."$tableNameSource" AS
             | "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_1" ) AS "SUBQUERY_2" ) AS "SUBQUERY_3""""
            .stripMargin
        )

        val post = sqlContext.sql(s"SELECT * FROM $tableName").collect().map(row => row.toSeq).toSeq

        assert(pre == 0)
        val expected = Array(Array(1, null, 100), Array(2, null, 200), Array(3, null, 300))
        post should contain theSameElementsAs expected
      }
    }
  }

  test("Push down insert with another table using table statement") {
    withTempRedshiftTable("insertTargetTable") { tableName =>
      withTempRedshiftTable("insertSourceTable") { tableNameSource =>
        redshiftWrapper.executeUpdate(conn,
          s"create table ${tableNameSource} (id int, value int, quantity int)"
        )
        redshiftWrapper.executeUpdate(conn,
          s"create table ${tableName} (id int, value int, quantity int)"
        )
        read.option("dbtable", tableNameSource).load.createOrReplaceTempView(tableNameSource)
        read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

        val initialData = Seq((1, 10, 100), (2, 20, 200), (3, 30, 300))
        val schema = List("id", "value", "quantity")
        val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
        write(df).option("dbtable", tableNameSource).mode("append").save()

        val pre = sqlContext.sql(s"SELECT * FROM $tableName").count()

        sqlContext.sql(s"INSERT INTO $tableName TABLE $tableNameSource")

        checkSqlStatement(
          s"""INSERT INTO "PUBLIC"."$tableName"
             | SELECT * FROM "PUBLIC"."$tableNameSource" AS "RS_CONNECTOR_QUERY_ALIAS""""
            .stripMargin
        )

        val post = sqlContext.sql(s"SELECT * FROM $tableName").collect().map(row => row.toSeq).toSeq

        assert(pre == 0)
        val expected = Array(Array(1, 10, 100), Array(2, 20, 200), Array(3, 30, 300))
        post should contain theSameElementsAs expected
      }
    }
  }

  test("Push down insert rows from reading another table with where clause") {
    withTempRedshiftTable("insertTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE ${tableName} (a int, b int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)
      read.option("dbtable", source_table).load().createOrReplaceTempView(source_table)

      val pre = sqlContext.sql(s"select * from ${tableName}").count

      sqlContext.sql(
        s"INSERT INTO TABLE ${tableName} (a) SELECT testint FROM $source_table WHERE testint > 0")

      checkSqlStatement(
        s"""INSERT INTO "PUBLIC"."$tableName"
           | SELECT ( "SUBQUERY_4"."SUBQUERY_4_COL_0" ) AS "SUBQUERY_5_COL_0" ,
           | ( NULL ) AS "SUBQUERY_5_COL_1" FROM ( SELECT ( "SUBQUERY_3"."SUBQUERY_3_COL_0" ) AS
           | "SUBQUERY_4_COL_0" FROM ( SELECT ( "SUBQUERY_2"."TESTINT" ) AS "SUBQUERY_3_COL_0" FROM
           | ( SELECT * FROM ( SELECT * FROM "PUBLIC"."$source_table" AS "RS_CONNECTOR_QUERY_ALIAS" )
           | AS "SUBQUERY_1" WHERE ( "SUBQUERY_1"."TESTINT" > 0 ) )
           | AS "SUBQUERY_2" ) AS "SUBQUERY_3" ) AS "SUBQUERY_4""""
          .stripMargin
      )

      val post = sqlContext.sql(s"select * from ${tableName}").collect().map(row => row.toSeq).toSeq

      assert(pre == 0)
      val expected = Array(Array(4141214, null), Array(42, null), Array(42, null))
      post should contain theSameElementsAs expected
    }
  }

  test("Push down insert from reading another table with where clause in a sub-query") {
    withTempRedshiftTable("insertTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE table ${tableName} (a int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)
      read.option("dbtable", source_table).load().createOrReplaceTempView(source_table)

      val pre = sqlContext.sql(s"SELECT * FROM ${tableName}").count

      sqlContext.sql(
        s"""INSERT INTO ${tableName} (a) SELECT testint FROM $source_table
           |WHERE testint IN (SELECT testint FROM $source_table WHERE testint > 0)""".stripMargin)

      checkSqlStatement(
        s"""INSERT INTO "PUBLIC"."$tableName"
           | SELECT ( "SUBQUERY_3"."SUBQUERY_3_COL_0" ) AS "SUBQUERY_4_COL_0" FROM ( SELECT ( "SUBQUERY_2"."TESTINT" )
           | AS "SUBQUERY_3_COL_0" FROM ( SELECT * FROM ( SELECT * FROM "PUBLIC"."$source_table"
           | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_1"
           | WHERE "SUBQUERY_1"."TESTINT" IN ( SELECT ( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT * FROM ( SELECT * FROM "PUBLIC"."$source_table" AS
           | "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE ( "SUBQUERY_0"."TESTINT" > 0 ) )
           | AS "SUBQUERY_1" ) ) AS "SUBQUERY_2" ) AS "SUBQUERY_3""""
          .stripMargin
      )

      val post = sqlContext.sql(s"select * from ${tableName}").collect().map(row => row.toSeq).toSeq

      assert(pre == 0)
      val expected = Array(Array(4141214), Array(42), Array(42))
      post should contain theSameElementsAs expected
    }
  }

  test("Push down insert row from reading aggregation of another table") {
    withTempRedshiftTable("insertTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE ${tableName} (a int, b int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)
      read.option("dbtable", source_table).load().createOrReplaceTempView(source_table)

      val pre = sqlContext.sql(s"SELECT * FROM ${tableName}").count

      sqlContext.sql(s"INSERT INTO ${tableName} (a) SELECT MIN(testint) FROM $source_table")

      checkSqlStatement(
        s"""INSERT INTO "PUBLIC"."$tableName"
           | SELECT ( CAST ( "SUBQUERY_3"."SUBQUERY_3_COL_0" AS INTEGER ) ) AS "SUBQUERY_4_COL_0" ,
           | ( NULL ) AS "SUBQUERY_4_COL_1" FROM ( SELECT ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) AS "SUBQUERY_3_COL_0"
           | FROM ( SELECT ( MIN ( "SUBQUERY_1"."TESTINT" ) ) AS "SUBQUERY_2_COL_0" FROM
           | ( SELECT * FROM "PUBLIC"."$source_table" AS "RS_CONNECTOR_QUERY_ALIAS" )
           | AS "SUBQUERY_1" LIMIT 1 ) AS "SUBQUERY_2" ) AS "SUBQUERY_3""""
          .stripMargin
      )

      val post = sqlContext.sql(s"select * from ${tableName}").collect().map(row => row.toSeq).toSeq

      assert(pre == 0)
      val expected = Array(Array(42, null))
      post should contain theSameElementsAs expected
    }
  }

  test("Push down insert from reading another table with order by") {
    withTempRedshiftTable("insertTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE ${tableName} (a int, b int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)
      read.option("dbtable", source_table).load().createOrReplaceTempView(source_table)

      val pre = sqlContext.sql(s"select * from ${tableName}").count

      sqlContext.sql(s"""INSERT INTO ${tableName} (b)
           |SELECT testint FROM $source_table ORDER BY testint""".stripMargin)

      checkSqlStatement(
        s"""INSERT INTO "PUBLIC"."$tableName"
           | SELECT ( NULL ) AS "SUBQUERY_5_COL_0" , ( "SUBQUERY_4"."SUBQUERY_4_COL_0" ) AS "SUBQUERY_5_COL_1"
           | FROM ( SELECT ( "SUBQUERY_3"."SUBQUERY_2_COL_0" ) AS "SUBQUERY_4_COL_0" FROM
           | ( SELECT * FROM ( SELECT ( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT * FROM "PUBLIC"."$source_table" AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_1" )
           | AS "SUBQUERY_2" ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" )
           | ASC NULLS FIRST ) AS "SUBQUERY_3" ) AS "SUBQUERY_4""""
          .stripMargin
      )

      val post = sqlContext.sql(s"select * from ${tableName}").collect().map(row => row.toSeq).toSeq

      assert(pre == 0)
      val expected = Array(Array(null, null),
        Array(null, 4141214), Array(null, null), Array(null, 42), Array(null, 42))
      post should contain theSameElementsAs expected
    }
  }

  test("Push down insert with replace where is not supported") {
    withTempRedshiftTable("insertTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE ${tableName} (a bigint, b varchar(256))"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      assertThrows[AnalysisException] {
        sqlContext.sql(
          s"INSERT INTO ${tableName} REPLACE WHERE a=0 SELECT testint FROM $source_table")
      }
    }
  }

  test("Push down insert with partition spec is not supported") {
    withTempRedshiftTable("insertTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE ${tableName} (a bigint, b varchar(256))"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      assertThrows[AnalysisException] {
        sqlContext.sql(
          s"INSERT INTO ${tableName} PARTITION (a = 0) VALUES (100L, '1'), (2000,'3')")
      }
    }
  }
}

trait NonPushDownInsertCorrectnessSuite extends IntegrationPushdownSuiteBase {
  test("Not able to push down insert with simple select literals") {
    withTempRedshiftTable("insertTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table ${tableName} (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val pre = sqlContext.sql(s"SELECT * FROM $tableName").count()

      sqlContext.sql(s"INSERT INTO ${tableName} SELECT 1 as id, 10 as value")

      val post = sqlContext.sql(s"SELECT * FROM $tableName").collect().map(row => row.toSeq).toSeq

      assert(pre == 0)
      val expected = Array(Array(1, 10))
      post should contain theSameElementsAs expected
    }
  }

  test("Not able to push down insert with simple select union literals") {
    withTempRedshiftTable("insertTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table ${tableName} (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val pre = sqlContext.sql(s"SELECT * FROM $tableName").count()

      sqlContext.sql(
        s"INSERT INTO ${tableName} " +
          s"SELECT 1 as id, 10 as value UNION " +
          s"SELECT 2 as id, 20 as value UNION " +
          s"SELECT 3 as id, 30 as value")

      val post = sqlContext.sql(s"SELECT * FROM $tableName").collect().map(row => row.toSeq).toSeq

      assert(pre == 0)
      val expected = Array(Array(1, 10), Array(2, 20), Array(3, 30))
      post should contain theSameElementsAs expected
    }
  }

  test("Not able to push down insert with simple select literal with union expression") {
    withTempRedshiftTable("insertTargetTable") { tableName =>
      withTempRedshiftTable("insertSourceTable") { tableNameSource =>
        redshiftWrapper.executeUpdate(conn,
          s"create table ${tableNameSource} (id int, value int)"
        )
        redshiftWrapper.executeUpdate(conn,
          s"create table ${tableName} (id int, value int)"
        )
        read.option("dbtable", tableNameSource).load.createOrReplaceTempView(tableNameSource)
        read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

        val initialData = Seq((1, 10), (2, 20), (3, 30))
        val schema = List("id", "value")
        val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
        write(df).option("dbtable", tableNameSource).mode("append").save()

        val pre = sqlContext.sql(s"SELECT * FROM $tableName").count()

        sqlContext.sql(s"INSERT INTO ${tableName} " +
          s"SELECT * FROM $tableNameSource WHERE value < 30 UNION SELECT 3 as id, 30 as value")

        val post = sqlContext.sql(s"SELECT * FROM $tableName").collect().map(row => row.toSeq).toSeq

        assert(pre == 0)
        val expected = Array(Array(2, 20), Array(1, 10), Array(3, 30))
        post should contain theSameElementsAs expected
      }
    }
  }

  // DML push down is not supported for overwrite mode, will fall back to default insert impl
  test("Not able to push down insert overwrite rows into the table") {
    withTempRedshiftTable("insertTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE ${tableName} (a bigint, b varchar(256))"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val ds = sqlContext.range(0, 10)
      val dsWithTwoColumnsStatic: Dataset[Row] = ds.withColumn("b", lit("y"))

      write(dsWithTwoColumnsStatic).option("dbtable", tableName).mode("append").save()

      val pre = sqlContext.sql(s"SELECT * FROM ${tableName}").count

      sqlContext.sql(s"INSERT OVERWRITE ${tableName} VALUES (100L, '1'), (2000,'3')")

      val post = sqlContext.sql(s"select * from ${tableName}").collect().map(row => row.toSeq).toSeq

      assert(pre == 10)
      val expected = Array(Array(100, "1"), Array(2000, "3"))
      post should contain theSameElementsAs expected
    }
  }
}

class CSVNonPushDownInsertCorrectnessSuite extends NonPushDownInsertCorrectnessSuite {
  override def read: DataFrameReader =
    super.read.option("tempformat", "CSV")
}

class CSVGZipNonPushDownInsertCorrectnessSuite extends NonPushDownInsertCorrectnessSuite {
  override def read: DataFrameReader =
    super.read.option("tempformat", "CSV GZIP")
}

class AvroNonPushDownInsertCorrectnessSuite extends NonPushDownInsertCorrectnessSuite {
  override def read: DataFrameReader =
    super.read.option("tempformat", "AVRO")
}

class ParquetNonPushDownInsertCorrectnessSuite extends NonPushDownInsertCorrectnessSuite {
  override def read: DataFrameReader =
    super.read.option("tempformat", "PARQUET")
}
