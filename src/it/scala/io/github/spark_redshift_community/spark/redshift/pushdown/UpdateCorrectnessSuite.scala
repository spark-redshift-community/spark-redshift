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

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._

class UpdateCorrectnessSuite extends IntegrationPushdownSuiteBase {
  // These tests cannot disable pushdown since update happens in pushdown
  override protected val auto_pushdown: String = "true"
  // These tests cannot use cache since they check the result changing
  override val s3_result_cache: String = "false"

  test("Simple Update command with SET and WHERE clause") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      sqlContext.sql(s"update $tableName set value = 25 where id = 2")

      checkSqlStatement(
        s""" UPDATE "PUBLIC"."$tableName"
           | SET "VALUE" = 25
           | WHERE ( "PUBLIC"."$tableName"."ID" = 2 ) """.stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $tableName"),
        Seq( Row(1, 10),
          Row(2, 25),
          Row(3, 30)))
    }
  }

  test("Update command with SET subquery") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      sqlContext.sql(s"update $tableName set value = " +
        s"(select max(value) from $tableName) + 5 where id <= 2 ")

      checkSqlStatement(
        s""" UPDATE "PUBLIC"."$tableName"
           | SET "VALUE" = ( ( SELECT ( MAX ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) )
           | AS "SUBQUERY_2_COL_0" FROM ( SELECT ( "SUBQUERY_0"."VALUE" )
           | AS "SUBQUERY_1_COL_0" FROM ( SELECT * FROM "PUBLIC"."$tableName"
           | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) AS "SUBQUERY_1" LIMIT 1 ) + 5 )
           | WHERE ( "PUBLIC"."$tableName"."ID" <= 2 ) """.stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $tableName"),
        Seq( Row(1, 35),
          Row(2, 35),
          Row(3, 30)))
    }
  }

  test("Update command with SET different column in the same table") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      sqlContext.sql(s"update $tableName set value = id, id = value")
      checkSqlStatement(
        s""" UPDATE "PUBLIC"."$tableName"
           | SET "VALUE" = "PUBLIC"."$tableName"."ID",
           |     "ID" = "PUBLIC"."$tableName"."VALUE" """.stripMargin)
      checkAnswer(
        sqlContext.sql(s"select * from $tableName"),
        Seq( Row(10, 1),
          Row(20, 2),
          Row(30, 3))
      )
    }
  }

  test("Command with an alias name for the target table in Redshift.") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )

      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      sqlContext.sql(s"update $tableName as TARGET set value = 25 where TARGET.id = 2")

      checkSqlStatement(
        s""" UPDATE "PUBLIC"."$tableName"
           | SET "VALUE" = 25
           | WHERE ( "PUBLIC"."$tableName"."ID" = 2 ) """.stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $tableName"),
        Seq( Row(1, 10),
          Row(2, 25),
          Row(3, 30)))
    }
  }

  test("Command with only a SET value.") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )

      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      sqlContext.sql(s"update $tableName set value = 25")

      checkSqlStatement(
        s""" UPDATE "PUBLIC"."$tableName"
           | SET "VALUE" = 25""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $tableName"),
        Seq( Row(1, 25),
          Row(2, 25),
          Row(3, 25)))
    }
  }

  test("Command with multiple conditions.") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )

      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      sqlContext.sql(s"update $tableName set value = 25 where id = 2 or id = 1")
      checkSqlStatement(
        s""" UPDATE "PUBLIC"."$tableName"
           | SET "VALUE" = 25
           | WHERE ( ( "PUBLIC"."$tableName"."ID" = 2 ) OR
           |        ( "PUBLIC"."$tableName"."ID" = 1 ) ) """.stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $tableName"),
        Seq( Row(1, 25),
          Row(2, 25),
          Row(3, 30)))
    }
  }

  test("Command with a sub-query in WHERE clause using EXISTS check.") {
    withTempRedshiftTable("target") { targetTable =>
      withTempRedshiftTable("condition") { conditionTable =>

        redshiftWrapper.executeUpdate(conn,
          s"create table $targetTable (id int, value int)"
        )
        redshiftWrapper.executeUpdate(conn,
          s"create table $conditionTable (id int, value int)"
        )
        read.option("dbtable", targetTable).load.createOrReplaceTempView(targetTable)
        read.option("dbtable", conditionTable).load.createOrReplaceTempView(conditionTable)

        val initialData = Seq((1, 10), (2, 20), (3, 30))
        val schema = List("id", "value")
        val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
        write(df).option("dbtable", targetTable).mode("append").save()
        write(df).option("dbtable", conditionTable).mode("append").save()

        sqlContext.sql(s"update $targetTable as TARGET set value = 25 where " +
          s"exists (select id from $conditionTable where id > 1) ")

        checkSqlStatement(
          s""" UPDATE "PUBLIC"."$targetTable" SET "VALUE" = 25
             | WHERE ( ( SELECT * FROM ( SELECT ( 1 ) AS "SUBQUERY_2_COL_0"
             |  FROM ( SELECT * FROM ( SELECT * FROM "PUBLIC"."$conditionTable"
             |    AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
             |    WHERE ( ( "SUBQUERY_0"."ID" IS NOT NULL )
             |    AND ( "SUBQUERY_0"."ID" > 1 ) ) ) AS "SUBQUERY_1" )
             |    AS "SUBQUERY_2" LIMIT 1 ) IS NOT NULL ) """.stripMargin)

        checkAnswer(
          sqlContext.sql(s"select * from $targetTable"),
          Seq( Row(1, 25),
            Row(2, 25),
            Row(3, 25)))

      }
    }
  }

  test("Test non-updated columns retain original values") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int, status varchar(15))"
      )

      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10, "OPEN"), (2, 20, "WIP"), (3, 30, "RESOLVED"))
      val schema = List("id", "value", "status")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      sqlContext.sql(s"update $tableName set status = \'RESOLVED\' where id = 2")
      checkSqlStatement(
        s""" UPDATE "PUBLIC"."$tableName"
           | SET "STATUS" = 'RESOLVED'
           | WHERE ( "PUBLIC"."$tableName"."ID" = 2 ) """.stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $tableName"),
        Seq( Row(1, 10, "OPEN"),
          Row(2, 20, "RESOLVED"),
          Row(3, 30, "RESOLVED")))
    }
  }

  test("simple test table name with schema name") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (id int, value int)"""
      )
      read.option("dbtable", s""""PUBLIC"."$tableName"""").load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      sqlContext.sql(s"update $tableName set value = 25 where id = 2")

      checkSqlStatement(
        s""" UPDATE "PUBLIC"."$tableName"
           | SET "VALUE" = 25
           | WHERE ( "PUBLIC"."$tableName"."ID" = 2 ) """.stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $tableName"),
        Seq( Row(1, 10),
          Row(2, 25),
          Row(3, 30)))
    }
  }


  test("Command with multiple assignments.") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )

      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      sqlContext.sql(s"update $tableName set value = 25, id = 100 where id = 2")
      checkSqlStatement(
        s""" UPDATE "PUBLIC"."$tableName"
           | SET "VALUE" = 25 , "ID" = 100
           | WHERE ( "PUBLIC"."$tableName"."ID" = 2 ) """.stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $tableName"),
        Seq( Row(1, 10),
          Row(100, 25),
          Row(3, 30)))
    }
  }

  test("Command with multiple assignments and conditions.") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )

      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      sqlContext.sql(s"update $tableName set value = 25, id = 100 where id = 2 or id = 1")
      checkSqlStatement(
        s""" UPDATE "PUBLIC"."$tableName"
           | SET "VALUE" = 25 , "ID" = 100
           | WHERE ( ( "PUBLIC"."$tableName"."ID" = 2 )
           |  OR ( "PUBLIC"."$tableName"."ID" = 1 ) ) """.stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $tableName"),
        Seq( Row(100, 25),
          Row(100, 25),
          Row(3, 30)))
    }
  }

  test("Negative test with complex-type column"){
    // update $tableName set address.city = 'newyork' where id = 1
    withTempRedshiftTable("updateTable") { tableName =>
      val firstString = "NewYork"
      val secondString = "Seattle"
      val expectedRows = Seq(Row(firstString), Row(secondString))

      redshiftWrapper.executeUpdate(conn,
        s"""create table $tableName (id int, address super)"""
      )

      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      redshiftWrapper.executeUpdate(conn,
        s"""insert into $tableName values
           | (1, json_parse('{"city": "$firstString"}')),
           | (2, json_parse('{"city": "$secondString"}'))""".stripMargin
      )

      val superSchema = StructType(StructField("city", StringType)::Nil)
      val dataframeSchema = StructType(StructField("address", superSchema)::Nil)
      val exception = intercept[AnalysisException] {
        sqlContext.sql(s"update $tableName set address.city = 'Boston' where id = 1")
      }

      assert(exception.getMessage.equals("[INVALID_EXTRACT_BASE_FIELD_TYPE] Can't extract a " +
        "value from \"address\". Need a complex type [STRUCT, ARRAY, MAP] but got \"STRING\".; " +
        "line 1 pos 42"))

      checkAnswer(
        read.schema(dataframeSchema).option("dbtable", tableName).load().select("address.city"),
        expectedRows
      )
    }
  }

  ignore("subquery in assignment"){ // FAIL: ScalarSubquery Join Condition
    withTempRedshiftTable("updateTable") { tableName =>
      withTempRedshiftTable("assignment") { assignmentTable =>

        redshiftWrapper.executeUpdate(conn,
          s"create table $tableName (id int, value int)"
        )
        redshiftWrapper.executeUpdate(conn,
          s"create table $assignmentTable (id int, value int)"
        )
        read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)
        read.option("dbtable", assignmentTable).load.createOrReplaceTempView(assignmentTable)

        val initialData = Seq((1, 100), (2, 200), (3, 300))
        val assignData = Seq((1, 99), (2, 199), (3, 299))
        val schema = List("id", "value")
        val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
        write(df).option("dbtable", tableName).mode("append").save()
        val df_assign = sqlContext.createDataFrame(assignData).toDF(schema: _*)
        write(df_assign).option("dbtable", assignmentTable).mode("append").save()

        sqlContext.sql(s"update $tableName set value = ( select max(value) from " +
          s"$assignmentTable " +
          s"where $assignmentTable.id = $tableName.id)")

        val post = sqlContext.sql(s"select id from $tableName").collect()
          .map(_.getInt(0)).toSeq

        redshiftWrapper.executeUpdate(conn, s"drop table if exists $assignmentTable")

        assert(post.equals(Seq(99, 199, 299)))
      }
    }
  }

  test("Test less than operator subquery condition ") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )

      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30), (4, 40), (5, 50))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      sqlContext.sql(s"update $tableName set value = -1 where value < 30")
      checkSqlStatement(
        s""" UPDATE "PUBLIC"."$tableName"
           | SET "VALUE" = -1
           | WHERE ( "PUBLIC"."$tableName"."VALUE" < 30 ) """.stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $tableName"),
        Seq( Row(1, -1), Row(2, -1), Row(3, 30), Row(4, 40), Row(5, 50)))
    }
  }

  test("Test EXISTS subquery in condition ") {
    withTempRedshiftTable("target") { targetTable =>
      withTempRedshiftTable("condition") { conditionTable =>

        redshiftWrapper.executeUpdate(conn,
          s"create table $targetTable (id int, status varchar(30))"
        )
        redshiftWrapper.executeUpdate(conn,
          s"create table $conditionTable (id int, status varchar(30))"
        )
        read.option("dbtable", targetTable).load.createOrReplaceTempView(targetTable)
        read.option("dbtable", conditionTable).load.createOrReplaceTempView(conditionTable)

        val initialData = Seq((1, "shipped"), (2, "delivered"), (3, "processing"))
        val schema = List("id", "status")
        val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
        write(df).option("dbtable", targetTable).mode("append").save()

        val conditionData = Seq((2, "returned"))
        val conditionDf = sqlContext.createDataFrame(conditionData).toDF(schema: _*)
        write(conditionDf).option("dbtable", conditionTable).mode("append").save()

        sqlContext.sql(s"update $targetTable as TARGET set status = \'returned\' where " +
          s"exists (select id from $conditionTable where TARGET.id = id) ")

        checkSqlStatement(
          s""" UPDATE "PUBLIC"."$targetTable"
             | SET "STATUS" = 'returned'
             | WHERE EXISTS ( SELECT ( "SUBQUERY_0"."ID" ) AS "SUBQUERY_1_COL_0"
             |  FROM ( SELECT * FROM "PUBLIC"."$conditionTable" AS "RS_CONNECTOR_QUERY_ALIAS" )
             |  AS "SUBQUERY_0" WHERE ( "PUBLIC"."$targetTable"."ID" = "SUBQUERY_1_COL_0" ) )
             """.stripMargin)

        checkAnswer(
          sqlContext.sql(s"select * from $targetTable"),
          Seq( Row(1, "shipped"), Row(2, "returned"), Row(3, "processing")))
      }
    }
  }

  test("Test NOT IN subquery condition ") {
    withTempRedshiftTable("target") { targetTable =>
      withTempRedshiftTable("condition") { conditionTable =>
        redshiftWrapper.executeUpdate(conn,
          s"create table $targetTable (id int, value int)"
        )
        redshiftWrapper.executeUpdate(conn,
          s"create table $conditionTable (id int, value int)"
        )
        read.option("dbtable", targetTable).load.createOrReplaceTempView(targetTable)
        read.option("dbtable", conditionTable).load.createOrReplaceTempView(conditionTable)

        val schema = List("id", "value")

        val initialData = Seq((1, 10), (2, 20), (3, 30), (4, 40), (5, 50))
        val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
        write(df).option("dbtable", targetTable).mode("append").save()

        val conditionData = Seq((1, -1), (2, -1), (3, 1), (4, 1), (5, -1))
        val conditionDf = sqlContext.createDataFrame(conditionData).toDF(schema: _*)
        write(conditionDf).option("dbtable", conditionTable).mode("append").save()

        sqlContext.sql(s"update $targetTable set value = 0 where " +
          s" id NOT IN (select id from $conditionTable where value = -1) ")

        checkSqlStatement(
          s""" UPDATE "PUBLIC"."$targetTable"
             | SET "VALUE" = 0
             | WHERE NOT ( ( "PUBLIC"."$targetTable"."ID" ) IN ( SELECT ( "SUBQUERY_1"."ID" )
             |  AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM
             |    ( SELECT * FROM "PUBLIC"."$conditionTable" AS "RS_CONNECTOR_QUERY_ALIAS" )
             |     AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."VALUE" IS NOT NULL ) AND
             |     ( "SUBQUERY_0"."VALUE" = -1 ) ) ) AS "SUBQUERY_1" ) ) """.stripMargin)

        checkAnswer(
          sqlContext.sql(s"select * from $targetTable"),
          Seq( Row(1, 10), Row(2, 20), Row(3, 0), Row(4, 0), Row(5, 50)))

      }
    }
  }

  test("Test DEFAULT null assignment") { // Only work for Null Default
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30), (4, 40), (5, 50))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      sqlContext.sql(s"update $tableName set value = DEFAULT where value > 30")
      checkSqlStatement(
        s""" UPDATE "PUBLIC"."$tableName"
           | SET "VALUE" = NULL
           | WHERE( "PUBLIC"."$tableName"."VALUE" > 30 )""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $tableName"),
        Seq( Row(1, 10), Row(2, 20), Row(3, 30), Row(4, null), Row(5, null)))

    }
  }

  ignore("Test DEFAULT non-null assignment") { // FAIL: Spark does not know Redshift Default Value
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int default 0)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30), (4, 40), (5, 50))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      val pre = sqlContext.sql(s"select value from $tableName").collect()
        .map(_.getInt(0)).toSeq

      sqlContext.sql(s"update $tableName set value = DEFAULT where value > 30")

      val post = sqlContext.sql(s"select value from $tableName").collect()
        .map(row => if (!row.isNullAt(0)) row.getInt(0) else null)
        .toSeq

      assert(pre.equals(Seq(10, 20, 30, 40, 50)))
      assert(post.equals(Seq(10, 20, 30, 0, 0)))
    }
  }

  test("IN Subquery condition") {
    withTempRedshiftTable("target") { targetTable =>
      withTempRedshiftTable("condition") { conditionTable =>
        redshiftWrapper.executeUpdate(conn,
          s"create table $targetTable (id int, value int)"
        )
        redshiftWrapper.executeUpdate(conn,
          s"create table $conditionTable (id int, value int)"
        )
        read.option("dbtable", targetTable).load.createOrReplaceTempView(targetTable)
        read.option("dbtable", conditionTable).load.createOrReplaceTempView(conditionTable)

        val schema = List("id", "value")

        val initialData = Seq((1, 10), (2, 20), (3, 30), (4, 40), (5, 50))
        val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
        write(df).option("dbtable", targetTable).mode("append").save()

        val conditionData = Seq((1, -1), (2, -1), (3, 1), (4, 1), (5, -1))
        val conditionDf = sqlContext.createDataFrame(conditionData).toDF(schema: _*)
        write(conditionDf).option("dbtable", conditionTable).mode("append").save()

        sqlContext.sql(s"update $targetTable set value = 0 where " +
          s" id IN (select id from $conditionTable where value = -1) ")

        val testTableName = s""""PUBLIC"."${targetTable}""""
        val testTableName2 = s""""PUBLIC"."${conditionTable}""""
        checkSqlStatement(
          s"""UPDATE
             |  $testTableName
             |SET
             |  "VALUE" = 0
             |WHERE
             |  ($testTableName."ID") IN (
             |    SELECT
             |      ("SUBQUERY_1"."ID") AS "SUBQUERY_2_COL_0"
             |    FROM
             |      (
             |        SELECT
             |          *
             |        FROM
             |          (
             |            SELECT
             |              *
             |            FROM
             |              $testTableName2 AS "RS_CONNECTOR_QUERY_ALIAS"
             |          ) AS "SUBQUERY_0"
             |        WHERE
             |          (
             |            ("SUBQUERY_0"."VALUE" IS NOT NULL)
             |            AND ("SUBQUERY_0"."VALUE" = -1)
             |          )
             |      ) AS "SUBQUERY_1"
             |  )""".stripMargin)


        checkAnswer(
          sqlContext.sql(s"select * from $targetTable"),
          Seq( Row(1, 0), Row(2, 0), Row(3, 30), Row(4, 40), Row(5, 0)))

      }
    }
  }

  test("Basic Update") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()
      val updateIdValue = 20
      val updateCountValue = 20
      sqlContext.sql(s"UPDATE $tableName SET id = $updateIdValue, value = $updateCountValue")

      val testTableName = s""""PUBLIC"."${tableName}""""
      checkSqlStatement(
        s"""UPDATE $testTableName SET "ID" = 20, "VALUE" = 20""".stripMargin)

      val post = sqlContext.sql(s"select * from $tableName")
      checkAnswer( post, Seq( Row(20, 20), Row(20, 20), Row(20, 20) ) )
      val filteredIdDf = post.filter(col("id") === updateIdValue).count()
      val filteredCountDf = post.filter(col("value") === updateIdValue).count()

      assert(filteredIdDf == 3)
      assert(filteredCountDf == 3)
    }
  }

  test("Basic Update with WHERE") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()
      val updateIdValue = 20
      val updateCountValue = 20
      sqlContext.sql(s"UPDATE $tableName SET id = $updateIdValue, value = $updateCountValue " +
        s"WHERE id=1 AND value=10")

      val testTableName = s""""PUBLIC"."${tableName}""""
      checkSqlStatement(s"""UPDATE
                           |    $testTableName
                           |SET
                           |    "ID" = $updateIdValue,
                           |    "VALUE" = $updateCountValue
                           |WHERE
                           |    (
                           |        (
                           |            $testTableName."ID" = 1
                           |        )
                           |        AND (
                           |            $testTableName."VALUE" = 10
                           |        )
                           |    )""".stripMargin)

      val post = sqlContext.sql(s"select * from $tableName")
      val filteredIdDf = post.filter(col("id") === updateIdValue).count()
      val filteredCountDf = post.filter(col("value") === updateIdValue).count()

      assert(filteredIdDf == 1)
      assert(filteredCountDf == 2)
    }
  }

  test("Update with nested query") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()
      val updateIdValue = 20
      val updateCountValue = 20
      sqlContext.sql(
        s"""
           |UPDATE $tableName
           |SET id = $updateIdValue, value = $updateCountValue
           |WHERE value < (SELECT value FROM $tableName WHERE value = $updateCountValue)
           |""".stripMargin)

      val testTableName = s""""PUBLIC"."${tableName}""""
      checkSqlStatement(s"""UPDATE
                           | $testTableName
                           |SET
                           | "ID" = $updateIdValue,
                           | "VALUE" = $updateCountValue
                           |WHERE
                           | (
                           |  $testTableName."VALUE" < (
                           |   SELECT
                           |    ("SUBQUERY_1"."VALUE") AS "SUBQUERY_2_COL_0"
                           |   FROM
                           |    (
                           |     SELECT
                           |      *
                           |     FROM
                           |      (
                           |       SELECT
                           |        *
                           |       FROM
                           |        $testTableName AS "RS_CONNECTOR_QUERY_ALIAS"
                           |      ) AS "SUBQUERY_0"
                           |     WHERE
                           |      (
                           |       ("SUBQUERY_0"."VALUE" IS NOT NULL)
                           |       AND ("SUBQUERY_0"."VALUE" = 20)
                           |      )
                           |    ) AS "SUBQUERY_1"
                           |  )
                           | )""".stripMargin)

      val post = sqlContext.sql(s"select * from $tableName")
      val filteredIdDf = post.filter(col("id") === updateIdValue).count()
      val filteredCountDf = post.filter(col("value") === updateIdValue).count()

      assert(filteredIdDf == 1)
      assert(filteredCountDf == 2)
    }
  }

  test("Update with nested query IN") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()
      val updateIdValue = 20
      val updateCountValue = 20
      sqlContext.sql(
        s"""
           |UPDATE $tableName
           |SET id = $updateIdValue, value = $updateCountValue
           |WHERE id IN (SELECT id FROM $tableName WHERE id = 1 AND value = 10)
           |""".stripMargin)

      val testTableName = s""""PUBLIC"."${tableName}""""
      checkSqlStatement(s"""UPDATE
                           |  $testTableName
                           |SET
                           |  "ID" = 20,
                           |  "VALUE" = 20
                           |WHERE
                           |  ($testTableName."ID") IN (
                           |    SELECT
                           |      ("SUBQUERY_1"."ID") AS "SUBQUERY_2_COL_0"
                           |    FROM
                           |      (
                           |        SELECT
                           |          *
                           |        FROM
                           |          (
                           |            SELECT
                           |              *
                           |            FROM
                           |              $testTableName AS "RS_CONNECTOR_QUERY_ALIAS"
                           |          ) AS "SUBQUERY_0"
                           |        WHERE
                           |          (
                           |            (
                           |              ("SUBQUERY_0"."ID" IS NOT NULL)
                           |              AND ("SUBQUERY_0"."VALUE" IS NOT NULL)
                           |            )
                           |            AND (
                           |              ("SUBQUERY_0"."ID" = 1)
                           |              AND ("SUBQUERY_0"."VALUE" = 10)
                           |            )
                           |          )
                           |      ) AS "SUBQUERY_1"
                           |  )""".stripMargin)

      val post = sqlContext.sql(s"select * from $tableName")
      val filteredIdDf = post.filter(col("id") === updateIdValue).count()
      val filteredCountDf = post.filter(col("value") === updateIdValue).count()

      assert(filteredIdDf == 1)
      assert(filteredCountDf == 2)
    }
  }

  test("Update with nested query NOT IN") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()
      val updateIdValue = 20
      val updateCountValue = 20
      sqlContext.sql(
        s"""
           |UPDATE $tableName
           |SET id = $updateIdValue, value = $updateCountValue
           |WHERE id NOT IN (SELECT id FROM $tableName WHERE id = 1 AND value = 10)
           |""".stripMargin)

      val testTableName = s""""PUBLIC"."${tableName}""""
      checkSqlStatement(s"""UPDATE
                           |  $testTableName
                           |SET
                           |  "ID" = 20,
                           |  "VALUE" = 20
                           |WHERE
                           |  NOT (
                           |    ($testTableName."ID") IN (
                           |      SELECT
                           |        ("SUBQUERY_1"."ID") AS "SUBQUERY_2_COL_0"
                           |      FROM
                           |        (
                           |          SELECT
                           |            *
                           |          FROM
                           |            (
                           |              SELECT
                           |                *
                           |              FROM
                           |                $testTableName AS "RS_CONNECTOR_QUERY_ALIAS"
                           |            ) AS "SUBQUERY_0"
                           |          WHERE
                           |            (
                           |              (
                           |                ("SUBQUERY_0"."ID" IS NOT NULL)
                           |                AND ("SUBQUERY_0"."VALUE" IS NOT NULL)
                           |              )
                           |              AND (
                           |                ("SUBQUERY_0"."ID" = 1)
                           |                AND ("SUBQUERY_0"."VALUE" = 10)
                           |              )
                           |            )
                           |        ) AS "SUBQUERY_1"
                           |    )
                           |  )""".stripMargin)

      val post = sqlContext.sql(s"select * from $tableName")
      val filteredIdDf = post.filter(col("id") === updateIdValue).count()
      val filteredCountDf = post.filter(col("value") === updateIdValue).count()

      assert(filteredIdDf == 2)
      assert(filteredCountDf == 2)
    }
  }

  test("Update with tablename alias without AS") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()
      val updateIdValue = 20
      val updateCountValue = 20
      sqlContext.sql(
        s"""
           |UPDATE $tableName t1
           |SET id = $updateIdValue, value = $updateCountValue
           |WHERE id IN (SELECT id FROM $tableName WHERE t1.id = id AND value = 10)
           |""".stripMargin)

      val testTableName = s""""PUBLIC"."${tableName}""""
      checkSqlStatement(s"""UPDATE
                           |  $testTableName
                           |SET
                           |  "ID" = 20,
                           |  "VALUE" = 20
                           |WHERE
                           |  ($testTableName."ID") IN (
                           |    SELECT
                           |      ("SUBQUERY_1"."ID") AS "SUBQUERY_2_COL_0"
                           |    FROM
                           |      (
                           |        SELECT
                           |          *
                           |        FROM
                           |          (
                           |            SELECT
                           |              *
                           |            FROM
                           |              $testTableName AS "RS_CONNECTOR_QUERY_ALIAS"
                           |          ) AS "SUBQUERY_0"
                           |        WHERE
                           |          (
                           |            ("SUBQUERY_0"."VALUE" IS NOT NULL)
                           |            AND ("SUBQUERY_0"."VALUE" = 10)
                           |          )
                           |      ) AS "SUBQUERY_1"
                           |    WHERE
                           |      (
                           |        $testTableName."ID" = "SUBQUERY_2_COL_0"
                           |      )
                           |  )""".stripMargin)

      val post = sqlContext.sql(s"select * from $tableName")
      val filteredIdDf = post.filter(col("id") === updateIdValue).count()
      val filteredCountDf = post.filter(col("value") === updateIdValue).count()

      assert(filteredIdDf == 1)
      assert(filteredCountDf == 2)
    }
  }

  test("Update with tablename alias AS") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()
      val updateIdValue = 20
      val updateCountValue = 20
      sqlContext.sql(
        s"""
           |UPDATE $tableName AS t1
           |SET id = $updateIdValue, value = $updateCountValue
           |WHERE id IN (SELECT id FROM $tableName WHERE t1.id = id AND value = 10)
           |""".stripMargin)

      val testTableName = s""""PUBLIC"."${tableName}""""
      checkSqlStatement(s"""UPDATE
                           |  $testTableName
                           |SET
                           |  "ID" = 20,
                           |  "VALUE" = 20
                           |WHERE
                           |  ($testTableName."ID") IN (
                           |    SELECT
                           |      ("SUBQUERY_1"."ID") AS "SUBQUERY_2_COL_0"
                           |    FROM
                           |      (
                           |        SELECT
                           |          *
                           |        FROM
                           |          (
                           |            SELECT
                           |              *
                           |            FROM
                           |              $testTableName AS "RS_CONNECTOR_QUERY_ALIAS"
                           |          ) AS "SUBQUERY_0"
                           |        WHERE
                           |          (
                           |            ("SUBQUERY_0"."VALUE" IS NOT NULL)
                           |            AND ("SUBQUERY_0"."VALUE" = 10)
                           |          )
                           |      ) AS "SUBQUERY_1"
                           |    WHERE
                           |      (
                           |        $testTableName."ID" = "SUBQUERY_2_COL_0"
                           |      )
                           |  )""".stripMargin)

      val post = sqlContext.sql(s"select * from $tableName")
      val filteredIdDf = post.filter(col("id") === updateIdValue).count()
      val filteredCountDf = post.filter(col("value") === updateIdValue).count()

      assert(filteredIdDf == 1)
      assert(filteredCountDf == 2)
    }
  }

  test("Update with nested query EXISTS") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()
      val updateIdValue = 20
      val updateCountValue = 20
      sqlContext.sql(
        s"""
           |UPDATE $tableName AS t1
           |SET id = $updateIdValue, value = $updateCountValue
           |WHERE EXISTS (SELECT id FROM $tableName WHERE id = t1.id AND value = 10)
           |""".stripMargin)

      val testTableName = s""""PUBLIC"."${tableName}""""
      checkSqlStatement(s"""UPDATE
                           |  $testTableName
                           |SET
                           |  "ID" = 20,
                           |  "VALUE" = 20
                           |WHERE
                           |  EXISTS (
                           |    SELECT
                           |      ("SUBQUERY_1"."ID") AS "SUBQUERY_2_COL_0"
                           |    FROM
                           |      (
                           |        SELECT
                           |          *
                           |        FROM
                           |          (
                           |            SELECT
                           |              *
                           |            FROM
                           |              $testTableName AS "RS_CONNECTOR_QUERY_ALIAS"
                           |          ) AS "SUBQUERY_0"
                           |        WHERE
                           |          (
                           |            ("SUBQUERY_0"."VALUE" IS NOT NULL)
                           |            AND ("SUBQUERY_0"."VALUE" = 10)
                           |          )
                           |      ) AS "SUBQUERY_1"
                           |    WHERE
                           |      (
                           |        "SUBQUERY_2_COL_0" = $testTableName."ID"
                           |      )
                           |  )""".stripMargin)

      val post = sqlContext.sql(s"select * from $tableName")
      val filteredIdDf = post.filter(col("id") === updateIdValue).count()
      val filteredCountDf = post.filter(col("value") === updateIdValue).count()

      assert(filteredIdDf == 1)
      assert(filteredCountDf == 2)
    }
  }

  test("Update with nested query NOT EXISTS") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()
      val updateIdValue = 20
      val updateCountValue = 20
      sqlContext.sql(
        s"""
           |UPDATE $tableName AS t1
           |SET id = $updateIdValue, value = $updateCountValue
           |WHERE NOT EXISTS (SELECT id FROM $tableName WHERE id = t1.id AND value = 10)
           |""".stripMargin)

      val testTableName = s""""PUBLIC"."${tableName}""""
      checkSqlStatement(s"""UPDATE
                           |  $testTableName
                           |SET
                           |  "ID" = 20,
                           |  "VALUE" = 20
                           |WHERE
                           |  NOT (
                           |    EXISTS (
                           |      SELECT
                           |        ("SUBQUERY_1"."ID") AS "SUBQUERY_2_COL_0"
                           |      FROM
                           |        (
                           |          SELECT
                           |            *
                           |          FROM
                           |            (
                           |              SELECT
                           |                *
                           |              FROM
                           |                $testTableName AS "RS_CONNECTOR_QUERY_ALIAS"
                           |            ) AS "SUBQUERY_0"
                           |          WHERE
                           |            (
                           |              ("SUBQUERY_0"."VALUE" IS NOT NULL)
                           |              AND ("SUBQUERY_0"."VALUE" = 10)
                           |            )
                           |        ) AS "SUBQUERY_1"
                           |      WHERE
                           |        (
                           |          "SUBQUERY_2_COL_0" = $testTableName."ID"
                           |        )
                           |    )
                           |  )""".stripMargin)

      val post = sqlContext.sql(s"select * from $tableName")
      val filteredIdDf = post.filter(col("id") === updateIdValue).count()
      val filteredCountDf = post.filter(col("value") === updateIdValue).count()

      assert(filteredIdDf == 2)
      assert(filteredCountDf == 2)
    }
  }

  ignore("Update_with_WITH") { // FAIL: WITH
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table $tableName (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()
      val updateIdValue = 20
      sqlContext.sql(
        s"""
           |WITH t2 AS (SELECT 1 AS id UNION SELECT 100 AS id)
           |UPDATE $tableName
           |SET id = 20, value = 20
           |WHERE EXISTS(SELECT id FROM t2 WHERE id = t2.id AND value = 10);
           |""".stripMargin)

      val testTableName = s""""PUBLIC"."${tableName}""""
      checkSqlStatement(s"""TBD""".stripMargin)

      val post = sqlContext.sql(s"select * from $tableName")
      val filteredIdDf = post.filter(col("id") === updateIdValue).count()
      val filteredCountDf = post.filter(col("value") === updateIdValue).count()

      assert(filteredIdDf == 1)
      assert(filteredCountDf == 2)
    }
  }
}
