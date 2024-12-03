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
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

trait UpdateCorrectnessSuite extends IntegrationPushdownSuiteBase {
  // These tests cannot disable pushdown since update happens in pushdown
  override protected val auto_pushdown: String = "true"
  // These tests cannot use cache since they check the result changing
  override val s3_result_cache: String = "false"

  // DML Update command
  test("Simple Update command with SET and WHERE clause") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table ${tableName} (id int, value int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      val pre = sqlContext.sql(s"select value from $tableName where id = 2").collect()(0)(0)

      sqlContext.sql(s"update $tableName set value = 25 where id = 2")

      val post = sqlContext.sql(s"select value from $tableName where id = 2").collect()(0)(0)

      assert(pre == 20)
      assert(post == 25)
    }
  }

  test("Command with an alias name for the target table in Redshift.") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table ${tableName} (id int, value int)"
      )

      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      val pre = sqlContext.sql(s"select value from $tableName where id = 2").collect()(0)(0)

      sqlContext.sql(s"update $tableName as TARGET set value = 25 where TARGET.id = 2")

      val post = sqlContext.sql(s"select value from $tableName where id = 2").collect()(0)(0)

      assert(pre == 20)
      assert(post == 25)
    }
  }

  test("Command with only a SET value.") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table ${tableName} (id int, value int)"
      )

      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      val pre = sqlContext.sql(s"select value from $tableName")
        .collect().map(_.getInt(0)).toSeq

      sqlContext.sql(s"update $tableName set value = 25")

      val post = sqlContext.sql(s"select value from $tableName ").collect()
        .map(_.getInt(0)).toSeq

      assert(pre.toSet.equals(Seq(10, 20, 30).toSet))
      assert(post.equals(Seq(25, 25, 25)))
    }
  }

  test("Command with multiple conditions.") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table ${tableName} (id int, value int)"
      )

      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      val pre = sqlContext.sql(s"select id from $tableName where value = 25").collect()
        .map(_.getInt(0)).toSeq

      sqlContext.sql(s"update $tableName set value = 25 where id = 2 or id = 1")

      val post = sqlContext.sql(s"select id from $tableName where value = 25").collect()
        .map(_.getInt(0)).toSeq

      assert(pre.isEmpty)
      assert(post.equals(Seq(1, 2)))
    }
  }

  test("Command with a sub-query in WHERE clause using EXISTS check.") {
    withTempRedshiftTable("target") { targetTable =>
      withTempRedshiftTable("condition") { conditionTable =>

        redshiftWrapper.executeUpdate(conn,
          s"create table ${targetTable} (id int, value int)"
        )
        redshiftWrapper.executeUpdate(conn,
          s"create table ${conditionTable} (id int, value int)"
        )
        read.option("dbtable", targetTable).load.createOrReplaceTempView(targetTable)
        read.option("dbtable", conditionTable).load.createOrReplaceTempView(conditionTable)

        val initialData = Seq((1, 10), (2, 20), (3, 30))
        val schema = List("id", "value")
        val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
        write(df).option("dbtable", targetTable).mode("append").save()
        write(df).option("dbtable", conditionTable).mode("append").save()

        val pre = sqlContext.sql(s"select id from $targetTable where value = 25").collect()
          .map(_.getInt(0)).toSeq

        sqlContext.sql(s"update $targetTable as TARGET set value = 25 where " +
          s"exists (select id from $conditionTable where id > 1) ")

        val post = sqlContext.sql(s"select id from $targetTable where value = 25").collect()
          .map(_.getInt(0)).toSeq

        sqlContext.sql(s"delete from $targetTable")
        sqlContext.sql(s"delete from $conditionTable")

        assert(pre.isEmpty)
        assert(post.equals(Seq(1, 2, 3)))
      }
    }
  }

  test("Test non-updated columns retain original values") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table ${tableName} (id int, value int, status varchar(15))"
      )

      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10, "OPEN"), (2, 20, "WIP"), (3, 30, "RESOLVED"))
      val schema = List("id", "value", "status")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      val preValue = sqlContext.sql(s"select value,id from $tableName order by id asc").collect()
        .map(_.getInt(0)).toSeq
      val preStatus = sqlContext.sql(s"select status,id from $tableName order by id asc").collect()
        .map(_.getString(0)).toSeq

      sqlContext.sql(s"update $tableName set status = \'RESOLVED\' where id = 2")

      val postValue = sqlContext.sql(s"select value,id from $tableName order by id asc").collect()
        .map(_.getInt(0)).toSeq
      val postStatus = sqlContext.sql(s"select status,id from $tableName order by id asc").collect()
        .map(_.getString(0)).toSeq

      assert(preValue.equals(Seq(10, 20, 30)))
      assert(postValue.equals(preValue))
      assert(preStatus.equals(Seq("OPEN", "WIP", "RESOLVED")))
      assert(postStatus.equals(Seq("OPEN", "RESOLVED", "RESOLVED")))
    }
  }


  test("simple test table name with schema name") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"""create table ${tableName} (id int, value int)"""
      )
      read.option("dbtable", s""""PUBLIC"."$tableName"""").load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      val pre = sqlContext.sql(s"select value from $tableName where id = 2")
        .collect()(0)(0)

      sqlContext.sql(s"update $tableName set value = 25 where id = 2")

      val post = sqlContext.sql(s"select value from $tableName where id = 2").collect()(0)(0)

      assert(pre == 20)
      assert(post == 25)
    }
  }


  test("Command with multiple assignments.") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table ${tableName} (id int, value int)"
      )

      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      val pre = sqlContext.sql(s"select id from $tableName where value = 25").collect()
        .map(_.getInt(0)).toSeq

      sqlContext.sql(s"update $tableName set value = 25, id = 100 where id = 2")

      val post = sqlContext.sql(s"select id from $tableName where value = 25").collect()
        .map(_.getInt(0)).toSeq

      assert(pre.isEmpty)
      assert(post.equals(Seq(100)))
    }
  }

  test("Command with multiple assignments and conditions.") {
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table ${tableName} (id int, value int)"
      )

      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      val pre = sqlContext.sql(s"select id from $tableName where value = 25").collect()
        .map(_.getInt(0)).toSeq

      sqlContext.sql(s"update $tableName set value = 25, id = 100 where id = 2 or id = 1")

      val post = sqlContext.sql(s"select id from $tableName where value = 25").collect()
        .map(_.getInt(0)).toSeq

      assert(pre.isEmpty)
      assert(post.equals(Seq(100, 100)))
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
          s"create table ${tableName} (id int, value int)"
        )
        redshiftWrapper.executeUpdate(conn,
          s"create table ${assignmentTable} (id int, value int)"
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
          s"${assignmentTable} " +
          s"where ${assignmentTable}.id = $tableName.id)")

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
        s"create table ${tableName} (id int, value int)"
      )

      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)

      val initialData = Seq((1, 10), (2, 20), (3, 30), (4, 40), (5, 50))
      val schema = List("id", "value")
      val df = sqlContext.createDataFrame(initialData).toDF(schema: _*)
      write(df).option("dbtable", tableName).mode("append").save()

      val pre = sqlContext.sql(s"select id from $tableName where value = -1").collect()
        .map(_.getInt(0)).toSeq

      sqlContext.sql(s"update $tableName set value = -1 where value < 30")

      val post = sqlContext.sql(s"select id from $tableName where value = -1").collect()
        .map(_.getInt(0)).toSeq

      assert(pre.isEmpty)
      assert(post.equals(Seq(1, 2)))
    }
  }

  ignore("Test EXISTS subquery in condition ") { // FAIL: EXISTS pushdown.
    withTempRedshiftTable("target") { targetTable =>
      withTempRedshiftTable("condition") { conditionTable =>

        redshiftWrapper.executeUpdate(conn,
          s"create table ${targetTable} (id int, status varchar(30))"
        )
        redshiftWrapper.executeUpdate(conn,
          s"create table ${conditionTable} (id int, status varchar(30))"
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

        val pre = sqlContext.sql(s"select id from $targetTable where status = \'returned\'")
          .collect().map(_.getInt(0)).toSeq

        sqlContext.sql(s"update $targetTable as TARGET set status = \'returned\' where " +
          s"exists (select id from $conditionTable where TARGET.id = id) ")

        val post = sqlContext.sql(s"select id from $targetTable where status = \'returned\'")
          .collect().map(_.getInt(0)).toSeq

        sqlContext.sql(s"delete from $targetTable")
        sqlContext.sql(s"delete from $conditionTable")

        assert(pre.isEmpty)
        assert(post.equals(Seq(2)))
      }
    }
  }

  test("Test NOT IN subquery condition ") {
    withTempRedshiftTable("target") { targetTable =>
      withTempRedshiftTable("condition") { conditionTable =>
        redshiftWrapper.executeUpdate(conn,
          s"create table ${targetTable} (id int, value int)"
        )
        redshiftWrapper.executeUpdate(conn,
          s"create table ${conditionTable} (id int, value int)"
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

        val pre = sqlContext.sql(s"select id from $targetTable where value = 0").collect()
          .map(_.getInt(0)).toSeq

        sqlContext.sql(s"update $targetTable set value = 0 where " +
          s" id NOT IN (select id from $conditionTable where value = -1) ")

        val post = sqlContext.sql(s"select id from $targetTable where value = 0").collect()
          .map(_.getInt(0)).toSeq

        sqlContext.sql(s"delete from $targetTable")
        sqlContext.sql(s"delete from $conditionTable")

        assert(pre.isEmpty)
        assert(post.equals(Seq(3, 4)))
      }
    }
  }

  test("Test DEFAULT null assignment") { // Only work for Null Default
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table ${tableName} (id int, value int)"
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
      assert(post.equals(Seq(10, 20, 30, null, null)))
    }
  }

  ignore("Test DEFAULT non-null assignment") { // FAIL: Spark does not know Redshift Default Value
    withTempRedshiftTable("updateTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"create table ${tableName} (id int, value int default 0)"
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
          s"create table ${targetTable} (id int, value int)"
        )
        redshiftWrapper.executeUpdate(conn,
          s"create table ${conditionTable} (id int, value int)"
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

        val pre = sqlContext.sql(s"select id from $targetTable where value = 0").collect()
          .map(_.getInt(0)).toSeq

        sqlContext.sql(s"update $targetTable set value = 0 where " +
          s" id IN (select id from $conditionTable where value = -1) ")

        val post = sqlContext.sql(s"select id from $targetTable where value = 0").collect()
          .map(_.getInt(0)).toSeq

        sqlContext.sql(s"delete from $targetTable")
        sqlContext.sql(s"delete from $conditionTable")

        assert(pre.isEmpty)
        assert(post.equals(Seq(1, 2, 5)))
      }
    }
  }

  test("Basic_Update") {
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

      val post = sqlContext.sql(s"select * from $tableName")
      val filteredIdDf = post.filter(col("id") === updateIdValue).count()
      val filteredCountDf = post.filter(col("value") === updateIdValue).count()

      assert(filteredIdDf == 3)
      assert(filteredCountDf == 3)
    }
  }

  test("Basic_Update_WHERE") {
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

      val post = sqlContext.sql(s"select * from $tableName")
      val filteredIdDf = post.filter(col("id") === updateIdValue).count()
      val filteredCountDf = post.filter(col("value") === updateIdValue).count()

      assert(filteredIdDf == 1)
      assert(filteredCountDf == 2)
    }
  }

  test("Update_with_nested_query") {
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

      val post = sqlContext.sql(s"select * from $tableName")
      val filteredIdDf = post.filter(col("id") === updateIdValue).count()
      val filteredCountDf = post.filter(col("value") === updateIdValue).count()

      assert(filteredIdDf == 1)
      assert(filteredCountDf == 2)
    }
  }

  test("Update_with_nested_query_IN") {
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

      val post = sqlContext.sql(s"select * from $tableName")
      val filteredIdDf = post.filter(col("id") === updateIdValue).count()
      val filteredCountDf = post.filter(col("value") === updateIdValue).count()

      assert(filteredIdDf == 1)
      assert(filteredCountDf == 2)
    }
  }

  test("Update_with_nested_query_NOT_IN") {
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

      val post = sqlContext.sql(s"select * from $tableName")
      val filteredIdDf = post.filter(col("id") === updateIdValue).count()
      val filteredCountDf = post.filter(col("value") === updateIdValue).count()

      assert(filteredIdDf == 2)
      assert(filteredCountDf == 2)
    }
  }

  ignore("Update_with_tablename_alias_without_AS") { // FAIL: InSubquery Join Condition
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

      val post = sqlContext.sql(s"select * from $tableName")
      val filteredIdDf = post.filter(col("id") === updateIdValue).count()
      val filteredCountDf = post.filter(col("value") === updateIdValue).count()

      assert(filteredIdDf == 1)
      assert(filteredCountDf == 2)
    }
  }

  ignore("Update_with_tablename_alias_AS") { // FAIL: InSubquery Join Condition
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

      val post = sqlContext.sql(s"select * from $tableName")
      val filteredIdDf = post.filter(col("id") === updateIdValue).count()
      val filteredCountDf = post.filter(col("value") === updateIdValue).count()

      assert(filteredIdDf == 1)
      assert(filteredCountDf == 2)
    }
  }

  ignore("Update_with_nested_query_EXISTS") { // FAIL: EXISTS pushdown and Join condition
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

      val post = sqlContext.sql(s"select * from $tableName")
      val filteredIdDf = post.filter(col("id") === updateIdValue).count()
      val filteredCountDf = post.filter(col("value") === updateIdValue).count()

      assert(filteredIdDf == 1)
      assert(filteredCountDf == 2)
    }
  }

  ignore("Update_with_nested_query_NOT_EXISTS") { // FAIL: EXISTS pushdown and Join condition
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

      val post = sqlContext.sql(s"select * from $tableName")
      val filteredIdDf = post.filter(col("id") === updateIdValue).count()
      val filteredCountDf = post.filter(col("value") === updateIdValue).count()

      assert(filteredIdDf == 2)
      assert(filteredCountDf == 2)
    }
  }

  ignore("Update_with_WITH") { // FAIL: EXISTS pushdown and Join condition
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
           |WITH t2 AS (SELECT 1 AS id UNION SELECT 100 AS id)
           |UPDATE $tableName
           |SET id = 20, value = 20
           |WHERE EXISTS(SELECT id FROM t2 WHERE id = t2.id AND value = 10);
           |""".stripMargin)

      val post = sqlContext.sql(s"select * from $tableName")
      val filteredIdDf = post.filter(col("id") === updateIdValue).count()
      val filteredCountDf = post.filter(col("value") === updateIdValue).count()

      assert(filteredIdDf == 1)
      assert(filteredCountDf == 2)
    }
  }
}



class TextUpdateCorrectnessSuite extends UpdateCorrectnessSuite {
  override val s3format = "TEXT"
}

class ParquetUpdateCorrectnessSuite extends UpdateCorrectnessSuite {
  override val s3format = "PARQUET"
}
