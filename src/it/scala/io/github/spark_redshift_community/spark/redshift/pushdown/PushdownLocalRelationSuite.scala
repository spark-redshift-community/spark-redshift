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

class PushdownLocalRelationSuite extends IntegrationPushdownSuiteBase {
  // These tests cannot disable pushdown since insert happens in pushdown
  override protected val auto_pushdown: String = "true"
  // These tests cannot use cache since they check the result changing
  override val s3_result_cache: String = "false"

  test("Push down insert literal values") {
    withTempRedshiftTable("insertTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE ${tableName} (a int, b int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)
      val pre = sqlContext.sql(s"SELECT * FROM ${tableName}").count

      sqlContext.sql(s"INSERT INTO TABLE ${tableName} VALUES (1, 100), (3,2000)")

      checkSqlStatement(
        s"""INSERT INTO "PUBLIC"."$tableName"
           | SELECT ( CAST ( "SQ_1"."COL1" AS INTEGER ) ) AS "SQ_2_COL_0" ,
           | ( CAST ( "SQ_1"."COL2" AS INTEGER ) ) AS "SQ_2_COL_1"
           | FROM ( ( (SELECT 1  AS "col1", 100  AS "col2")
           | UNION ALL (SELECT 3  AS "col1", 2000  AS "col2") ) ) AS "SQ_1"""".stripMargin
      )

      val post = sqlContext.sql(s"SELECT * FROM ${tableName}").collect().map(row => row.toSeq).toSeq

      assert(pre == 0)
      val expected = Array(Array(1, 100), Array(3, 2000))
      post should contain theSameElementsAs expected
    }
  }

  // Can not push down because Local Relation can not generate a SourceQuery
  test("Not able to push down simple select query with literal values") {
    val post = sqlContext.sql(
      s"""SELECT * FROM (VALUES (1, '1'), (2, '2'), (3, '3'))
         | AS tests(id, name)""".stripMargin).collect().map(row => row.toSeq).toSeq

    val expected = Array(Array(1, "1"), Array(2, "2"), Array(3, "3"))
    post should contain theSameElementsAs expected
  }

  test("Push down simple select query join with literal values") {
    withTempRedshiftTable("insertTable") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE ${tableName} (a int, b int)"
      )
      read.option("dbtable", tableName).load.createOrReplaceTempView(tableName)
      val pre = sqlContext.sql(s"SELECT * FROM ${tableName}").count

      sqlContext.sql(s"INSERT INTO TABLE ${tableName} VALUES (1, 100), (3,2000)")

      val post = sqlContext.sql(
        s"""SELECT * FROM
           |(VALUES (1, 1000), (2, 2000), (3, 3000)) AS v(id, name)
           | JOIN ${tableName} AS t
           | ON v.id = t.a""".stripMargin).collect().map(row => row.toSeq).toSeq

      checkSqlStatement(
        s"""SELECT ( "SQ_0"."ID" ) AS "SQ_3_COL_0" ,
           | ( "SQ_0"."NAME" ) AS "SQ_3_COL_1" ,
           | ( "SQ_2"."A" ) AS "SQ_3_COL_2" , ( "SQ_2"."B" ) AS "SQ_3_COL_3"
           | FROM ( ( (SELECT 1  AS "id", 1000  AS "name") UNION ALL (SELECT 2  AS "id", 2000  AS "name")
           | UNION ALL (SELECT 3  AS "id", 3000  AS "name") ) ) AS "SQ_0" INNER JOIN
           | ( SELECT * FROM ( SELECT * FROM "PUBLIC"."${tableName}" AS "RCQ_ALIAS" )
           | AS "SQ_1" WHERE ( "SQ_1"."A" IS NOT NULL ) ) AS "SQ_2" ON
           |  ( "SQ_0"."ID" = "SQ_2"."A" )""".stripMargin
      )

      assert(pre == 0)
      val expected = Array(Array(3, 3000, 3, 2000), Array(1, 1000, 1, 100))
      post should contain theSameElementsAs expected
    }
  }
}
