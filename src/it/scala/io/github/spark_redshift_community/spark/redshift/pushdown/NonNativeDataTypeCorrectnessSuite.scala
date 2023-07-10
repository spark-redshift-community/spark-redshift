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

import io.github.spark_redshift_community.spark.redshift.DefaultJDBCWrapper
import org.apache.spark.sql.Row

import java.sql.SQLException

abstract class NonNativeDataTypeCorrectnessSuite extends IntegrationPushdownSuiteBase {

  override def createTestDataInRedshift(tableName: String): Unit = {
    conn.createStatement().executeUpdate(
      s"""
         |create table $tableName (
         |testid int,
         |testsuper super
         |)
       """.stripMargin
    )

    // scalastyle:off
    conn.createStatement().executeUpdate(
      s"""
         |insert into $tableName values
         |(0, null),
         |(1, true),
         |(2, 124),
         |(3, 3.14),
         |(4, 'Hello World'),
         |(5, JSON_PARSE('[10001,10002,3333]')),
         |(6, JSON_PARSE('{"foo": "bar", "foobar": {"abc": "def"}}'))
       """.stripMargin
    )
    // scalastyle:on
  }

  test("Test super data type") {
    // (id, result)
    val testCases = List(
      // Super types should return nulls in JSON format which is the string literal "null".
      // Parquet doesn't handle this properly and returns null values instead.
      // Comment out until [Redshift-7247] is addressed.
//      (0, "null"),
      (1, "true"),
      (2, "124"),
      (3, "3.14"),
      (4, "\"Hello World\""),
      (5, "[10001,10002,3333]"),
      (6, "{\"foo\":\"bar\",\"foobar\":{\"abc\":\"def\"}}")
    )
    testCases.foreach(testCase => {
      val id = testCase._1
      val result = testCase._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT testsuper FROM test_table where testid=$id""".stripMargin),
        Seq(Row(result)))

      checkSqlStatement(
        s"""SELECT ( "SUBQUERY_1"."TESTSUPER" ) AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM
           | ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
           | WHERE ( ( "SUBQUERY_0"."TESTID" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTID" = $id )
           | ) ) AS "SUBQUERY_1"""".stripMargin)
    })
  }

  test("Test unsupported data types") {
    // (Redshift data type, Java sql type value)
    val testCases = List(
      ("geometry", java.sql.Types.LONGVARBINARY),
      ("geography", java.sql.Types.LONGVARBINARY),
      ("hllsketch", java.sql.Types.OTHER),
      ("varbinary", java.sql.Types.LONGVARBINARY)
    )

    testCases.foreach(testCase => {
      val columnType = testCase._1
      val sqlType = testCase._2

      val tableName = s"${columnType}_not_supported$randomSuffix"
      try {
        conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
        conn.createStatement().executeUpdate(
          s"CREATE TABLE $tableName (test$columnType $columnType)")
        conn.createStatement().executeUpdate(s"INSERT INTO $tableName VALUES (null)")
        assert(DefaultJDBCWrapper.tableExists(conn, tableName))
        val e = intercept[SQLException] {
          read
            .option("dbtable", tableName)
            .load()
        }
        assert(e.getMessage.contains(s"Unsupported type $sqlType"))
      } finally {
        conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      }
    })
  }
}

class DefaultPushdownNonNativeDataTypeCorrectnessSuite extends NonNativeDataTypeCorrectnessSuite {
  override protected val s3format: String = "DEFAULT"
  override protected val auto_pushdown: String = "true"
}

class ParquetPushdownNonNativeDataTypeCorrectnessSuite extends NonNativeDataTypeCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "true"
}

class DefaultNoPushdownNonNativeDataTypeCorrectnessSuite extends NonNativeDataTypeCorrectnessSuite {
  override protected val s3format: String = "DEFAULT"
  override protected val auto_pushdown: String = "false"
}

class ParquetNoPushdownNonNativeDataTypeCorrectnessSuite extends NonNativeDataTypeCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}
