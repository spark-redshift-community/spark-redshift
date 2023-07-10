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

import io.github.spark_redshift_community.spark.redshift.TestUtils
import org.apache.spark.sql.Row

abstract class PushdownFilterSuite extends IntegrationPushdownSuiteBase {

  test("String equal filter pushdown") {
    checkAnswer(
      sqlContext.sql("""SELECT * FROM test_table WHERE teststring='asdf'"""),
      Seq(Row(1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42,
        1239012341823719L, -13.toShort, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0)),
      ))

    checkSqlStatement(s"""SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
         |( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'asdf\\' ) )""".stripMargin
    )
  }

  test("String LIKE start with filter pushdown") {
    checkAnswer(
      sqlContext.sql("""SELECT * FROM test_table WHERE teststring LIKE 'asd%'"""),
      Seq(Row(1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42,
        1239012341823719L, -13.toShort, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0)),
      ))

    checkSqlStatement(s"""SELECT * FROM ( SELECT * FROM $test_table
                         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
                         |( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
                         |AND ( "SUBQUERY_0"."TESTSTRING" LIKE \\'asd%\\' ) )""".stripMargin
    )
  }

  test("String LIKE contain filter pushdown") {
    checkAnswer(
      sqlContext.sql("""SELECT * FROM test_table WHERE teststring LIKE '%sd%'"""),
      Seq(Row(1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42,
        1239012341823719L, -13.toShort, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0)),
      ))

    checkSqlStatement(s"""SELECT * FROM ( SELECT * FROM $test_table
                         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
                         |( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
                         |AND ( "SUBQUERY_0"."TESTSTRING" LIKE \\'%sd%\\' ) )""".stripMargin
    )
  }

  test("String LIKE end with filter pushdown") {
    checkAnswer(
      sqlContext.sql("""SELECT * FROM test_table WHERE teststring LIKE '%sdf'"""),
      Seq(Row(1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42,
        1239012341823719L, -13.toShort, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0)),
      ))

    checkSqlStatement(s"""SELECT * FROM ( SELECT * FROM $test_table
                         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
                         |( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
                         |AND ( "SUBQUERY_0"."TESTSTRING" LIKE \\'%sdf\\' ) )""".stripMargin
    )
  }

  test("String LIKE % in the middle filter pushdown") {
    checkAnswer(
      sqlContext.sql("""SELECT * FROM test_table WHERE teststring LIKE 'as%f'"""),
      Seq(Row(1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42,
        1239012341823719L, -13.toShort, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0)),
      ))

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( ( LENGTH ( "SUBQUERY_0"."TESTSTRING" ) >= 3 )
         |AND ( ( "SUBQUERY_0"."TESTSTRING" LIKE \\'as%\\' )
         |AND ( "SUBQUERY_0"."TESTSTRING" LIKE \\'%f\\' ) ) ) )""".stripMargin
    )
  }

  test("Integer equal filter pushdown") {
    checkAnswer(
      sqlContext.sql("""SELECT testint FROM test_table WHERE testint=4141214"""),
      Seq(Row(4141214))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTINT" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTINT" = 4141214 ) ) ) AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("GreaterThan filter pushdown") {
    checkAnswer(
      sqlContext.sql("""SELECT testint FROM test_table WHERE testint>45"""),
      Seq(Row(4141214))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTINT" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTINT" > 45 ) ) ) AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("GreaterThanOrEqual filter pushdown") {
    checkAnswer(
      sqlContext.sql("""SELECT testint FROM test_table WHERE testint>=45"""),
      Seq(Row(4141214))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTINT" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTINT" >= 45 ) ) ) AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("LessThan filter pushdown") {
    checkAnswer(
      sqlContext.sql("""SELECT testshort FROM test_table WHERE testshort<0"""),
      Seq(Row(-13))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTSHORT" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSHORT" < 0 ) ) ) AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("LessThanOrEqual filter pushdown") {
    checkAnswer(
      sqlContext.sql("""SELECT testshort FROM test_table WHERE testshort<=0"""),
      Seq(Row(-13))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTSHORT" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSHORT" <= 0 ) ) ) AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("Between filter pushdown") {
    checkAnswer(
      sqlContext.sql("""SELECT testshort FROM test_table WHERE testshort BETWEEN -15 AND 0"""),
      Seq(Row(-13))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTSHORT" IS NOT NULL )
         |AND ( ( "SUBQUERY_0"."TESTSHORT" >= -15 )
         |AND ( "SUBQUERY_0"."TESTSHORT" <= 0 ) ) ) ) AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("IN filter pushdown") {
    checkAnswer(
      sqlContext.sql("""SELECT testshort FROM test_table WHERE testshort IN (-15, -13, 0)"""),
      Seq(Row(-13))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE "SUBQUERY_0"."TESTSHORT" IN (-15,-13,0) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("AND filter pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT testshort FROM test_table
          |WHERE testshort > -15 AND testshort < 0""".stripMargin),
      Seq(Row(-13))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTSHORT" IS NOT NULL )
         |AND ( ( "SUBQUERY_0"."TESTSHORT" > -15 )
         |AND ( "SUBQUERY_0"."TESTSHORT" < 0 ) ) ) ) AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("OR filter pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT testshort, testint FROM test_table
          |WHERE testshort < 0 OR testint > 45""".stripMargin),
      Seq(Row(null, 4141214), Row(-13, 42))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_0" ,
         |( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_1"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTSHORT" < 0 )
         |OR ( "SUBQUERY_0"."TESTINT" > 45 ) ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("IS NULL filter pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT testshort, testint FROM test_table WHERE testshort IS NULL""".stripMargin),
      Seq(Row(null, null), Row(null, 4141214))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_0" ,
         |( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_1"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( "SUBQUERY_0"."TESTSHORT" IS NULL ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("IS NOT NULL filter pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT testshort FROM test_table WHERE testshort IS NOT NULL""".stripMargin),
      Seq(Row(24), Row(-13), Row(23))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( "SUBQUERY_0"."TESTSHORT" IS NOT NULL ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("NOT filter pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT testlong FROM test_table WHERE NOT testlong = 1239012341823719""".stripMargin),
      Seq()
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTLONG" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTLONG" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTLONG" != 1239012341823719 ) ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("Combined condition NOT filter optimized") {
    checkAnswer(
      sqlContext.sql(
        """SELECT teststring, testfloat FROM test_table
          |WHERE NOT (teststring = 'asdf'
          |OR testfloat > 0)""".stripMargin),
      Seq(Row("f", -1))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTSTRING" ) AS "SUBQUERY_2_COL_0" ,
         |( "SUBQUERY_1"."TESTFLOAT" ) AS "SUBQUERY_2_COL_1"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTFLOAT" IS NOT NULL ) )
         |AND ( ( "SUBQUERY_0"."TESTSTRING" != \\'asdf\\' )
         |AND ( "SUBQUERY_0"."TESTFLOAT" <= 0.0 ) ) ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("Filter NOT LIKE filter pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT testlong FROM test_table WHERE NOT teststring LIKE 'asd%'""".stripMargin),
      Seq(Row(1239012341823719L), Row(1239012341823719L), Row(1239012341823719L))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTLONG" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM (
         |SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND NOT ( ( "SUBQUERY_0"."TESTSTRING" LIKE \\'asd%\\' ) ) ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("Filter EqualNullSafe not supported: operator <=>") {

    conn.createStatement().executeUpdate(
      s""" create table $test_table_safe_null(c1 varchar, c2 varchar)""")
    conn.createStatement().executeUpdate(
      s"insert into $test_table_safe_null values(null, null), ('a', null), ('a', 'a')")

    read
      .option("dbtable", test_table_safe_null)
      .load()
      .createOrReplaceTempView("test_table_safenull")

    checkAnswer(
      sqlContext.sql("""select * from test_table_safenull where c1 <=> c2"""),
      Seq(Row(null, null), Row("a", "a"))
    )
    checkSqlStatement(
      s"""SELECT "c1", "c2" FROM $test_table_safe_null"""
    )

  }
}

class DefaultPushdownFilterSuite extends PushdownFilterSuite {
  override protected val s3format: String = "DEFAULT"
}

class ParquetPushdownFilterSuite extends PushdownFilterSuite {
  override protected val s3format: String = "PARQUET"
}

class DefaultNoPushdownFilterSuite extends PushdownDateTimeSuite {
  override protected val auto_pushdown: String = "false"
  override protected val s3format: String = "DEFAULT"
}