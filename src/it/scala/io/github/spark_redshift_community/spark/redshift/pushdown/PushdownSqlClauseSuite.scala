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

import org.apache.spark.sql.{Row, SparkSession}


abstract class PushdownSqlClauseSuite extends IntegrationPushdownSuiteBase {

  test("Limit clause pushdown") {
    checkAnswer(
      sqlContext.sql("""SELECT testbyte FROM test_table LIMIT 1"""),
      Seq(Row(null))
    )

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT ( "SUBQUERY_0"."TESTBYTE" ) AS "SUBQUERY_1_COL_0"
         | FROM ( SELECT * FROM $test_table
         | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
         | AS "SUBQUERY_1" LIMIT 1""".stripMargin
    )
  }

  test("Union clause pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT testString, testshort FROM test_table  WHERE testString IS NOT NULL
          |UNION
          |SELECT testString, testshort FROM test_table  WHERE testString='f'
          |""".stripMargin),
      Seq(
        Row("Unicode's樂趣", 23),
        Row("___|_123", 24),
        Row("asdf", -13),
        Row("f", null))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_0"."SUBQUERY_2_COL_0" ) AS "SUBQUERY_1_COL_0" ,
         |( "SUBQUERY_0"."SUBQUERY_2_COL_1" ) AS "SUBQUERY_1_COL_1"
         |FROM ( ( SELECT ( "SUBQUERY_1"."TESTSTRING" ) AS "SUBQUERY_2_COL_0" ,
         |( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_1" FROM
         |( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL ) ) AS "SUBQUERY_1" )
         |UNION ALL
         |( SELECT ( "SUBQUERY_1"."TESTSTRING" ) AS "SUBQUERY_2_COL_0" ,
         |( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_1" FROM
         |( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'f\\' ) ) ) AS "SUBQUERY_1" ) ) AS "SUBQUERY_0"
         |GROUP BY "SUBQUERY_0"."SUBQUERY_2_COL_0" , "SUBQUERY_0"."SUBQUERY_2_COL_1"
         |""".stripMargin
    )
  }

  test("Union All clause pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT testString, testshort FROM test_table  WHERE testString IS NOT NULL
          |UNION ALL
          |SELECT testString, testshort FROM test_table  WHERE testString='f'
          |""".stripMargin),
      Seq(
        Row("Unicode's樂趣", 23),
        Row("___|_123", 24),
        Row("asdf", -13),
        Row("f", null),
        Row("f", null))
    )

    checkSqlStatement(
      s"""( SELECT ( "SUBQUERY_1"."TESTSTRING" ) AS "SUBQUERY_2_COL_0" ,
         |( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_1" FROM
         |( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL ) ) AS "SUBQUERY_1" )
         |UNION ALL
         |( SELECT ( "SUBQUERY_1"."TESTSTRING" ) AS "SUBQUERY_2_COL_0" ,
         |( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_1" FROM
         |( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'f\\' ) ) ) AS "SUBQUERY_1" )
         |""".stripMargin
    )
  }

  test("Order by pushdown") {
    checkAnswer(
      sqlContext.sql("""SELECT testshort FROM test_table where testshort > 0 ORDER BY testshort"""),
      Seq(Row(23), Row(24))
    )

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT ( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSHORT" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSHORT" > 0 ) ) ) AS "SUBQUERY_1" ) AS "SUBQUERY_2"
         |ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC
         |""".stripMargin
    )
  }

  test("Order by DESC pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT testshort FROM test_table where testshort > 0
          |ORDER BY testshort DESC""".stripMargin),
      Seq(Row(24), Row(23))
    )

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT ( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSHORT" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSHORT" > 0 ) ) ) AS "SUBQUERY_1" ) AS "SUBQUERY_2"
         |ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) DESC
         |""".stripMargin
    )
  }

  test("Inner Join pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT table1.testint, table2.testshort
          |FROM test_table table1
          |INNER JOIN  test_table table2
          |ON table1.testint = table2.testint
          |WHERE table1.teststring='asdf'
          |""".stripMargin),
      Seq(Row(42, -13), Row(42, 23))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_6"."SUBQUERY_6_COL_0" ) AS "SUBQUERY_7_COL_0" ,
         |( "SUBQUERY_6"."SUBQUERY_6_COL_2" ) AS "SUBQUERY_7_COL_1"
         |FROM (
         |SELECT ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) AS "SUBQUERY_6_COL_0" ,
         |( "SUBQUERY_5"."SUBQUERY_5_COL_0" ) AS "SUBQUERY_6_COL_1" ,
         |( "SUBQUERY_5"."SUBQUERY_5_COL_1" ) AS "SUBQUERY_6_COL_2"
         |FROM ( SELECT ( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_0" FROM
         |( SELECT * FROM
         |( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'asdf\\' ) )
         |AND ( "SUBQUERY_0"."TESTINT" IS NOT NULL ) ) ) AS "SUBQUERY_1" )
         |AS "SUBQUERY_2"
         |INNER JOIN
         |( SELECT ( "SUBQUERY_4"."TESTINT" ) AS "SUBQUERY_5_COL_0" ,
         |( "SUBQUERY_4"."TESTSHORT" ) AS "SUBQUERY_5_COL_1"
         |FROM ( SELECT * FROM
         |( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_3"
         |WHERE ( "SUBQUERY_3"."TESTINT" IS NOT NULL ) ) AS "SUBQUERY_4" )
         |AS "SUBQUERY_5"
         | ON ( "SUBQUERY_2"."SUBQUERY_2_COL_0" = "SUBQUERY_5"."SUBQUERY_5_COL_0" ) ) AS "SUBQUERY_6"
         |""".stripMargin
    )
  }

  test("Left outer Join pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT table1.testint, new_table.testshort
          |FROM test_table table1
          |LEFT JOIN
          |(SELECT table2.testint, table2.testshort FROM test_table table2 WHERE table2.testint != 42) AS new_table
          |ON table1.testint = new_table.testint
          |WHERE table1.teststring='asdf'
          |""".stripMargin),
      Seq(Row(42, null))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_6"."SUBQUERY_6_COL_0" ) AS "SUBQUERY_7_COL_0" ,
         |( "SUBQUERY_6"."SUBQUERY_6_COL_2" ) AS "SUBQUERY_7_COL_1"
         |FROM (
         |SELECT ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) AS "SUBQUERY_6_COL_0" ,
         |( "SUBQUERY_5"."SUBQUERY_5_COL_0" ) AS "SUBQUERY_6_COL_1" ,
         |( "SUBQUERY_5"."SUBQUERY_5_COL_1" ) AS "SUBQUERY_6_COL_2"
         |FROM ( SELECT ( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'asdf\\' ) ) ) AS "SUBQUERY_1" ) AS "SUBQUERY_2"
         |LEFT OUTER JOIN
         |( SELECT ( "SUBQUERY_4"."TESTINT" ) AS "SUBQUERY_5_COL_0" ,
         |( "SUBQUERY_4"."TESTSHORT" ) AS "SUBQUERY_5_COL_1"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_3"
         |WHERE ( ( "SUBQUERY_3"."TESTINT" IS NOT NULL )
         |AND ( "SUBQUERY_3"."TESTINT" != 42 ) ) ) AS "SUBQUERY_4" ) AS "SUBQUERY_5"
         |ON ( "SUBQUERY_2"."SUBQUERY_2_COL_0" = "SUBQUERY_5"."SUBQUERY_5_COL_0" ) ) AS "SUBQUERY_6"
         |""".stripMargin
    )
  }

  test("Right Join pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT table2.testint, new_table.testshort
          |FROM (SELECT table1.testint, table1.testshort FROM test_table table1 WHERE table1.testint != 42) AS new_table
          |RIGHT JOIN  test_table table2
          |ON new_table.testint = table2.testint
          |WHERE table2.teststring='asdf'
          |""".stripMargin),
      Seq(Row(42, null))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_6"."SUBQUERY_6_COL_2" ) AS "SUBQUERY_7_COL_0" ,
         |( "SUBQUERY_6"."SUBQUERY_6_COL_1" ) AS "SUBQUERY_7_COL_1"
         |FROM (
         |SELECT ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) AS "SUBQUERY_6_COL_0" ,
         |( "SUBQUERY_2"."SUBQUERY_2_COL_1" ) AS "SUBQUERY_6_COL_1" ,
         |( "SUBQUERY_5"."SUBQUERY_5_COL_0" ) AS "SUBQUERY_6_COL_2"
         |FROM ( SELECT ( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_0" ,
         |( "SUBQUERY_1"."TESTSHORT" ) AS "SUBQUERY_2_COL_1"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTINT" IS NOT NULL ) AND ( "SUBQUERY_0"."TESTINT" != 42 ) ) )
         |AS "SUBQUERY_1" ) AS "SUBQUERY_2"
         |RIGHT OUTER JOIN
         |( SELECT ( "SUBQUERY_4"."TESTINT" ) AS "SUBQUERY_5_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_3"
         |WHERE ( ( "SUBQUERY_3"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_3"."TESTSTRING" = \\'asdf\\' ) ) ) AS "SUBQUERY_4" )
         |AS "SUBQUERY_5" ON ( "SUBQUERY_2"."SUBQUERY_2_COL_0" = "SUBQUERY_5"."SUBQUERY_5_COL_0" ) )
         |AS "SUBQUERY_6"
         |""".stripMargin
    )
  }

  test("Full Join pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT new_table1.teststring as S1, new_table2.teststring as S2
          |FROM (SELECT table1.teststring FROM test_table table1 WHERE table1.teststring = 'f') AS new_table1
          |FULL JOIN
          |(SELECT table2.teststring FROM test_table table2 WHERE table2.teststring = 'asdf') AS new_table2
          |ON new_table1.teststring = new_table2.teststring
          |""".stripMargin),
      Seq(Row("f", null), Row(null, "asdf"))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_6"."SUBQUERY_6_COL_0" ) AS "SUBQUERY_7_COL_0" ,
         |( "SUBQUERY_6"."SUBQUERY_6_COL_1" ) AS "SUBQUERY_7_COL_1"
         |FROM (
         |SELECT ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) AS "SUBQUERY_6_COL_0" ,
         |( "SUBQUERY_5"."SUBQUERY_5_COL_0" ) AS "SUBQUERY_6_COL_1"
         |FROM (
         |SELECT ( "SUBQUERY_1"."TESTSTRING" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'f\\' ) ) ) AS "SUBQUERY_1" ) AS "SUBQUERY_2"
         |FULL OUTER JOIN
         |( SELECT ( "SUBQUERY_4"."TESTSTRING" ) AS "SUBQUERY_5_COL_0"
         |FROM (
         |SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_3"
         |WHERE ( ( "SUBQUERY_3"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_3"."TESTSTRING" = \\'asdf\\' ) ) ) AS "SUBQUERY_4" )
         |AS "SUBQUERY_5"
         |ON ( "SUBQUERY_2"."SUBQUERY_2_COL_0" = "SUBQUERY_5"."SUBQUERY_5_COL_0" ) )
         |AS "SUBQUERY_6"
         |""".stripMargin
    )
  }

  test("Join with Non-redshift table - partial pushdown") {
    val spark = SparkSession.getActiveSession.get
    import spark.implicits._

    Seq(
      ("asdf", "lang1"),
      ("f", "lang2"),
      ("notexist", "lang3"))
    .toDF("name", "language")
    .createOrReplaceTempView("new_table")

    checkAnswer(
      sqlContext.sql(
        """SELECT table1.teststring, table2.language
          |FROM test_table table1
          |INNER JOIN  new_table table2
          |ON table1.teststring = table2.name
          |ORDER BY table1.teststring
          |""".stripMargin),
      Seq(Row("asdf", "lang1"), Row("f", "lang2"))
    )

    // ORDER BY is not pushed down
    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTSTRING" ) AS "SUBQUERY_2_COL_0" FROM
         |( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("Cast long to decimal") {
    checkAnswer(
      sqlContext.sql(
        """SELECT CAST(testlong AS DECIMAL(20,2)) FROM
          |test_table where testlong > 0 ORDER BY testlong""".stripMargin),
      Seq(Row(1239012341823719d),
        Row(1239012341823719d),
        Row(1239012341823719d),
        Row(1239012341823719d))
    )

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT ( CAST ( "SUBQUERY_1"."TESTLONG" AS DECIMAL(20, 2) ) )
         |AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTLONG" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTLONG" > 0 ) ) ) AS "SUBQUERY_1" ) AS "SUBQUERY_2"
         |ORDER BY ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) ASC
         |""".stripMargin
    )
  }

  test("Distinct pushdown") {
    checkAnswer(
      sqlContext.sql(
        """SELECT DISTINCT testlong FROM
          |test_table where testlong > 0""".stripMargin),
      Seq(Row(1239012341823719d))
    )

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) AS "SUBQUERY_3_COL_0"
         |FROM ( SELECT ( "SUBQUERY_1"."TESTLONG" ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( ( "SUBQUERY_0"."TESTLONG" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTLONG" > 0 ) ) ) AS "SUBQUERY_1" ) AS "SUBQUERY_2"
         |GROUP BY "SUBQUERY_2"."SUBQUERY_2_COL_0"
         |""".stripMargin
    )
  }

  test("Use option query") {
      val df = read
        .option("query",
        s"""SELECT avg(testlong) FROM
          |$test_table WHERE testlong > 0 GROUP BY testlong
          |ORDER BY testlong limit 1""".stripMargin)
        .load()

    checkAnswer(
      df,
      Seq(Row(1239012341823719d))
    )

    checkSqlStatement(
      s"""SELECT * FROM ( SELECT avg(testlong) FROM
         |$test_table WHERE testlong > 0 GROUP BY testlong
         |ORDER BY testlong limit 1 ) AS "RS_CONNECTOR_QUERY_ALIAS"
         |""".stripMargin
    )
  }

  test("Use option query join with Non-redshift table - partial pushdown") {
    val spark = SparkSession.getActiveSession.get
    import spark.implicits._

    Seq(
      ("asdf", "lang1"),
      ("f", "lang2"),
      ("notexist", "lang3")
    )
      .toDF("name", "language")
      .createOrReplaceTempView("new_table")

    read
      .option("query",
        s"""SELECT teststring FROM
           |$test_table WHERE testlong > 0 AND teststring = 'asdf'
           |Limit 1
           |""".stripMargin)
      .load()
      .createOrReplaceTempView("query_table")

    checkAnswer(
      sqlContext.sql(
        """SELECT table1.teststring, table2.language
          |FROM query_table table1
          |INNER JOIN  new_table table2
          |ON table1.teststring = table2.name
          |ORDER BY table1.teststring
          |""".stripMargin),
      Seq(Row("asdf", "lang1"))
    )

    // ORDER BY is not pushed down
    checkSqlStatement(
      s"""SELECT * FROM ( SELECT * FROM ( SELECT teststring FROM
         |$test_table WHERE testlong > 0 AND teststring = \\'asdf\\'
         |Limit 1)
         |AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |WHERE ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |""".stripMargin
    )
  }
}

class DefaultPushdownSqlClauseSuite extends PushdownSqlClauseSuite {
  override protected val s3format: String = "DEFAULT"
  override protected val auto_pushdown: String = "true"
}

class ParquetPushdownSqlClauseSuite extends PushdownSqlClauseSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "true"
}

class DefaultNoPushdownSqlClauseSuite extends PushdownSqlClauseSuite {
  override protected val s3format: String = "DEFAULT"
  override protected val auto_pushdown: String = "false"
}

class ParquetNoPushdownSqlFuncSuite extends PushdownSqlClauseSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}
