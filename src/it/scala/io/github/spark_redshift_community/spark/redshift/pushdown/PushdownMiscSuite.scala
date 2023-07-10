package io.github.spark_redshift_community.spark.redshift.pushdown

import org.apache.spark.sql.Row

class PushdownMiscSuite extends IntegrationPushdownSuiteBase {

  test("Boolean subquery cast to String") {
    checkAnswer(
      sqlContext.sql(
        s"""SELECT CAST((SELECT testbool from test_table WHERE testbool = true) AS STRING)""".stripMargin),
      Seq(Row("true")))

    checkSqlStatement(
      s"""SELECT ( "SUBQUERY_1"."TESTBOOL" ) AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM (
         | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
         | ( ( "SUBQUERY_0"."TESTBOOL" IS NOT NULL ) AND "SUBQUERY_0"."TESTBOOL" ) ) AS
         | "SUBQUERY_1"""".stripMargin
    )
  }

  test("Boolean type cast to String") {
    checkAnswer(
      sqlContext.sql(
        s"""SELECT CAST(testbool AS STRING) FROM test_table WHERE testbool = true""".stripMargin),
      Seq(Row("true")))

    checkSqlStatement(
      s"""SELECT ( CASE "SUBQUERY_1"."TESTBOOL" WHEN TRUE THEN \\'true\\'
         | WHEN FALSE THEN \\'false\\' ELSE \\'null\\' END )
         |  AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT *
         |  FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         | WHERE ( ( "SUBQUERY_0"."TESTBOOL" IS NOT NULL ) AND "SUBQUERY_0"."TESTBOOL" ) )
         | AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("Boolean expression cast to String") {
    checkAnswer(
      sqlContext.sql(
        s"""SELECT CAST(testint > testfloat AS STRING) FROM test_table WHERE testbool = true""".stripMargin),
      Seq(Row("true")))

    checkSqlStatement(
      s"""SELECT ( CASE ( CAST ( "SUBQUERY_1"."TESTINT" AS FLOAT4 ) > "SUBQUERY_1"."TESTFLOAT" )
         | WHEN TRUE THEN \\'true\\' WHEN FALSE THEN \\'false\\' ELSE \\'null\\' END ) AS "SUBQUERY_2_COL_0"
         | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         | WHERE ( ( "SUBQUERY_0"."TESTBOOL" IS NOT NULL ) AND "SUBQUERY_0"."TESTBOOL" ) ) AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("Non-boolean type casts to Strings") {
    // (id, column, result)
    val paramTuples = List(
      ("testbyte", "1"),
      ("testdate", "2015-07-01"),
      ("testint", "42"),
      ("testlong", "1239012341823719"),
      ("testshort", "23"),
      ("testtimestamp", "2015-07-01 00:00:00.001")
    )

    paramTuples.foreach(paramTuple => {
      val column = paramTuple._1
      val result = paramTuple._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT CAST($column AS STRING) FROM test_table WHERE testbool = true""".stripMargin),
        Seq(Row(result)))

      checkSqlStatement(
        s"""SELECT ( CAST ( "SUBQUERY_1"."${column.toUpperCase}" AS VARCHAR ) )
           | AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table
           | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE ( (
           | "SUBQUERY_0"."TESTBOOL" IS NOT NULL ) AND "SUBQUERY_0"."TESTBOOL" ) )
           | AS "SUBQUERY_1"""".stripMargin
      )
    })
  }
}

class TextMiscSuite extends PushdownMiscSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "true"
}

class ParquetMiscSuite extends PushdownMiscSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "true"
}

class TextNoPushdownMiscSuite extends PushdownMiscSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "false"
}

class ParquetNoPushdownMiscSuite extends PushdownMiscSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}
