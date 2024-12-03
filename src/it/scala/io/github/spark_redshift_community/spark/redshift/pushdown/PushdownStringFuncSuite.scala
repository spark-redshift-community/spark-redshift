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
import org.apache.spark.sql.{Row, SaveMode}

import scala.collection.immutable.Seq

abstract class PushdownStringFuncSuite extends IntegrationPushdownSuiteBase {

  /*
  test("Test GDC V1 Read") {
    sqlContext.sparkSession.sql(
//      "set spark.datasource.redshift.community.glue_endpoint = https://glue-gamma.us-east-1.amazonaws.com"
      "set spark.datasource.redshift.community.glue_endpoint = https://glue.us-east-1.amazonaws.com"
    )

    sqlContext.read
      .format("io.github.spark_redshift_community.spark.redshift")
      .option("glue_database", "bsharifi-test-glue-db")
//      .option("glue_database", "bsharifi-test-empty-glue-db")
      .option("dbtable", "table1")
      .load()
      .show()
  }

  test("Test GDC V1 Read2") {
    sqlContext.sparkSession.sql(
      //      "set spark.datasource.redshift.community.glue_endpoint = https://glue-gamma.us-east-1.amazonaws.com"
      "set spark.datasource.redshift.community.glue_endpoint = https://glue.us-east-1.amazonaws.com"
    )

    sqlContext.read
      .format("io.github.spark_redshift_community.spark.redshift")
      .option("glue_database", "bsharifi-test-glue-db")
      //      .option("glue_database", "bsharifi-test-empty-glue-db")
      .option("dbtable", "table1")
      .load()
      .show()
  }

  test("Test GDC V1 Write") {
    // This test can be simplified once #98 is fixed.
    val tableName = s"roundtrip_save_and_load_$randomSuffix"
    try {
      val df = sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData), TestUtils.testSchema)
      df.write
        .format("io.github.spark_redshift_community.spark.redshift")
        .option("glue_database", "bsharifi-test-glue-db")
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(redshiftWrapper.tableExists(conn, tableName))
      checkAnswer(sqlContext.read
        .format("io.github.spark_redshift_community.spark.redshift")
        .option("glue_database", "bsharifi-test-glue-db")
        .option("dbtable", tableName).load(), TestUtils.expectedData)
    } finally {
      redshiftWrapper.executeUpdate(conn, s"drop table if exists $tableName")
    }
  }
  */

  /*
  test ("Tahoe public schema") {
    val df1 = read.option("dbtable", "awsdatacatalog.tahoe_db.\"public.table1\"").load()
    df1.createOrReplaceTempView("temp1")
    sqlContext.sql("select * from temp1").show()

    val df2 = read.option("dbtable", "awsdatacatalog.tahoe_db.\"test_schema.table2\"").load()
    df2.createOrReplaceTempView("temp2")
    sqlContext.sql("select * from temp2").show()
  }


  test ("three part table name") {
    val df1 = read.option("dbtable", "cons_data_share.public.table1").load()
    df1.createOrReplaceTempView("temp1")
    sqlContext.sql("select * from temp1").show()

    val df2 = read.option("dbtable", "cons_data_share.test_schema.table2").load()
    df2.createOrReplaceTempView("temp2")
    sqlContext.sql("select * from temp2").show()
  }

  test ("three part query name") {
    val df1 = read.option("query", "select * from cons_data_share.public.table1").load()
    df1.createOrReplaceTempView("temp1")
    sqlContext.sql("select * from temp1").show()

    val df2 = read.option("query", "select * from cons_data_share.test_schema.table2").load()
    df2.createOrReplaceTempView("temp2")
    sqlContext.sql("select * from temp2").show()
  }

  test("DataAPI - basic serverless test") {
      read.option("query", "select * from foo").load().show()
    }

  test("MixedCase-dbtable") {
    sqlContext.sql("set spark.sql.caseSensitive=true")

    val simple_df = read
      .option("dbtable", "MixedCaseSchema.MixedCaseTable")
      .load()
      .createOrReplaceTempView("MixedCaseTable")

    sqlContext.sql("select * from MixedCaseTable").show()
  }

  test("MixedCase-query") {
    sqlContext.sql("set spark.sql.caseSensitive=true")

    val simple_df = read
      .option("query", """select "mIXEDcASEcOLUMN" from "MixedCaseSchema"."mIXEDcASEtABLE"""")
      .load()
      .createOrReplaceTempView("MixedCaseTable")

    sqlContext.sql("select * from MixedCaseTable").show()
  }

  test("MixedCase-Alias") {
    sqlContext.sql("set spark.sql.caseSensitive=true")

    val simple_df = read
      .option("dbtable", """"MixedCaseSchema"."mIXEDcASEtABLE"""")
      .load()
      .createOrReplaceTempView("MixedCaseTable")

    sqlContext.sql("SELECT FooBar FROM (select MixedCaseColumn AS FooBar from MixedCaseTable)").show()
  }
  */

  test("Upper pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql("""SELECT UPPER(testString) FROM test_table WHERE testString='asdf'"""),
      Seq(Row("ASDF"))
    )

    checkSqlStatement(
      s"""SELECT ( UPPER ( "SUBQUERY_1"."TESTSTRING" ) )
         |AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'asdf\\' ) ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("Lower pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql("""SELECT LOWER(testString) FROM test_table WHERE testbool=true"""),
      Seq(Row("unicode's樂趣"))
    )

    checkSqlStatement(
      s"""SELECT ( LOWER ( "SUBQUERY_1"."TESTSTRING" ) )
         |AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTBOOL" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTBOOL" = true ) ) ) AS "SUBQUERY_1"
         |""".stripMargin,
      s"""SELECT ( LOWER ( "SUBQUERY_1"."TESTSTRING" ) )
         |AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTBOOL" IS NOT NULL )
         |AND "SUBQUERY_0"."TESTBOOL" ) ) AS "SUBQUERY_1"
         |""".stripMargin
    )
  }

  test("Substring pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT SUBSTRING(testString, 1, 2)
          |FROM test_table WHERE testString='asdf'""".stripMargin),
      Seq(Row("as"))
    )

    checkSqlStatement(
      s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTSTRING" , 1 , 2  ) )
         |AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'asdf\\' ) ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("Substr pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT SUBSTR(testString, 1, 2)
          |FROM test_table WHERE testString='asdf'""".stripMargin),
      Seq(Row("as"))
    )

    checkSqlStatement(
      s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTSTRING" , 1 , 2  ) )
         |AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'asdf\\' ) ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("Length pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT LENGTH(testString)
          |FROM test_table WHERE testString='asdf'""".stripMargin),
      Seq(Row(4))
    )

    checkSqlStatement(
      s"""SELECT ( LENGTH ( "SUBQUERY_1"."TESTSTRING" ) )
         |AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'asdf\\' ) ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("Concat pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT Concat(testString, 'Test')
          |FROM test_table WHERE testString='asdf'""".stripMargin),
      Seq(Row("asdfTest"))
    )

    checkSqlStatement(
      s"""SELECT ( CONCAT ( "SUBQUERY_1"."TESTSTRING" , \\'Test\\' ) )
         |AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'asdf\\' ) ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("Ascii pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT ASCII(testString)
          |FROM test_table WHERE testString='asdf'""".stripMargin),
      Seq(Row(97))
    )

    checkSqlStatement(
      s"""SELECT ( ASCII ( "SUBQUERY_1"."TESTSTRING" ) )
         |AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'asdf\\' ) ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("Translate pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT TRANSLATE(testString,'ad','ce')
          |FROM test_table WHERE testString='asdf'""".stripMargin),
      Seq(Row("csef"))
    )

    checkSqlStatement(
      s"""SELECT (
         |TRANSLATE ( "SUBQUERY_1"."TESTSTRING" , \\'ad\\' , \\'ce\\' ) ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'asdf\\' ) ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("Lpad pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT LPAD(testString,6,'_')
          |FROM test_table WHERE testString='asdf'""".stripMargin),
      Seq(Row("__asdf"))
    )

    checkSqlStatement(
      s"""SELECT ( LPAD ( "SUBQUERY_1"."TESTSTRING" , 6 , \\'_\\' )  )
         |AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'asdf\\' ) ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("Rpad pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT RPAD(testString,6,'_')
          |FROM test_table WHERE testString='asdf'""".stripMargin),
      Seq(Row("asdf__"))
    )

    checkSqlStatement(
      s"""SELECT ( RPAD ( "SUBQUERY_1"."TESTSTRING" , 6 , \\'_\\' )  )
         |AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'asdf\\' ) ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("Trim pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT TRIM('_', RPAD(testString,6,'_'))
          |FROM test_table WHERE testString='asdf'""".stripMargin),
      Seq(Row("asdf"))
    )

    checkSqlStatement(
      s"""SELECT ( TRIM (
         |RPAD ( "SUBQUERY_1"."TESTSTRING" , 6 , \\'_\\' ) , \\'_\\' ) ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'asdf\\' ) ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("Trim From pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT TRIM('_' FROM RPAD(testString,6,'_'))
          |FROM test_table WHERE testString='asdf'""".stripMargin),
      Seq(Row("asdf"))
    )

    checkSqlStatement(
      s"""SELECT ( TRIM (
         |RPAD ( "SUBQUERY_1"."TESTSTRING" , 6 , \\'_\\' ) , \\'_\\' ) ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'asdf\\' ) ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("Trim Both From pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT TRIM(BOTH FROM LPAD(RPAD(testString,6,' '),8,' '))
          |FROM test_table WHERE testString='asdf'""".stripMargin),
      Seq(Row("asdf"))
    )

    checkSqlStatement(
      s"""SELECT ( TRIM ( LPAD (
         |RPAD ( "SUBQUERY_1"."TESTSTRING" , 6 , \\' \\' ) , 8 , \\' \\' ) , \\' \\') )
         |AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'asdf\\' ) ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("Trim Leading From pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT TRIM(LEADING '_' FROM LPAD(TESTSTRING,6,'_'))
          |FROM test_table WHERE TESTSTRING='asdf'""".stripMargin),
      Seq(Row("asdf"))
    )

    checkSqlStatement(
      s"""SELECT ( LTRIM ( LPAD (
         |"SUBQUERY_1"."TESTSTRING" , 6 , \\'_\\' ) , \\'_\\' ) ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'asdf\\' ) ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("Trim Trailing From pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT TRIM(TRAILING '_' FROM RPAD(testString,6,'_'))
          |FROM test_table WHERE testString='asdf'""".stripMargin),
      Seq(Row("asdf"))
    )

    checkSqlStatement(
      s"""SELECT ( RTRIM (
         |RPAD ( "SUBQUERY_1"."TESTSTRING" , 6 , \\'_\\' ) ,\\'_\\' ) ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'asdf\\' ) ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("LTrim pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT LTRIM('_', LPAD(testString,6,'_'))
          |FROM test_table WHERE testString='asdf'""".stripMargin),
      Seq(Row("asdf"))
    )

    checkSqlStatement(
      s"""SELECT ( LTRIM (
         |LPAD ( "SUBQUERY_1"."TESTSTRING" , 6 , \\'_\\' ) , \\'_\\' ) ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'asdf\\' ) ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }

  test("RTrim pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql(
        """SELECT RTRIM('_', RPAD(testString,6,'_'))
          |FROM test_table WHERE testString='asdf'""".stripMargin),
      Seq(Row("asdf"))
    )

    checkSqlStatement(
      s"""SELECT ( RTRIM (
         |RPAD ( "SUBQUERY_1"."TESTSTRING" , 6 , \\'_\\' ) , \\'_\\' ) ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'asdf\\' ) ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }
}

class TextPushdownStringFuncSuite extends PushdownStringFuncSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "true"
}

class ParquetPushdownStringFuncSuite extends PushdownStringFuncSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "true"
}

class TextNoPushdownStringFuncSuite extends PushdownStringFuncSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "false"
}

class ParquetNoPushdownStringFuncSuite extends PushdownStringFuncSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}

class TextNoCachePushdownStringFuncSuite extends TextPushdownStringFuncSuite {
  override protected val s3_result_cache = "false"
}

class ParquetNoCachePushdownStringFuncSuite extends ParquetPushdownStringFuncSuite {
  override protected val s3_result_cache = "false"
}
