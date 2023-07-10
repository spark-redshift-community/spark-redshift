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

abstract class PushdownStringFuncSuite extends IntegrationPushdownSuiteBase {
  test("Upper pushdown", P0Test, P1Test) {
    checkAnswer(
      sqlContext.sql("""SELECT UPPER(testString) FROM test_table WHERE testString='asdf'"""),
      Seq(Row("ASDF"))
    )

    checkSqlStatement(
		expectedAnswerSpark3_2 = s"""SELECT ( UPPER ( "SUBQUERY_1"."TESTSTRING" ) )
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
		expectedAnswerSpark3_2 = s"""SELECT ( LOWER ( "SUBQUERY_1"."TESTSTRING" ) )
      |AS "SUBQUERY_2_COL_0"
      |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
      |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTBOOL" IS NOT NULL )
      |AND ( "SUBQUERY_0"."TESTBOOL" = true ) ) ) AS "SUBQUERY_1"
      |""".stripMargin,
    expectedAnswerSpark3_3 = s"""SELECT ( LOWER ( "SUBQUERY_1"."TESTSTRING" ) )
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
		expectedAnswerSpark3_2 = s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTSTRING" , 1 , 2  ) )
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
		expectedAnswerSpark3_2 = s"""SELECT ( SUBSTRING ( "SUBQUERY_1"."TESTSTRING" , 1 , 2  ) )
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
		expectedAnswerSpark3_2 = s"""SELECT ( LENGTH ( "SUBQUERY_1"."TESTSTRING" ) )
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
		expectedAnswerSpark3_2 = s"""SELECT ( CONCAT ( "SUBQUERY_1"."TESTSTRING" , \\'Test\\' ) )
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
		expectedAnswerSpark3_2 = s"""SELECT ( ASCII ( "SUBQUERY_1"."TESTSTRING" ) )
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
		expectedAnswerSpark3_2 = s"""SELECT (
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
		expectedAnswerSpark3_2 = s"""SELECT ( LPAD ( "SUBQUERY_1"."TESTSTRING" , 6 , \\'_\\' )  )
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
		expectedAnswerSpark3_2 = s"""SELECT ( RPAD ( "SUBQUERY_1"."TESTSTRING" , 6 , \\'_\\' )  )
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
		expectedAnswerSpark3_2 = s"""SELECT ( TRIM (
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
		expectedAnswerSpark3_2 = s"""SELECT ( TRIM (
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
		expectedAnswerSpark3_2 = s"""SELECT ( TRIM ( LPAD (
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
		expectedAnswerSpark3_2 = s"""SELECT ( LTRIM ( LPAD (
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
		expectedAnswerSpark3_2 = s"""SELECT ( RTRIM (
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
		expectedAnswerSpark3_2 = s"""SELECT ( LTRIM (
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
		expectedAnswerSpark3_2 = s"""SELECT ( RTRIM (
         |RPAD ( "SUBQUERY_1"."TESTSTRING" , 6 , \\'_\\' ) , \\'_\\' ) ) AS "SUBQUERY_2_COL_0"
         |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."TESTSTRING" IS NOT NULL )
         |AND ( "SUBQUERY_0"."TESTSTRING" = \\'asdf\\' ) ) )
         |AS "SUBQUERY_1"""".stripMargin
    )
  }
}

class TextStringFuncPushdownSuite extends PushdownStringFuncSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "true"
}

class ParquetStringFuncPushdownSuite extends PushdownStringFuncSuite {
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
