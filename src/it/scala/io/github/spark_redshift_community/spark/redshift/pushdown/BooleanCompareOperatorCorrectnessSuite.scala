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
package io.github.spark_redshift_community.spark.redshift.pushdown.test

import org.apache.spark.sql.Row

abstract class BooleanCompareOperatorCorrectnessSuite extends IntegrationPushdownSuiteBase {

  override protected val preloaded_data: String = "true"
  override def setTestTableName(): String = """"PUBLIC"."all_shapes_dist_all_sort_compound_12col""""

  val string2000Char =
    """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
      |""".stripMargin

  val string255Char =
    """rl5hpi6nKVTUWAaIbt8xI7G2jsoagoEk23E8aUqsPMpgPMPs2zhPgdnJLpz
      |ANvcLYOafVzwtqchGOV5jsxBJNbpUjAOa0SoUneoYX9vdrfzSTwVuwUzWpSKIzw
      |QzRhyxsYRyaNpiMmyKcSOrgt4Uv6NmOL6yfeq0CtOMFq910WIGbQXOYsX2kvHtuSqb0MA
      |Nf70dqRVopWxDuaXb7Ghpm8xt4CWKq9NCzXB7DDzpYSpqNlryoJjN4sPLKnfb5zF""".stripMargin
      
  test("child GreaterThanOrEqual and LessThan pushdown", PreloadTest) {
    // "Column name", value of right operand, and result size
    val input = List(
      ("col_smallint_raw", 100, 1309),
      ("col_smallint_bytedict", 100, 1282),
      ("col_smallint_delta", 100, 1223),
      ("col_smallint_lzo", 100, 1260),
      ("col_smallint_mostly8", 100, 1247),
      ("col_smallint_runlength", 100, 1216),
      ("col_smallint_zstd", 100, 1300),
      ("col_int_raw", 100, 2008),
      ("col_int_bytedict", 100, 2075),
      ("col_int_delta", 100, 2000),
      ("col_int_delta32k", 100, 2010),
      ("col_int_lzo", 100, 2007),
      ("col_int_mostly8", 100, 2026),
      ("col_int_mostly16", 100, 2059),
      ("col_int_runlength", 100, 1980),
      ("col_int_zstd", 100, 2026),
      ("col_bigint_raw", 100, 2561),
      ("col_bigint_bytedict", 100, 2437),
      ("col_bigint_delta", 100, 2495),
      ("col_bigint_delta32k", 100, 2537),
      ("col_bigint_lzo", 100, 2546),
      ("col_bigint_mostly8", 100, 2468),
      ("col_bigint_mostly16", 100, 2490),
      ("col_bigint_mostly32", 100, 2488),
      ("col_bigint_runlength", 100, 2490),
      ("col_bigint_zstd", 100, 2488),
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2
      val result_size = test_case._3
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name >= $expected_res"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
           |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" >= $expected_res ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name < $expected_res"""),
        Seq(Row(5000 - result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
           |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" < $expected_res ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child GreaterThanOrEqual and LessThan pushdown (int v.s. float type)", PreloadTest) {
    // "Column name", value of right operand, and result size
    val input = List(
      ("col_smallint_raw", 100.0001, 1299),
      ("col_smallint_bytedict", 100.0001, 1271),
      ("col_smallint_delta", 100.0001, 1216),
      ("col_smallint_lzo", 100.0001, 1255),
      ("col_smallint_mostly8", 100.0001, 1238),
      ("col_smallint_runlength", 100.0001, 1200),
      ("col_smallint_zstd", 100.0001, 1281),
      ("col_int_raw", 100.0001, 2003),
      ("col_int_bytedict", 100.0001, 2071),
      ("col_int_delta", 100.0001, 1994),
      ("col_int_delta32k", 100.0001, 2008),
      ("col_int_lzo", 100.0001, 2001),
      ("col_int_mostly8", 100.0001, 2023),
      ("col_int_mostly16", 100.0001, 2053),
      ("col_int_runlength", 100.0001, 1977),
      ("col_int_zstd", 100.0001, 2023),
      ("col_bigint_raw", 100.0001, 2561),
      ("col_bigint_bytedict", 100.0001, 2437),
      ("col_bigint_delta", 100.0001, 2495),
      ("col_bigint_delta32k", 100.0001, 2537),
      ("col_bigint_lzo", 100.0001, 2545),
      ("col_bigint_mostly8", 100.0001, 2468),
      ("col_bigint_mostly16", 100.0001, 2489),
      ("col_bigint_mostly32", 100.0001, 2488),
      ("col_bigint_runlength", 100.0001, 2490),
      ("col_bigint_zstd", 100.0001, 2488),
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2
      val result_size = test_case._3
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name >= $expected_res"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
           |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" >= ${math.ceil(expected_res).toInt} ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name < $expected_res"""),
        Seq(Row(5000 - result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
           |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" < ${math.ceil(expected_res).toInt} ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child LessThanOrEqual and GreaterThan pushdown", PreloadTest) {
    // "Column name", value of right operand, and result size
    val input = List(
      ("col_smallint_raw", 100, 3701),
      ("col_smallint_bytedict", 100, 3729),
      ("col_smallint_delta", 100, 3784),
      ("col_smallint_lzo", 100, 3745),
      ("col_smallint_mostly8", 100, 3762),
      ("col_smallint_runlength", 100, 3800),
      ("col_smallint_zstd", 100, 3719),
      ("col_int_raw", 100, 2997),
      ("col_int_bytedict", 100, 2929),
      ("col_int_delta", 100, 3006),
      ("col_int_delta32k", 100, 2992),
      ("col_int_lzo", 100, 2999),
      ("col_int_mostly8", 100, 2977),
      ("col_int_mostly16", 100, 2947),
      ("col_int_runlength", 100, 3023),
      ("col_int_zstd", 100, 2977),
      ("col_bigint_raw", 100, 2439),
      ("col_bigint_bytedict", 100, 2563),
      ("col_bigint_delta", 100, 2505),
      ("col_bigint_delta32k", 100, 2463),
      ("col_bigint_lzo", 100, 2455),
      ("col_bigint_mostly8", 100, 2532),
      ("col_bigint_mostly16", 100, 2511),
      ("col_bigint_mostly32", 100, 2512),
      ("col_bigint_runlength", 100, 2510),
      ("col_bigint_zstd", 100, 2512),
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2
      val result_size = test_case._3
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name <= $expected_res"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
           |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" <= $expected_res ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name > $expected_res"""),
        Seq(Row(5000 - result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
           |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" > $expected_res ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child LessThanOrEqual and GreaterThan pushdown (int v.s. float)", PreloadTest) {
    // "Column name", value of right operand, and result size
    val input = List(
      ("col_smallint_raw", 100.0001, 3701),
      ("col_smallint_bytedict", 100.0001, 3729),
      ("col_smallint_delta", 100.0001, 3784),
      ("col_smallint_lzo", 100.0001, 3745),
      ("col_smallint_mostly8", 100.0001, 3762),
      ("col_smallint_runlength", 100.0001, 3800),
      ("col_smallint_zstd", 100.0001, 3719),
      ("col_int_raw", 100.0001, 2997),
      ("col_int_bytedict", 100.0001, 2929),
      ("col_int_delta", 100.0001, 3006),
      ("col_int_delta32k", 100.0001, 2992),
      ("col_int_lzo", 100.0001, 2999),
      ("col_int_mostly8", 100.0001, 2977),
      ("col_int_mostly16", 100.0001, 2947),
      ("col_int_runlength", 100.0001, 3023),
      ("col_int_zstd", 100.0001, 2977),
      ("col_bigint_raw", 100.0001, 2439),
      ("col_bigint_bytedict", 100.0001, 2563),
      ("col_bigint_delta", 100.0001, 2505),
      ("col_bigint_delta32k", 100.0001, 2463),
      ("col_bigint_lzo", 100.0001, 2455),
      ("col_bigint_mostly8", 100.0001, 2532),
      ("col_bigint_mostly16", 100.0001, 2511),
      ("col_bigint_mostly32", 100.0001, 2512),
      ("col_bigint_runlength", 100.0001, 2510),
      ("col_bigint_zstd", 100.0001, 2512),
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2
      val result_size = test_case._3
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name <= $expected_res"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
           |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" <= ${math.floor(expected_res).toInt} ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name > $expected_res"""),
        Seq(Row(5000 - result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0"
           |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" > ${math.floor(expected_res).toInt} ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child GreaterThanOrEqual and LessThan pushdown - decimal 18 scale", PreloadTest) {
    // "Column name", value of right operand, and result size
    val input = List(
      ("col_decimal_18_18_raw", "-1.230000000000000000", 5000),
      ("col_decimal_18_18_bytedict", "-1.230000000000000000", 5000),
      ("col_decimal_18_18_delta", "-1.230000000000000000", 5000),
      ("col_decimal_18_18_delta32k", "-1.230000000000000000", 5000),
      ("col_decimal_18_18_lzo", "-1.230000000000000000", 5000),
      ("col_decimal_18_18_mostly8", "-1.230000000000000000", 5000),
      ("col_decimal_18_18_mostly16", "-1.230000000000000000", 5000),
      ("col_decimal_18_18_mostly32", "-1.230000000000000000", 5000),
      ("col_decimal_18_18_runlength", "-1.230000000000000000", 5000),
      ("col_decimal_18_18_zstd", "-1.230000000000000000", 5000)
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2
      val result_size = test_case._3
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name >= $expected_res"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0" WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( CAST ( "SQ_0"."$column_name" AS DECIMAL(19, 18) ) >= $expected_res ) ) )
           |AS "SQ_1" LIMIT 1
           |""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name < $expected_res"""),
        Seq(Row(5000 - result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0" WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( CAST ( "SQ_0"."$column_name" AS DECIMAL(19, 18) ) < $expected_res ) ) )
           |AS "SQ_1" LIMIT 1
           |""".stripMargin)
    })
  }

  test("child LessThanOrEqual and GreaterThan pushdown - decimal 18 scale", PreloadTest) {
    // "Column name", value of right operand, and result size
    val input = List(
      ("col_decimal_18_18_raw", "-1.230000000000000000", 0),
      ("col_decimal_18_18_bytedict", "-1.230000000000000000", 0),
      ("col_decimal_18_18_delta", "-1.230000000000000000", 0),
      ("col_decimal_18_18_delta32k", "-1.230000000000000000", 0),
      ("col_decimal_18_18_lzo", "-1.230000000000000000", 0),
      ("col_decimal_18_18_mostly8", "-1.230000000000000000", 0),
      ("col_decimal_18_18_mostly16", "-1.230000000000000000", 0),
      ("col_decimal_18_18_mostly32", "-1.230000000000000000", 0),
      ("col_decimal_18_18_runlength", "-1.230000000000000000", 0),
      ("col_decimal_18_18_zstd", "-1.230000000000000000", 0)
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2
      val result_size = test_case._3
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name <= $expected_res"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0" WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( CAST ( "SQ_0"."$column_name" AS DECIMAL(19, 18) ) <= $expected_res ) ) )
           |AS "SQ_1" LIMIT 1
           |""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name > $expected_res"""),
        Seq(Row(5000 - result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0" WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( CAST ( "SQ_0"."$column_name" AS DECIMAL(19, 18) ) > $expected_res ) ) )
           |AS "SQ_1" LIMIT 1
           |""".stripMargin)
    })
  }

  test("child LessThanOrEqual and GreaterThan pushdown - decimal 18 scale v.s int", PreloadTest) {
    // "Column name", value of right operand, and result size
    val input = List(
      ("col_decimal_18_18_raw", "-1", 0),
      ("col_decimal_18_18_bytedict", "-1", 0),
      ("col_decimal_18_18_delta", "-1", 0),
      ("col_decimal_18_18_delta32k", "-1", 0),
      ("col_decimal_18_18_lzo", "-1", 0),
      ("col_decimal_18_18_mostly8", "-1", 0),
      ("col_decimal_18_18_mostly16", "-1", 0),
      ("col_decimal_18_18_mostly32", "-1", 0),
      ("col_decimal_18_18_runlength", "-1", 0),
      ("col_decimal_18_18_zstd", "-1", 0)
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2
      val result_size = test_case._3
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name <= $expected_res"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0" WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( CAST ( "SQ_0"."$column_name" AS DECIMAL(19, 18) )
           |<= -1.000000000000000000 ) ) ) AS "SQ_1" LIMIT 1""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name > $expected_res"""),
        Seq(Row(5000 - result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0" WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( CAST ( "SQ_0"."$column_name" AS DECIMAL(19, 18) )
           |> -1.000000000000000000 ) ) ) AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child GreaterThanOrEqual and LessThan pushdown - decimal 37 scale", PreloadTest) {
    // "Column name", value of right operand, and result size
    val input = List(
      ("col_decimal_38_37_raw", "0.7664120400000000000000000000000000001", 1311),
      ("col_decimal_38_37_bytedict", "0.7664120400000000000000000000000000001", 1282),
      ("col_decimal_38_37_delta", "0.7664120400000000000000000000000000001", 1298),
      ("col_decimal_38_37_delta32k", "0.7664120400000000000000000000000000001", 1271),
      ("col_decimal_38_37_lzo", "0.7664120400000000000000000000000000001", 1220),
      ("col_decimal_38_37_mostly8", "0.7664120400000000000000000000000000001", 1298),
      ("col_decimal_38_37_mostly16", "0.7664120400000000000000000000000000001", 1305),
      ("col_decimal_38_37_mostly32", "0.7664120400000000000000000000000000001", 1263),
      ("col_decimal_38_37_runlength", "0.7664120400000000000000000000000000001", 1318),
      ("col_decimal_38_37_zstd", "0.7664120400000000000000000000000000001", 1347)
    )
    input.par.foreach( test_case => {

      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2
      val result_size = test_case._3
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name >= $expected_res"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" >= $expected_res ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name < $expected_res"""),
        Seq(Row(5000 - result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" < $expected_res ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child LessThanOrEqual and GreaterThan pushdown - decimal 37 scale", PreloadTest) {
    // "Column name", value of right operand, and result size
    val input = List(
      ("col_decimal_38_37_raw", "0.7664120400000000000000000000000000001", 3689),
      ("col_decimal_38_37_bytedict", "0.7664120400000000000000000000000000001", 3718),
      ("col_decimal_38_37_delta", "0.7664120400000000000000000000000000001", 3702),
      ("col_decimal_38_37_delta32k", "0.7664120400000000000000000000000000001", 3729),
      ("col_decimal_38_37_lzo", "0.7664120400000000000000000000000000001", 3780),
      ("col_decimal_38_37_mostly8", "0.7664120400000000000000000000000000001", 3702),
      ("col_decimal_38_37_mostly16", "0.7664120400000000000000000000000000001", 3695),
      ("col_decimal_38_37_mostly32", "0.7664120400000000000000000000000000001", 3737),
      ("col_decimal_38_37_runlength", "0.7664120400000000000000000000000000001", 3682),
      ("col_decimal_38_37_zstd", "0.7664120400000000000000000000000000001", 3653),
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2
      val result_size = test_case._3
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name <= $expected_res"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" <= $expected_res ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name > $expected_res"""),
        Seq(Row(5000 - result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" > $expected_res ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child GreaterThanOrEqual and LessThan pushdown - string + varchar types", PreloadTest) {
    // "Column name", value of right operand, and result size
    val input = List(
      ("col_char_1_raw", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_char_255_raw", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_char_2000_raw", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_char_max_raw", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_char_1_bytedict", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_char_255_bytedict", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_char_2000_bytedict", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_char_max_bytedict", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_char_1_lzo", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_char_255_lzo", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_char_2000_lzo", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_char_max_lzo", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_char_1_runlength", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_char_255_runlength", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_char_2000_runlength", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_char_max_runlength", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_char_1_zstd", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_char_255_zstd", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_char_2000_zstd", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_char_max_zstd", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_1_raw", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_255_raw", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_2000_raw", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_max_raw", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_1_bytedict", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_255_bytedict", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_2000_bytedict", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_max_bytedict", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_1_lzo", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_255_lzo", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_2000_lzo", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_max_lzo", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_1_runlength", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_255_runlength", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_2000_runlength", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_max_runlength", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_1_text255", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_255_text255", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_1_text32k", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_255_text32k", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_2000_text32k", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_max_text32k", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_1_zstd", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_255_zstd", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_2000_zstd", s"'$string255Char'", s"\\'$string255Char\\'", 0),
      ("col_varchar_max_zstd", s"'$string255Char'", s"\\'$string255Char\\'", 0)
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val query_in = test_case._2
      val expected_res = test_case._3
      val result_size = test_case._4
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name >= $query_in"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM
           |( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0" WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" >= $expected_res ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name < $query_in"""),
        Seq(Row(5000 - result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM
           |( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0" WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" < $expected_res ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child LessThanOrEqual and GreaterThan pushdown - string + varchar types", PreloadTest) {
    // "Column name", value of right operand, and result size
    val input = List(
      ("col_char_1_raw", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_char_255_raw", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_char_2000_raw", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_char_max_raw", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_char_1_bytedict", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_char_255_bytedict", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_char_2000_bytedict", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_char_max_bytedict", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_char_1_lzo", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_char_255_lzo", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_char_2000_lzo", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_char_max_lzo", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_char_1_runlength", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_char_255_runlength", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_char_2000_runlength", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_char_max_runlength", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_char_1_zstd", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_char_255_zstd", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_char_2000_zstd", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_char_max_zstd", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_1_raw", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_255_raw", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_2000_raw", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_max_raw", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_1_bytedict", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_255_bytedict", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_2000_bytedict", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_max_bytedict", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_1_lzo", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_255_lzo", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_2000_lzo", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_max_lzo", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_1_runlength", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_255_runlength", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_2000_runlength", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_max_runlength", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_1_text255", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_255_text255", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_1_text32k", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_255_text32k", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_2000_text32k", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_max_text32k", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_1_zstd", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_255_zstd", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_2000_zstd", s"'$string255Char'", s"\\'$string255Char\\'", 5000),
      ("col_varchar_max_zstd", s"'$string255Char'", s"\\'$string255Char\\'", 5000)
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val query_in = test_case._2
      val expected_res = test_case._3
      val result_size = test_case._4
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name <= $query_in"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM
           |( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0" WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" <= $expected_res ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name > $query_in"""),
        Seq(Row(5000 - result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM
           |( SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0" WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" > $expected_res ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child GreaterThanOrEqual and LessThan pushdown - date type", PreloadTest) {
    // "Column name", value of right operand, and result size
    val input = List(
      ("col_date_raw", "'2010-05-11'", 824),
      ("col_date_bytedict", "'2010-05-11'", 866),
      ("col_date_delta", "'2010-05-11'", 883),
      ("col_date_delta32k", "'2010-05-11'", 893),
      ("col_date_lzo", "'2010-05-11'", 825),
      ("col_date_runlength", "'2010-05-11'", 870),
      ("col_date_zstd", "'2010-05-11'", 871)
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val query_in = test_case._2
      val result_size = test_case._3
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name >= $query_in"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0" WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" >=
           |DATEADD(day, 14740 , TO_DATE(\\'1970-01-01\\', \\'YYYY-MM-DD\\')) ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name < $query_in"""),
        Seq(Row(5000 - result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0" WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" <
           |DATEADD(day, 14740 , TO_DATE(\\'1970-01-01\\', \\'YYYY-MM-DD\\')) ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child LessThanOrEqual and GreaterThan pushdown - date type", PreloadTest) {
    // "Column name", value of right operand, and result size
    val input = List(
      ("col_date_raw", "'2010-05-11'", 4178),
      ("col_date_bytedict", "'2010-05-11'", 4134),
      ("col_date_delta", "'2010-05-11'", 4117),
      ("col_date_delta32k", "'2010-05-11'", 4107),
      ("col_date_lzo", "'2010-05-11'", 4175),
      ("col_date_runlength", "'2010-05-11'", 4133),
      ("col_date_zstd", "'2010-05-11'", 4129)
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val query_in = test_case._2
      val result_size = test_case._3
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name <= $query_in"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0" WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" <=
           |DATEADD(day, 14740 , TO_DATE(\\'1970-01-01\\', \\'YYYY-MM-DD\\')) ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name > $query_in"""),
        Seq(Row(5000 - result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0" WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" >
           |DATEADD(day, 14740 , TO_DATE(\\'1970-01-01\\', \\'YYYY-MM-DD\\')) ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child GreaterThanOrEqual and LessThan pushdown - timestamp type", PreloadTest) {
    // "Column name", value of right operand, and result size
    val input = List(
      ("col_timestamp_raw", "'1994-05-19 01:03:02'", 2519),
      ("col_timestamp_bytedict", "'1994-05-19 01:03:02'", 2534),
      ("col_timestamp_delta", "'1994-05-19 01:03:02'", 2534),
      ("col_timestamp_delta32k", "'1994-05-19 01:03:02'", 2522),
      ("col_timestamp_lzo", "'1994-05-19 01:03:02'", 2512),
      ("col_timestamp_runlength", "'1994-05-19 01:03:02'", 2500),
      ("col_timestamp_zstd", "'1994-05-19 01:03:02'", 2524),
      ("col_timestamptz_raw", "'1994-05-19 01:03:02'", 2505),
      ("col_timestamptz_bytedict", "'1994-05-19 01:03:02'", 2536),
      ("col_timestamptz_delta", "'1994-05-19 01:03:02'", 2438),
      ("col_timestamptz_delta32k", "'1994-05-19 01:03:02'", 2559),
      ("col_timestamptz_lzo", "'1994-05-19 01:03:02'", 2461),
      ("col_timestamptz_runlength", "'1994-05-19 01:03:02'", 2465),
      ("col_timestamptz_zstd", "'1994-05-19 01:03:02'", 2514),
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val query_in = test_case._2
      val result_size = test_case._3
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name >= $query_in"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" >=\\'1994-05-19 01:03:02\\' ::TIMESTAMP ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name < $query_in"""),
        Seq(Row(5000 - result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" < \\'1994-05-19 01:03:02\\' ::TIMESTAMP ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child LessThanOrEqual and GreaterThan pushdown - timestamp type", PreloadTest) {
    // "Column name", value of right operand, and result size
    val input = List(
      ("col_timestamp_raw", "'1994-05-19 01:03:02'", 2481),
      ("col_timestamp_bytedict", "'1994-05-19 01:03:02'", 2466),
      ("col_timestamp_delta", "'1994-05-19 01:03:02'", 2466),
      ("col_timestamp_delta32k", "'1994-05-19 01:03:02'", 2478),
      ("col_timestamp_lzo", "'1994-05-19 01:03:02'", 2488),
      ("col_timestamp_runlength", "'1994-05-19 01:03:02'", 2500),
      ("col_timestamp_zstd", "'1994-05-19 01:03:02'", 2476),
      ("col_timestamptz_raw", "'1994-05-19 01:03:02'", 2495),
      ("col_timestamptz_bytedict", "'1994-05-19 01:03:02'", 2464),
      ("col_timestamptz_delta", "'1994-05-19 01:03:02'", 2562),
      ("col_timestamptz_delta32k", "'1994-05-19 01:03:02'", 2441),
      ("col_timestamptz_lzo", "'1994-05-19 01:03:02'", 2539),
      ("col_timestamptz_runlength", "'1994-05-19 01:03:02'", 2535),
      ("col_timestamptz_zstd", "'1994-05-19 01:03:02'", 2486),
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val query_in = test_case._2
      val result_size = test_case._3
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name <= $query_in"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" <= \\'1994-05-19 01:03:02\\' ::TIMESTAMP ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name > $query_in"""),
        Seq(Row(5000 - result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" > \\'1994-05-19 01:03:02\\' ::TIMESTAMP ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child GreaterThanOrEqual and LessThan pushdown - float type", PreloadTest) {
    // "Column name", value of right operand, and result size
    val input = List(
      ("col_float4_raw", -26.2983, 4144, "::float4"),
      ("col_float4_bytedict", -26.2983, 4128, "::float4"),
      ("col_float4_runlength", -26.2983, 4154, "::float4"),
      ("col_float4_zstd", -26.2983, 4164, "::float4"),
      ("col_float8_raw", -6.5868966897085, 2805, ""),
      ("col_float8_bytedict", -6.5868966897085, 2808, ""),
      ("col_float8_runlength", -6.5868966897085, 2747, ""),
      ("col_float8_zstd", -6.5868966897085, 2792, "")
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2
      val result_size = test_case._3
      val cast = test_case._4
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name >= $expected_res"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" >= $expected_res $cast ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name < $expected_res"""),
        Seq(Row(5000 - result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" < $expected_res $cast ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child LessThanOrEqual and GreaterThan pushdown - float4 type", PreloadTest) {
    // "Column name", value of right operand, and result size
    val input = List(
      ("col_float4_raw", 10.8749, 3204),
      ("col_float4_bytedict", 23.4677, 3956),
      ("col_float4_runlength", 23.1635, 3956),
      ("col_float4_zstd", 6.57506, 2921)
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2
      val result_size = test_case._3
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name <= $expected_res"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" <= $expected_res::float4) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name > $expected_res"""),
        Seq(Row(5000 - result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" > $expected_res::float4 ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("child LessThanOrEqual and GreaterThan pushdown - float8 type", PreloadTest) {
    // "Column name", value of right operand, and result size
    val input = List(
      ("col_float8_raw", -6.5868966897085, 2195),
      ("col_float8_bytedict", -6.5868966897085, 2192),
      ("col_float8_runlength", -6.5868966897085, 2253),
      ("col_float8_zstd", -6.5868966897085, 2208)
    )
    input.par.foreach( test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2
      val result_size = test_case._3
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name <= $expected_res"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" <= $expected_res ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name > $expected_res"""),
        Seq(Row(5000 - result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SQ_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" )
           |AS "SQ_0"
           |WHERE ( ( "SQ_0"."$column_name" IS NOT NULL )
           |AND ( "SQ_0"."$column_name" > $expected_res ) ) )
           |AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

}

class TextBooleanCompareOperatorCorrectnessSuite extends BooleanCompareOperatorCorrectnessSuite {
  override protected val s3format: String = "TEXT"
}

class ParquetBooleanCompareOperatorCorrectnessSuite extends BooleanCompareOperatorCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
}

class TextNoPushdownBooleanCompareOperatorCorrectnessSuite
  extends BooleanCompareOperatorCorrectnessSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "false"
}

class ParquetNoPushdownBooleanCompareOperatorCorrectnessSuite
  extends BooleanCompareOperatorCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}

class TextNoCacheBooleanCompareOperatorCorrectnessSuite
  extends TextBooleanCompareOperatorCorrectnessSuite {
  override protected val s3_result_cache = "false"
}

class ParquetNoCacheBooleanCompareOperatorCorrectnessSuite
  extends ParquetBooleanCompareOperatorCorrectnessSuite {
  override protected val s3_result_cache = "false"
}
