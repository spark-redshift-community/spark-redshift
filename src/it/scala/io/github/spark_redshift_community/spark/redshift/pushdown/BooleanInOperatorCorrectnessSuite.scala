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
import org.scalatest.DoNotDiscover

abstract class BooleanInOperatorCorrectnessSuite extends IntegrationPushdownSuiteBase {

  override protected val preloaded_data: String = "true"
  override def setTestTableName(): String = """"PUBLIC"."all_shapes_dist_all_sort_compound_12col""""

  var string2000Char =
    """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
      |""".stripMargin

  var string255Char =
    """rl5hpi6nKVTUWAaIbt8xI7G2jsoagoEk23E8aUqsPMpgPMPs2zhPgdnJLpz
      |ANvcLYOafVzwtqchGOV5jsxBJNbpUjAOa0SoUneoYX9vdrfzSTwVuwUzWpSKIzw
      |QzRhyxsYRyaNpiMmyKcSOrgt4Uv6NmOL6yfeq0CtOMFq910WIGbQXOYsX2kvHtuSqb0MA
      |Nf70dqRVopWxDuaXb7Ghpm8xt4CWKq9NCzXB7DDzpYSpqNlryoJjN4sPLKnfb5zF""".stripMargin
      
  test("child in pushdown") {
    // "Column name" and result size
    val input = List(
      ("col_smallint_raw", "(0,1)", 21),
      ("col_smallint_bytedict", "(0,1)", 34),
      ("col_smallint_delta", "(0,1)", 24),
      ("col_smallint_lzo", "(0,1)", 18),
      ("col_smallint_mostly8", "(0,1)", 17),
      ("col_smallint_runlength", "(0,1)", 16),
      ("col_smallint_zstd", "(0,1)", 21),
      ("col_int_raw", "(1,2,3)", 16),
      ("col_int_bytedict", "(1,2,3)", 18),
      ("col_int_delta", "(1,2,3)", 19),
      ("col_int_delta32k", "(1,2,3)", 21),
      ("col_int_lzo", "(1,2,3)", 13),
      ("col_int_mostly8", "(1,2,3)", 14),
      ("col_int_mostly16", "(1,2,3)", 9),
      ("col_int_runlength", "(1,2,3)", 12),
      ("col_int_zstd", "(1,2,3)", 19),
      ("col_bigint_raw", "(1,2,3)", 0),
      ("col_bigint_bytedict", "(1,2,3)", 0),
      ("col_bigint_delta", "(1,2,3)", 0),
      ("col_bigint_delta32k", "(1,2,3)", 1),
      ("col_bigint_lzo", "(1,2,3)", 0),
      ("col_bigint_mostly8", "(1,2,3)", 1),
      ("col_bigint_mostly16", "(1,2,3)", 0),
      ("col_bigint_mostly32", "(1,2,3)", 0),
      ("col_bigint_runlength", "(1,2,3)", 0),
      ("col_bigint_zstd", "(1,2,3)", 1),
      ("col_decimal_1_0_raw", "(-1,0,1)", 1033),
      ("col_decimal_18_0_raw", "(-1,0,1)", 0),
      ("col_decimal_38_0_raw", "(-1,0,1)", 0),
      ("col_decimal_1_0_bytedict", "(-1,0,1)", 1007),
      ("col_decimal_18_0_bytedict", "(-1,0,1)", 0),
      ("col_decimal_38_0_bytedict", "(-1,0,1)", 0),
      ("col_decimal_1_0_delta", "(-1,0,1)", 1064),
      ("col_decimal_18_0_delta", "(-1,0,1)", 0),
      ("col_decimal_38_0_delta", "(-1,0,1)", 0),
      ("col_decimal_1_0_delta32k", "(-1,0,1)", 993),
      ("col_decimal_18_0_delta32k", "(-1,0,1)", 0),
      ("col_decimal_38_0_delta32k", "(-1,0,1)", 0),
      ("col_decimal_1_0_lzo", "(-1,0,1)", 992),
      ("col_decimal_18_0_lzo", "(-1,0,1)", 0),
      ("col_decimal_38_0_lzo", "(-1,0,1)", 0),
      ("col_decimal_1_0_mostly8", "(-1,0,1)", 1030),
      ("col_decimal_18_0_mostly8", "(-1,0,1)", 0),
      ("col_decimal_38_0_mostly8", "(-1,0,1)", 0),
      ("col_decimal_1_0_mostly16", "(-1,0,1)", 962),
      ("col_decimal_18_0_mostly16", "(-1,0,1)", 0),
      ("col_decimal_38_0_mostly16", "(-1,0,1)", 0),
      ("col_decimal_1_0_mostly32", "(-1,0,1)", 997),
      ("col_decimal_18_0_mostly32", "(-1,0,1)", 1),
      ("col_decimal_38_0_mostly32", "(-1,0,1)", 0),
      ("col_decimal_1_0_runlength", "(-1,0,1)", 1016),
      ("col_decimal_18_0_runlength", "(-1,0,1)", 0),
      ("col_decimal_38_0_runlength", "(-1,0,1)", 0),
      ("col_decimal_1_0_zstd", "(-1,0,1)", 1070),
      ("col_decimal_18_0_zstd", "(-1,0,1)", 0),
      ("col_decimal_38_0_zstd", "(-1,0,1)", 0),
      ("col_boolean_raw", (true, false), 5000),
      ("col_boolean_runlength", (true, false), 5000),
      ("col_boolean_zstd", (true, false), 5000)
    )
    input.foreach( test_case => {
      var column_name = test_case._1.toUpperCase
      var expected_res = test_case._2
      var result_size = test_case._3
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name in $expected_res"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SUBQUERY_2_COL_0"
           |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
           |AS "SUBQUERY_0" WHERE "SUBQUERY_0"."$column_name" IN $expected_res )
           |AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }

  test("child in pushdown - decimal 18 scale") {
    // "Column name" and result size
    val input = List(
      ("col_decimal_18_18_raw", "(-1,0,1)", 0),
      ("col_decimal_18_18_bytedict", "(-1,0,1)", 0),
      ("col_decimal_18_18_delta", "(-1,0,1)", 0),
      ("col_decimal_18_18_delta32k", "(-1,0,1)", 0),
      ("col_decimal_18_18_lzo", "(-1,0,1)", 0),
      ("col_decimal_18_18_mostly8", "(-1,0,1)", 0),
      ("col_decimal_18_18_mostly16", "(-1,0,1)", 0),
      ("col_decimal_18_18_mostly32", "(-1,0,1)", 0),
      ("col_decimal_18_18_runlength", "(-1,0,1)", 1),
      ("col_decimal_18_18_zstd", "(-1,0,1)", 0),
    )
    input.foreach( test_case => {
      var column_name = test_case._1.toUpperCase
      var expected_res = test_case._2
      var result_size = test_case._3
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name in $expected_res"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM
           |( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
           |WHERE "SUBQUERY_0"."$column_name" IN ( -1.000000000000000000 , 0E-18 , 1.000000000000000000 ) )
           |AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }

  test("child in pushdown - decimal 37 scale") {
    // "Column name" and result size
    val input = List(
      ("col_decimal_38_37_raw", "(0.7664120400000000000000000000000000000,0.4022369500000000000000000000000000000)", 2),
      ("col_decimal_38_37_bytedict", "(0.7664120400000000000000000000000000000,0.4022369500000000000000000000000000000)", 0),
      ("col_decimal_38_37_delta", "(0.7664120400000000000000000000000000000,0.4022369500000000000000000000000000000)", 0),
      ("col_decimal_38_37_delta32k", "(0.7664120400000000000000000000000000000,0.4022369500000000000000000000000000000)", 0),
      ("col_decimal_38_37_lzo", "(0.7664120400000000000000000000000000000,0.4022369500000000000000000000000000000)", 0),
      ("col_decimal_38_37_mostly8", "(0.7664120400000000000000000000000000000,0.4022369500000000000000000000000000000)", 0),
      ("col_decimal_38_37_mostly16", "(0.7664120400000000000000000000000000000,0.4022369500000000000000000000000000000)", 0),
      ("col_decimal_38_37_mostly32", "(0.7664120400000000000000000000000000000,0.4022369500000000000000000000000000000)", 0),
      ("col_decimal_38_37_runlength", "(0.7664120400000000000000000000000000000,0.4022369500000000000000000000000000000)", 0),
      ("col_decimal_38_37_zstd", "(0.7664120400000000000000000000000000000,0.4022369500000000000000000000000000000)", 0)
    )
    input.foreach( test_case => {
      var column_name = test_case._1.toUpperCase
      var expected_res = test_case._2
      var result_size = test_case._3
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name in $expected_res"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM
           |( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
           |WHERE "SUBQUERY_0"."$column_name" IN $expected_res )
           |AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }

  test("child in pushdown - string + varchar types") {
    // "Column name" and result size
    val input = List(
      ("col_char_1_raw", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_char_255_raw", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_char_2000_raw", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_char_max_raw", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_char_1_bytedict", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_char_255_bytedict", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_char_2000_bytedict", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_char_max_bytedict", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_char_1_lzo", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_char_255_lzo", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_char_2000_lzo", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_char_max_lzo", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_char_1_runlength", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_char_255_runlength", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_char_2000_runlength", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_char_max_runlength", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_char_1_zstd", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_char_255_zstd", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_char_2000_zstd", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_char_max_zstd", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_1_raw", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_255_raw", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_2000_raw", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_max_raw", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_1_bytedict", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_255_bytedict", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_2000_bytedict", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_max_bytedict", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_1_lzo", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_255_lzo", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_2000_lzo", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_max_lzo", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_1_runlength", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_255_runlength", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_2000_runlength", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_max_runlength", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_1_text255", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_255_text255", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_1_text32k", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_255_text32k", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_2000_text32k", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_max_text32k", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_1_zstd", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_255_zstd", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_2000_zstd", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0),
      ("col_varchar_max_zstd", s"('$string255Char', '$string2000Char')", s"(\\'$string255Char\\', \\'$string2000Char\\')", 0)
    )
    input.foreach( test_case => {
      var column_name = test_case._1.toUpperCase
      var query_in = test_case._2
      var expected_res = test_case._3
      var result_size = test_case._4
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name in $query_in"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SUBQUERY_2_COL_0"
           |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
           |AS "SUBQUERY_0" WHERE "SUBQUERY_0"."$column_name" IN $expected_res )
           |AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }

  test("child in pushdown - date and timestamp type") {
    // "Column name" and result size
    val input = List(
      ("col_date_raw", "('2010-05-11', '2010-05-12')", "(\\'2010-05-11\\', \\'2010-05-12\\')", 2),
      ("col_date_bytedict", "('2010-05-11', '2010-05-12')", "(\\'2010-05-11\\', \\'2010-05-12\\')", 0),
      ("col_date_delta", "('2010-05-11', '2010-05-12')", "(\\'2010-05-11\\', \\'2010-05-12\\')", 0),
      ("col_date_delta32k", "('2010-05-11', '2010-05-12')", "(\\'2010-05-11\\', \\'2010-05-12\\')", 0),
      ("col_date_lzo", "('2010-05-11', '2010-05-12')", "(\\'2010-05-11\\', \\'2010-05-12\\')", 0),
      ("col_date_runlength", "('2010-05-11', '2010-05-12')", "(\\'2010-05-11\\', \\'2010-05-12\\')", 3),
      ("col_date_zstd", "('2010-05-11', '2010-05-12')", "(\\'2010-05-11\\', \\'2010-05-12\\')", 0),
      ("col_timestamp_raw", "('1994-05-19 01:03:01', '1994-05-19 01:03:02')", "(\\'1994-05-19 01:03:01\\', \\'1994-05-19 01:03:02\\')", 1),
      ("col_timestamp_bytedict", "('1994-05-19 01:03:01', '1994-05-19 01:03:02')", "(\\'1994-05-19 01:03:01\\', \\'1994-05-19 01:03:02\\')", 0),
      ("col_timestamp_delta", "('1994-05-19 01:03:01', '1994-05-19 01:03:02')", "(\\'1994-05-19 01:03:01\\', \\'1994-05-19 01:03:02\\')", 0),
      ("col_timestamp_delta32k", "('1994-05-19 01:03:01', '1994-05-19 01:03:02')", "(\\'1994-05-19 01:03:01\\', \\'1994-05-19 01:03:02\\')", 0),
      ("col_timestamp_lzo", "('1994-05-19 01:03:01', '1994-05-19 01:03:02')", "(\\'1994-05-19 01:03:01\\', \\'1994-05-19 01:03:02\\')", 0),
      ("col_timestamp_runlength", "('1994-05-19 01:03:01', '1994-05-19 01:03:02')", "(\\'1994-05-19 01:03:01\\', \\'1994-05-19 01:03:02\\')", 0),
      ("col_timestamp_zstd", "('1994-05-19 01:03:01', '1994-05-19 01:03:02')", "(\\'1994-05-19 01:03:01\\', \\'1994-05-19 01:03:02\\')", 0),
      ("col_timestamptz_raw", "('1994-05-19 01:03:01', '1994-05-19 01:03:02')", "(\\'1994-05-19 01:03:01\\', \\'1994-05-19 01:03:02\\')", 0),
      ("col_timestamptz_bytedict", "('1994-05-19 01:03:01', '1994-05-19 01:03:02')", "(\\'1994-05-19 01:03:01\\', \\'1994-05-19 01:03:02\\')", 0),
      ("col_timestamptz_delta", "('1994-05-19 01:03:01', '1994-05-19 01:03:02')", "(\\'1994-05-19 01:03:01\\', \\'1994-05-19 01:03:02\\')", 0),
      ("col_timestamptz_delta32k", "('1994-05-19 01:03:01', '1994-05-19 01:03:02')", "(\\'1994-05-19 01:03:01\\', \\'1994-05-19 01:03:02\\')", 0),
      ("col_timestamptz_lzo", "('1994-05-19 01:03:01', '1994-05-19 01:03:02')", "(\\'1994-05-19 01:03:01\\', \\'1994-05-19 01:03:02\\')", 0),
      ("col_timestamptz_runlength", "('1994-05-19 01:03:01', '1994-05-19 01:03:02')", "(\\'1994-05-19 01:03:01\\', \\'1994-05-19 01:03:02\\')", 0),
      ("col_timestamptz_zstd", "('1994-05-19 01:03:01', '1994-05-19 01:03:02')", "(\\'1994-05-19 01:03:01\\', \\'1994-05-19 01:03:02\\')", 0)
    )
    input.foreach( test_case => {
      var column_name = test_case._1.toUpperCase
      var query_in = test_case._2
      var expected_res = test_case._3
      var result_size = test_case._4
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name in $query_in"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SUBQUERY_2_COL_0" FROM (
           |SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
           |AS "SUBQUERY_0" WHERE CAST ( "SUBQUERY_0"."$column_name" AS VARCHAR )
           |IN $expected_res ) AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }

  // SIM: Redshift-6989
  // float is not well supported.
  ignore("child in float pushdown") {
    // "Column name" and result size
    val input = List(
      ("col_float4_raw", "(0.5, -1.5)", 2),
      ("col_float4_bytedict", "(1.0, -2.0)", 0),
      ("col_float4_runlength", "(-6.5868966897085, -1.3480701575018)", 0),
      ("col_float4_zstd", "(-6.5868966897085, -1.3480701575018)", 0),
      ("col_float8_raw", "(-6.5868966897085, -1.3480701575018)", 0),
      ("col_float8_bytedict", "(-6.5868966897085, -1.3480701575018)", 0),
      ("col_float8_runlength", "(-6.5868966897085, -1.3480701575018)", 0),
      ("col_float8_zstd", "(-6.5868966897085, -1.3480701575018)", 0)
    )
    input.foreach( test_case => {
      var column_name = test_case._1.toUpperCase
      var expected_res = test_case._2
      var result_size = test_case._3
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name in $expected_res"""),
        Seq(Row(result_size)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SUBQUERY_2_COL_0"
           |FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
           |AS "SUBQUERY_0" WHERE "SUBQUERY_0"."$column_name" IN $expected_res )
           |AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }



}
@DoNotDiscover
class DefaultInOperatorBooleanCorrectnessSuite extends BooleanInOperatorCorrectnessSuite {
  override protected val s3format: String = "DEFAULT"
}

@DoNotDiscover
class ParquetInOperatorBooleanCorrectnessSuite extends BooleanInOperatorCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
}

@DoNotDiscover
class DefaultNoPushdownBooleanInOperatorCorrectnessSuite
  extends BooleanInOperatorCorrectnessSuite {
  override protected val auto_pushdown: String = "false"
  override protected val s3format: String = "DEFAULT"
}

@DoNotDiscover
class ParquetNoPushdownBooleanInOperatorCorrectnessSuite
  extends BooleanInOperatorCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}