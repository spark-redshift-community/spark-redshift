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

abstract class BooleanNullOperatorCorrectnessSuite extends IntegrationPushdownSuiteBase {

  override protected val preloaded_data: String = "true"
  override def setTestTableName(): String = """"PUBLIC"."all_shapes_dist_all_sort_compound_12col""""

  val allColumnNames = List(
    "col_smallint_raw",
    "col_smallint_bytedict",
    "col_smallint_delta",
    "col_smallint_lzo",
    "col_smallint_mostly8",
    "col_smallint_runlength",
    "col_smallint_zstd",
    "col_int_raw",
    "col_int_bytedict",
    "col_int_delta",
    "col_int_delta32k",
    "col_int_lzo",
    "col_int_mostly8",
    "col_int_mostly16",
    "col_int_runlength",
    "col_int_zstd",
    "col_bigint_raw",
    "col_bigint_bytedict",
    "col_bigint_delta",
    "col_bigint_delta32k",
    "col_bigint_lzo",
    "col_bigint_mostly8",
    "col_bigint_mostly16",
    "col_bigint_mostly32",
    "col_bigint_runlength",
    "col_bigint_zstd",
    "col_decimal_1_0_raw",
    "col_decimal_18_0_raw",
    "col_decimal_18_18_raw",
    "col_decimal_38_0_raw",
    "col_decimal_38_37_raw",
    "col_decimal_1_0_bytedict",
    "col_decimal_18_0_bytedict",
    "col_decimal_18_18_bytedict",
    "col_decimal_38_0_bytedict",
    "col_decimal_38_37_bytedict",
    "col_decimal_1_0_delta",
    "col_decimal_18_0_delta",
    "col_decimal_18_18_delta",
    "col_decimal_38_0_delta",
    "col_decimal_38_37_delta",
    "col_decimal_1_0_delta32k",
    "col_decimal_18_0_delta32k",
    "col_decimal_18_18_delta32k",
    "col_decimal_38_0_delta32k",
    "col_decimal_38_37_delta32k",
    "col_decimal_1_0_lzo",
    "col_decimal_18_0_lzo",
    "col_decimal_18_18_lzo",
    "col_decimal_38_0_lzo",
    "col_decimal_38_37_lzo",
    "col_decimal_1_0_mostly8",
    "col_decimal_18_0_mostly8",
    "col_decimal_18_18_mostly8",
    "col_decimal_38_0_mostly8",
    "col_decimal_38_37_mostly8",
    "col_decimal_1_0_mostly16",
    "col_decimal_18_0_mostly16",
    "col_decimal_18_18_mostly16",
    "col_decimal_38_0_mostly16",
    "col_decimal_38_37_mostly16",
    "col_decimal_1_0_mostly32",
    "col_decimal_18_0_mostly32",
    "col_decimal_18_18_mostly32",
    "col_decimal_38_0_mostly32",
    "col_decimal_38_37_mostly32",
    "col_decimal_1_0_runlength",
    "col_decimal_18_0_runlength",
    "col_decimal_18_18_runlength",
    "col_decimal_38_0_runlength",
    "col_decimal_38_37_runlength",
    "col_decimal_1_0_zstd",
    "col_decimal_18_0_zstd",
    "col_decimal_18_18_zstd",
    "col_decimal_38_0_zstd",
    "col_decimal_38_37_zstd",
    "col_float4_raw",
    "col_float4_bytedict",
    "col_float4_runlength",
    "col_float4_zstd",
    "col_float8_raw",
    "col_float8_bytedict",
    "col_float8_runlength",
    "col_float8_zstd",
    "col_boolean_raw",
    "col_boolean_runlength",
    "col_boolean_zstd",
    "col_char_1_raw",
    "col_char_255_raw",
    "col_char_2000_raw",
    "col_char_max_raw",
    "col_char_1_bytedict",
    "col_char_255_bytedict",
    "col_char_2000_bytedict",
    "col_char_max_bytedict",
    "col_char_1_lzo",
    "col_char_255_lzo",
    "col_char_2000_lzo",
    "col_char_max_lzo",
    "col_char_1_runlength",
    "col_char_255_runlength",
    "col_char_2000_runlength",
    "col_char_max_runlength",
    "col_char_1_zstd",
    "col_char_255_zstd",
    "col_char_2000_zstd",
    "col_char_max_zstd",
    "col_varchar_1_raw",
    "col_varchar_255_raw",
    "col_varchar_2000_raw",
    "col_varchar_max_raw",
    "col_varchar_1_bytedict",
    "col_varchar_255_bytedict",
    "col_varchar_2000_bytedict",
    "col_varchar_max_bytedict",
    "col_varchar_1_lzo",
    "col_varchar_255_lzo",
    "col_varchar_2000_lzo",
    "col_varchar_max_lzo",
    "col_varchar_1_runlength",
    "col_varchar_255_runlength",
    "col_varchar_2000_runlength",
    "col_varchar_max_runlength",
    "col_varchar_1_text255",
    "col_varchar_255_text255",
    "col_varchar_1_text32k",
    "col_varchar_255_text32k",
    "col_varchar_2000_text32k",
    "col_varchar_max_text32k",
    "col_varchar_1_zstd",
    "col_varchar_255_zstd",
    "col_varchar_2000_zstd",
    "col_varchar_max_zstd",
    "col_date_raw",
    "col_date_bytedict",
    "col_date_delta",
    "col_date_delta32k",
    "col_date_lzo",
    "col_date_runlength",
    "col_date_zstd",
    "col_timestamp_raw",
    "col_timestamp_bytedict",
    "col_timestamp_delta",
    "col_timestamp_delta32k",
    "col_timestamp_lzo",
    "col_timestamp_runlength",
    "col_timestamp_zstd",
    "col_timestamptz_raw",
    "col_timestamptz_bytedict",
    "col_timestamptz_delta",
    "col_timestamptz_delta32k",
    "col_timestamptz_lzo",
    "col_timestamptz_runlength",
    "col_timestamptz_zstd"
  )
  test("child IS NULL pushdown", PreloadTest) {
    // "Column name" and result size
    allColumnNames.par.foreach( c_name => {
      val column_name = c_name.toUpperCase()
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name is NULL"""),
        Seq(Row(0)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SUBQUERY_2_COL_0" FROM
           |( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
           |AS "SUBQUERY_0" WHERE ( "SUBQUERY_0"."$column_name" IS NULL ) )
           |AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }

  test("child IS NOT NULL pushdown", PreloadTest) {
    allColumnNames.par.foreach( c_name => {
      val column_name = c_name.toUpperCase()
      checkAnswer(
        sqlContext.sql(s"""SELECT count(*) FROM test_table where $column_name is NOT NULL"""),
        Seq(Row(5000)))

      checkSqlStatement(
        s"""SELECT ( COUNT ( 1 ) ) AS "SUBQUERY_2_COL_0" FROM
           |( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
           |AS "SUBQUERY_0" WHERE ( "SUBQUERY_0"."$column_name" IS NOT NULL ) )
           |AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }
}

class TextNullOperatorBooleanCorrectnessSuite extends BooleanNullOperatorCorrectnessSuite {
  override protected val s3format: String = "TEXT"
}

class ParquetNullOperatorBooleanCorrectnessSuite extends BooleanNullOperatorCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
}

class TextNoPushdownBooleanNullOperatorCorrectnessSuite
  extends BooleanNullOperatorCorrectnessSuite {
  override protected val auto_pushdown: String = "false"
  override protected val s3format: String = "TEXT"
}

class ParquetNoPushdownBooleanNullOperatorCorrectnessSuite
  extends BooleanNullOperatorCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}

class TextPushdownNoCacheBooleanNullOperatorCorrectnessSuite
  extends TextNullOperatorBooleanCorrectnessSuite {
  override protected val s3_result_cache = "false"
}

class ParquetPushdownNoCacheBooleanNullOperatorCorrectnessSuite
  extends ParquetNullOperatorBooleanCorrectnessSuite {
  override protected val s3_result_cache = "false"
}
