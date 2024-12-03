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

trait AggregateVarSampCorrectnessSuite extends IntegrationPushdownSuiteBase {
  test("var_samp operator - ALL values", PreloadTest) {
    // int, small int, big int, decimal, float4 types.
    // places to round to, column name, expected var_samp result.
    val inputList = List(
      (1E-6, "col_smallint_raw", 13493.23201216251),
      (1E-6, "col_smallint_bytedict", 13456.334480336061),
      (1E-6, "col_smallint_delta", 13341.529888017632),
      (1E-6, "col_smallint_lzo", 13373.781246489287),
      (1E-6, "col_smallint_mostly8", 13366.212727185404),
      (1E-6, "col_smallint_runlength", 13300.948164632933),
      (1E-6, "col_smallint_zstd", 13480.132344508887),
      (1E-6, "col_int_raw", 82905.89606877387),
      (1E-6, "col_int_bytedict", 85524.83057407518),
      (1E-6, "col_int_delta", 85150.19348825752),
      (1E-6, "col_int_delta32k", 85457.52412306465),
      (1E-6, "col_int_lzo", 83274.11188153636),
      (1E-6, "col_int_mostly8", 84256.38850186045),
      (1E-6, "col_int_mostly16", 84708.11154726952),
      (1E-6, "col_int_runlength", 83173.8359657528),
      (1E-6, "col_int_zstd", 84954.98964076828),
      (1E-6, "col_bigint_raw", 32663044.783589743),
      (1E-6, "col_bigint_bytedict", 32792681.800584793),
      (1E-6, "col_bigint_delta", 33382824.657302767),
      (1E-6, "col_bigint_delta32k", 32731986.183265302),
      (1E-6, "col_bigint_lzo", 33394458.225906998),
      (1E-6, "col_bigint_mostly8", 33435554.65281722),
      (1E-6, "col_bigint_mostly16", 32685328.692294497),
      (1E-6, "col_bigint_mostly32", 33600848.38678931),
      (1E-6, "col_bigint_runlength", 33034859.029356446),
      (1E-6, "col_bigint_zstd", 32911190.389810756),
      (1E-6, "col_decimal_18_0_raw", 3302939012.80725),
      (1E-6, "col_decimal_18_18_raw", 0.06853859856908336),
      (1E-0, "col_decimal_38_0_raw", 3270291179932453.175296),
      (1E-6, "col_decimal_38_37_raw", 0.06806422031876119),
      (1E-6, "col_decimal_18_0_bytedict", 3320797704.5647287),
      (1E-6, "col_decimal_18_18_bytedict", 0.06721178405966619),
      (1E-0, "col_decimal_38_0_bytedict", 3271555236354402.5),
      (1E-6, "col_decimal_38_37_bytedict", 0.06599476244067337),
      (1E-6, "col_decimal_18_0_delta", 3320532343.3650565),
      (1E-6, "col_decimal_18_18_delta", 0.06859519884453494),
      (1E-0, "col_decimal_38_0_delta", 3345237083507349D),
      (1E-6, "col_decimal_38_37_delta", 0.067445062290748),
      (1E-5, "col_decimal_18_0_delta32k", 3292634962.1117654),
      (1E-6, "col_decimal_18_18_delta32k", 0.06851288031152539),
      (1E-0, "col_decimal_38_0_delta32k", 3396134319974168.657920),
      (1E-6, "col_decimal_38_37_delta32k", 0.06704362526745133),
      (1E-5, "col_decimal_18_0_lzo", 3364404621.3899527),
      (1E-6, "col_decimal_18_18_lzo", 0.06626071407023519),
      (1E-0, "col_decimal_38_0_lzo", 3282233317772087D),
      (1E-6, "col_decimal_38_37_lzo", 0.06608173616459714),
      (1E-6, "col_decimal_18_0_mostly8", 3349324401.470721),
      (1E-6, "col_decimal_18_18_mostly8", 0.0671210819487035),
      (1E-0, "col_decimal_38_0_mostly8", 3314375753371360.428032),
      (1E-6, "col_decimal_38_37_mostly8", 0.06756683412157471),
      (1E-6, "col_decimal_18_0_mostly16", 3292795187.559212),
      (1E-6, "col_decimal_18_18_mostly16", 0.06780105168857439),
      (1E-0, "col_decimal_38_0_mostly16", 3319327475997583.343616),
      (1E-6, "col_decimal_38_37_mostly16", 0.06764222455649033),
      (1E-6, "col_decimal_18_0_mostly32", 3330399318.6259255),
      (1E-6, "col_decimal_18_18_mostly32", 0.06751977359611624),
      (1E-0, "col_decimal_38_0_mostly32", 3336024912531483.525120),
      (1E-6, "col_decimal_38_37_mostly32", 0.06665723827027543),
      (1E-6, "col_decimal_18_0_runlength", 3320691341.656961),
      (1E-6, "col_decimal_18_18_runlength", 0.0676515761116973),
      (1E-0, "col_decimal_38_0_runlength", 3321074575860790.5),
      (1E-6, "col_decimal_38_37_runlength", 0.06807996641446348),
      (1E-6, "col_decimal_18_0_zstd", 3266357618.068803),
      (1E-6, "col_decimal_18_18_zstd", 0.06686767476912223),
      (1E-0, "col_decimal_38_0_zstd", 3304039482387007.537152),
      (1E-6, "col_decimal_38_37_zstd", 0.06668480569282186),
      (1E-6, "col_float4_raw", 521.7137285425684),
      (1E-6, "col_float4_bytedict", 537.9125584250869),
      (1E-6, "col_float4_runlength", 530.888442846357),
      (1E-6, "col_float4_zstd", 522.1816427843112),
      (1E-6, "col_decimal_1_0_raw", 28.024333026605298),
      (1E-6, "col_decimal_1_0_bytedict", 28.36403884776961),
      (1E-6, "col_decimal_1_0_delta", 28.02124248849777),
      (1E-6, "col_decimal_1_0_delta32k", 28.154918943788775),
      (1E-6, "col_decimal_1_0_lzo", 28.558187637527453),
      (1E-6, "col_decimal_1_0_mostly8", 28.600464252850614),
      (1E-6, "col_decimal_1_0_mostly16", 29.00699995999189),
      (1E-6, "col_decimal_1_0_mostly32", 28.266779715943155),
      (1E-6, "col_decimal_1_0_runlength", 28.49316963392677),
      (1E-6, "col_decimal_1_0_zstd", 28.197555951190292)
    )

    inputList.par.foreach(test_case => {
      val epsilon = test_case._1
      val column_name = test_case._2.toUpperCase
      val expected_res = test_case._3

      val lower = expected_res - epsilon
      val upper = expected_res + epsilon

      checkAnswer(
        sqlContext.sql(
          s"""SELECT (VAR_SAMP ($column_name)) BETWEEN ${lower} and ${upper}
             |FROM test_table""".stripMargin),
        Seq(Row(true)))

      checkSqlStatement(
        s""" SELECT ( ( ( VAR_SAMP ( CAST ( "SQ_1"."SQ_1_COL_0" AS FLOAT8 ) )
           | >= ${lower} )
           | AND ( VAR_SAMP ( CAST ( "SQ_1"."SQ_1_COL_0" AS FLOAT8 ) ) <= ${upper} ) ) )
           | AS "SQ_2_COL_0"
           | FROM ( SELECT ( "SQ_0"."$column_name" ) AS "SQ_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
           | AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("var_samp operator - DISTINCT values", PreloadTest) {
    // int, small int, big int, decimal, float4 types.
    // column name, expected variance.
    // VAR_SAMP and VARIANCE are synonyms for the same function in Spark and Redshift.
    val inputList = List(
      (1E-6, "col_smallint_raw", 13433.500000000005),
      (1E-6, "col_smallint_bytedict", 13433.500000000005),
      (1E-6, "col_smallint_delta", 13433.499999999998),
      (1E-6, "col_smallint_lzo", 13433.500000000005),
      (1E-6, "col_smallint_mostly8", 13433.5),
      (1E-6, "col_smallint_runlength", 13433.499999999998),
      (1E-6, "col_smallint_zstd", 13433.5),
      (1E-6, "col_int_raw", 83729.1772037043),
      (1E-6, "col_int_bytedict", 83955.92611790601),
      (1E-6, "col_int_delta", 83712.06255170514),
      (1E-6, "col_int_delta32k", 83585.35962846363),
      (1E-6, "col_int_lzo", 83951.1664184503),
      (1E-6, "col_int_mostly8", 83591.71549032361),
      (1E-6, "col_int_mostly16", 83604.19781941832),
      (1E-6, "col_int_runlength", 83170.27297186785),
      (1E-6, "col_int_zstd", 83570.44567303311),
      (1E-6, "col_bigint_raw", 32842982.720796987),
      (1E-6, "col_bigint_bytedict", 32982070.039108045),
      (1E-6, "col_bigint_delta", 33252684.4063468),
      (1E-6, "col_bigint_delta32k", 32832548.13754982),
      (1E-6, "col_bigint_lzo", 33433063.308454785),
      (1E-6, "col_bigint_mostly8", 33398049.72449028),
      (1E-6, "col_bigint_mostly16", 32762336.606216386),
      (1E-6, "col_bigint_mostly32", 33660519.353677064),
      (1E-6, "col_bigint_runlength", 33146489.03868842),
      (1E-6, "col_bigint_zstd", 32822710.19381514),
      (1E-0, "col_decimal_18_0_raw", 3305254185.541879),
      (1E-6, "col_decimal_18_18_raw", 0.06886333692605201),
      (1E1, "col_decimal_38_0_raw", 3270291179932459D),
      (1E-6, "col_decimal_38_37_raw", 0.06806422031876119),
      (1E-0, "col_decimal_18_0_bytedict", 3322956856.7690134),
      (1E-6, "col_decimal_18_18_bytedict", 0.06727135169218722),
      (1E2, "col_decimal_38_0_bytedict", 3271555236354421D),
      (1E-6, "col_decimal_38_37_bytedict", 0.0659947624406734),
      (1E-0, "col_decimal_18_0_delta", 3317052034.3264575),
      (1E-6, "col_decimal_18_18_delta", 0.06861380384110588),
      (1E1, "col_decimal_38_0_delta", 3345237083507351D),
      (1E-6, "col_decimal_38_37_delta", 0.06745243442321834),
      (1E-0, "col_decimal_18_0_delta32k", 3295974321.2707934),
      (1E-6, "col_decimal_18_18_delta32k", 0.06855600276890168),
      (1E2, "col_decimal_38_0_delta32k", 3396134319974163D),
      (1E-6, "col_decimal_38_37_delta32k", 0.06704362526745156),
      (1E-0, "col_decimal_18_0_lzo", 3365182959.9704695),
      (1E-6, "col_decimal_18_18_lzo", 0.06647529164638379),
      (1E2, "col_decimal_38_0_lzo", 3282233317772083D),
      (1E-6, "col_decimal_38_37_lzo", 0.06608173616459717),
      (1E-0, "col_decimal_18_0_mostly8", 3341848876.9086504),
      (1E-6, "col_decimal_18_18_mostly8", 0.06721870714173672),
      (1E1, "col_decimal_38_0_mostly8", 3314375753371364.5),
      (1E-6, "col_decimal_38_37_mostly8", 0.06756683412157453),
      (1E-0, "col_decimal_18_0_mostly16", 3294462381.9101477),
      (1E-6, "col_decimal_18_18_mostly16", 0.06799330629643627),
      (1E1, "col_decimal_38_0_mostly16", 3319327475997594D),
      (1E-6, "col_decimal_38_37_mostly16", 0.06764222455649041),
      (1E-0, "col_decimal_18_0_mostly32", 3327824637.3583107),
      (1E-6, "col_decimal_18_18_mostly32", 0.06742479865572341),
      (1E1, "col_decimal_38_0_mostly32", 3336024912531482.5),
      (1E-6, "col_decimal_38_37_mostly32", 0.06665723827027543),
      (1E-0, "col_decimal_18_0_runlength", 3316553107.2099977),
      (1E-6, "col_decimal_18_18_runlength", 0.06755889716526844),
      (1E1, "col_decimal_38_0_runlength", 3321074575860781.5),
      (1E-6, "col_decimal_38_37_runlength", 0.06807996641446348),
      (1E-0, "col_decimal_18_0_zstd", 3271266945.41261),
      (1E-6, "col_decimal_18_18_zstd", 0.06703282372899359),
      (1E1, "col_decimal_38_0_zstd", 3304039482387008D),
      (1E-6, "col_decimal_38_37_zstd", 0.06668480569282165),
      (1E-6, "col_float4_raw", 521.7677673600476),
      (1E-6, "col_float4_bytedict", 537.9125584250856),
      (1E-6, "col_float4_runlength", 530.8884428463583),
      (1E-6, "col_float4_zstd", 522.0612461547563),
      (1E-6, "col_decimal_1_0_raw", 31.666666666666668),
      (1E-6, "col_decimal_1_0_bytedict", 31.666666666666668),
      (1E-6, "col_decimal_1_0_delta", 31.66666666666666),
      (1E-6, "col_decimal_1_0_delta32k", 31.66666666666666),
      (1E-6, "col_decimal_1_0_lzo", 31.66666666666666),
      (1E-6, "col_decimal_1_0_mostly8", 31.66666666666666),
      (1E-6, "col_decimal_1_0_mostly16", 31.66666666666666),
      (1E-6, "col_decimal_1_0_mostly32", 31.66666666666666),
      (1E-6, "col_decimal_1_0_runlength", 31.66666666666666),
      (1E-6, "col_decimal_1_0_zstd", 31.66666666666666)
    )

    inputList.par.foreach(test_case => {
      val epsilon = test_case._1
      val column_name = test_case._2.toUpperCase
      val expected_res = test_case._3

      val lower = expected_res - epsilon
      val upper = expected_res + epsilon

      checkAnswer(
        sqlContext.sql(
          s"""SELECT VARIANCE (DISTINCT $column_name) BETWEEN ${lower} and ${upper}
             | FROM test_table""".stripMargin),
        Seq(Row(true)))

      checkSqlStatement(
        s"""SELECT ( ( ( VARIANCE ( DISTINCT CAST ( "SQ_1"."SQ_1_COL_0" AS FLOAT8 ) )
           | >= ${lower} ) AND
           | ( VARIANCE ( DISTINCT CAST ( "SQ_1"."SQ_1_COL_0" AS FLOAT8 ) ) <=
           | ${upper} ) ) ) AS "SQ_2_COL_0"
           | FROM ( SELECT ( "SQ_0"."$column_name" ) AS "SQ_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
           | AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("var_samp operator - float8 type for ALL values", PreloadTest) {

    // column name, expected var_samp result.
    val inputList = List(
      ("col_float8_raw", 1221.177603672273),
      ("col_float8_bytedict", 1204.4578596418694),
      ("col_float8_runlength", 1196.5140340964945),
      ("col_float8_zstd", 1199.7061692927862)
    )

    inputList.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT VAR_SAMP ($column_name) FROM test_table"""),
        Seq(Row(expected_res)))

      checkSqlStatement(
        s"""SELECT ( VAR_SAMP ("SQ_1"."SQ_1_COL_0") )
           | AS "SQ_2_COL_0"
           | FROM ( SELECT ( "SQ_0"."$column_name" ) AS "SQ_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
           | AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("var_samp operator - float8 type for DISTINCT values", PreloadTest) {
    val epsilon = 1E-6

    // column name, expected var_samp result.
    // VAR_SAMP and VARIANCE are synonyms for the same function in Spark and Redshift.
    val inputList = List(
      ("col_float8_raw", 1221.1776036722774),
      ("col_float8_bytedict", 1204.457859641869),
      ("col_float8_runlength", 1196.5140340964929),
      ("col_float8_zstd", 1199.7061692927814)
    )

    inputList.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      val lower = expected_res - epsilon
      val upper = expected_res + epsilon

      checkAnswer(
        sqlContext.sql(
          s"""SELECT VARIANCE (DISTINCT $column_name) BETWEEN ${lower} AND ${upper}
             | FROM test_table""".stripMargin),
        Seq(Row(true)))

      checkSqlStatement(
        s"""SELECT ( ( ( VARIANCE ( DISTINCT "SQ_1"."SQ_1_COL_0" )
           | >= ${lower} ) AND ( VARIANCE ( DISTINCT "SQ_1"."SQ_1_COL_0" )
           | <= ${upper} ) ) ) AS "SQ_2_COL_0"
           | FROM ( SELECT ( "SQ_0"."$column_name" ) AS "SQ_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
           | AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }
}
