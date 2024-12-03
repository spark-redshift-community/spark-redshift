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

trait AggregateStddevPopCorrectnessSuite extends IntegrationPushdownSuiteBase {
  test("stddev_pop operator - ALL values", PreloadTest) {
    val epsilon = 1E-6

    // int, small int, big int, decimal, float4 types.
    // column name, expected stddev_pop result.
    val inputList = List(
      ("col_smallint_raw", 116.14875533452813),
      ("col_smallint_bytedict", 115.98984099239034),
      ("col_smallint_delta", 115.49398937624429),
      ("col_smallint_lzo", 115.6335007263898),
      ("col_smallint_mostly8", 115.60077631504024),
      ("col_smallint_runlength", 115.3182031380996),
      ("col_smallint_zstd", 116.09236115283377),
      ("col_int_raw", 287.9050449185636),
      ("col_int_bytedict", 292.417040556737),
      ("col_int_delta", 291.77587880008156),
      ("col_int_delta32k", 292.30195452346885),
      ("col_int_lzo", 288.5436831038934),
      ("col_int_mostly8", 290.2404817115629),
      ("col_int_mostly16", 291.0174735732548),
      ("col_int_runlength", 288.36990342017253),
      ("col_int_zstd", 291.4412438946144),
      ("col_bigint_raw", 5714.587664445531),
      ("col_bigint_bytedict", 5725.916805562641),
      ("col_bigint_delta", 5777.209368922967),
      ("col_bigint_delta32k", 5720.615332814176),
      ("col_bigint_lzo", 5778.215930048116),
      ("col_bigint_mostly8", 5781.770277509014),
      ("col_bigint_mostly16", 5716.536681116989),
      ("col_bigint_mostly32", 5796.044186953025),
      ("col_bigint_runlength", 5747.021146433218),
      ("col_bigint_zstd", 5736.253843034912),
      ("col_decimal_18_0_raw", 57465.45418775256),
      ("col_decimal_18_18_raw", 0.26177259377056555),
      ("col_decimal_38_0_raw", 57180740.82850332),
      ("col_decimal_38_37_raw", 0.26086511356388276),
      ("col_decimal_18_0_bytedict", 57620.600005760236),
      ("col_decimal_18_18_bytedict", 0.25922642940652146),
      ("col_decimal_38_0_bytedict", 57191790.71603836),
      ("col_decimal_38_37_bytedict", 0.2568687670546679),
      ("col_decimal_18_0_delta", 57618.29776118333),
      ("col_decimal_18_18_delta", 0.26188065947061845),
      ("col_decimal_38_0_delta", 57832240.45539519),
      ("col_decimal_38_37_delta", 0.259675900457262),
      ("col_decimal_18_0_delta32k", 57375.747795731106),
      ("col_decimal_18_18_delta32k", 0.2617234757056827),
      ("col_decimal_38_0_delta32k", 58270533.66076351),
      ("col_decimal_38_37_delta32k", 0.2589019438752785),
      ("col_decimal_18_0_lzo", 57997.68737170194),
      ("col_decimal_18_18_lzo", 0.2573858230894257),
      ("col_decimal_38_0_lzo", 57285049.28084231),
      ("col_decimal_38_37_lzo", 0.25703797349295343),
      ("col_decimal_18_0_mostly8", 57867.560313101385),
      ("col_decimal_18_18_mostly8", 0.25905145769192994),
      ("col_decimal_38_0_mostly8", 57564858.01442305),
      ("col_decimal_38_37_mostly8", 0.2599102167186784),
      ("col_decimal_18_0_mostly16", 57377.143781489336),
      ("col_decimal_18_18_mostly16", 0.260360310873675),
      ("col_decimal_38_0_mostly16", 57607843.307160735),
      ("col_decimal_38_37_mostly16", 0.26005517897473035),
      ("col_decimal_18_0_mostly32", 57703.84076265808),
      ("col_decimal_18_18_mostly32", 0.25981968678565726),
      ("col_decimal_38_0_mostly32", 57752555.85295751),
      ("col_decimal_38_37_mostly32", 0.2581548117363327),
      ("col_decimal_18_0_runlength", 57619.67722391917),
      ("col_decimal_18_18_runlength", 0.26007315470166265),
      ("col_decimal_38_0_runlength", 57623002.01261315),
      ("col_decimal_38_37_runlength", 0.2608952863146067),
      ("col_decimal_18_0_zstd", 57146.34149746761),
      ("col_decimal_18_18_zstd", 0.2585619872180913),
      ("col_decimal_38_0_zstd", 57475026.528837115),
      ("col_decimal_38_37_zstd", 0.25820818873862866),
      ("col_float4_raw", 22.838769358195723),
      ("col_float4_bytedict", 23.190622585722057),
      ("col_float4_runlength", 23.038712315530738),
      ("col_float4_zstd", 22.849008872503735),
      ("col_decimal_1_0_raw", 5.2932719710968925),
      ("col_decimal_1_0_bytedict", 5.325257368428314),
      ("col_decimal_1_0_delta", 5.292980090648374),
      ("col_decimal_1_0_delta32k", 5.305590255570064),
      ("col_decimal_1_0_lzo", 5.34345169342813),
      ("col_decimal_1_0_mostly8", 5.347405367091599),
      ("col_decimal_1_0_mostly16", 5.385276089486953),
      ("col_decimal_1_0_mostly32", 5.316119483232104),
      ("col_decimal_1_0_runlength", 5.337365548657876),
      ("col_decimal_1_0_zstd", 5.309606053183235)
    )

    inputList.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      val lower = expected_res - epsilon
      val upper = expected_res + epsilon

      checkAnswer(
        sqlContext.sql(
          s"""SELECT (STDDEV_POP ($column_name)) BETWEEN ${lower} and ${upper}
             |FROM test_table""".stripMargin),
        Seq(Row(true)))

      checkSqlStatement(
        s"""SELECT ( ( ( STDDEV_POP ( CAST ( "SQ_1"."SQ_1_COL_0" AS FLOAT8 ) )
           | >= ${lower} ) AND
           | ( STDDEV_POP ( CAST ( "SQ_1"."SQ_1_COL_0" AS FLOAT8 ) ) <= ${upper} ) ) )
           | AS "SQ_2_COL_0"
           | FROM ( SELECT ( "SQ_0"."$column_name" ) AS "SQ_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
           | AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }


  test("stddev_pop operator - DISTINCT values", PreloadTest) {
    val epsilon = 1E-6

    // int, small int, big int, decimal, float4 types.
    // column name, expected stddev_pop result.
    val inputList = List(
      ("col_smallint_raw", 115.75836902790228),
      ("col_smallint_bytedict", 115.75836902790228),
      ("col_smallint_delta", 115.75836902790225),
      ("col_smallint_lzo", 115.75836902790228),
      ("col_smallint_mostly8", 115.75836902790225),
      ("col_smallint_runlength", 115.75836902790225),
      ("col_smallint_zstd", 115.75836902790225),
      ("col_int_raw", 289.21406050052366),
      ("col_int_bytedict", 289.605556503091),
      ("col_int_delta", 289.1842058494274),
      ("col_int_delta32k", 288.9661547390281),
      ("col_int_lzo", 289.5974939541923),
      ("col_int_mostly8", 288.9768488946369),
      ("col_int_mostly16", 288.99871602096835),
      ("col_int_runlength", 288.2471716615588),
      ("col_int_zstd", 288.9402278592785),
      ("col_bigint_raw", 5730.2366629141125),
      ("col_bigint_bytedict", 5742.356224179777),
      ("col_bigint_delta", 5765.863322268026),
      ("col_bigint_delta32k", 5729.323993238715),
      ("col_bigint_lzo", 5781.482665320183),
      ("col_bigint_mostly8", 5778.452124846471),
      ("col_bigint_mostly16", 5723.187368886437),
      ("col_bigint_mostly32", 5801.116426246997),
      ("col_bigint_runlength", 5756.649064331918),
      ("col_bigint_zstd", 5728.4658522134805),
      ("col_decimal_18_0_raw", 57485.51493945966),
      ("col_decimal_18_18_raw", 0.26239123225292704),
      ("col_decimal_38_0_raw", 57180740.82850337),
      ("col_decimal_38_37_raw", 0.26086511356388276),
      ("col_decimal_18_0_bytedict", 57639.253221346924),
      ("col_decimal_18_18_bytedict", 0.25934040185079327),
      ("col_decimal_38_0_bytedict", 57191790.71603852),
      ("col_decimal_38_37_bytedict", 0.256868767054668),
      ("col_decimal_18_0_delta", 57588.02568416898),
      ("col_decimal_18_18_delta", 0.26191547255620806),
      ("col_decimal_38_0_delta", 57832240.4553952),
      ("col_decimal_38_37_delta", 0.25969008690686785),
      ("col_decimal_18_0_delta32k", 57404.763302010084),
      ("col_decimal_18_18_delta32k", 0.26180502924345933),
      ("col_decimal_38_0_delta32k", 58270533.660763465),
      ("col_decimal_38_37_delta32k", 0.25890194387527893),
      ("col_decimal_18_0_lzo", 58004.31334890519),
      ("col_decimal_18_18_lzo", 0.25780147306848644),
      ("col_decimal_38_0_lzo", 57285049.28084228),
      ("col_decimal_38_37_lzo", 0.2570379734929535),
      ("col_decimal_18_0_mostly8", 57802.88826939314),
      ("col_decimal_18_18_mostly8", 0.2592389833175875),
      ("col_decimal_38_0_mostly8", 57564858.01442309),
      ("col_decimal_38_37_mostly8", 0.2599102167186781),
      ("col_decimal_18_0_mostly16", 57391.58827691623),
      ("col_decimal_18_18_mostly16", 0.26072842772685956),
      ("col_decimal_38_0_mostly16", 57607843.307160825),
      ("col_decimal_38_37_mostly16", 0.2600551789747305),
      ("col_decimal_18_0_mostly32", 57681.45902977455),
      ("col_decimal_18_18_mostly32", 0.2596362058945862),
      ("col_decimal_38_0_mostly32", 57752555.85295751),
      ("col_decimal_38_37_mostly32", 0.2581548117363327),
      ("col_decimal_18_0_runlength", 57583.70042486493),
      ("col_decimal_18_18_runlength", 0.2598942896103807),
      ("col_decimal_38_0_runlength", 57623002.012613066),
      ("col_decimal_38_37_runlength", 0.2608952863146067),
      ("col_decimal_18_0_zstd", 57189.20715651766),
      ("col_decimal_18_18_zstd", 0.2588802860942191),
      ("col_decimal_38_0_zstd", 57475026.528837115),
      ("col_decimal_38_37_zstd", 0.2582081887386283),
      ("col_float4_raw", 22.83995168409272),
      ("col_float4_bytedict", 23.190622585722025),
      ("col_float4_runlength", 23.038712315530766),
      ("col_float4_zstd", 22.84637417663683),
      ("col_decimal_1_0_raw", 5.477225575051661),
      ("col_decimal_1_0_bytedict", 5.47722557505166),
      ("col_decimal_1_0_delta", 5.47722557505166),
      ("col_decimal_1_0_delta32k", 5.47722557505166),
      ("col_decimal_1_0_lzo", 5.47722557505166),
      ("col_decimal_1_0_mostly8", 5.47722557505166),
      ("col_decimal_1_0_mostly16", 5.47722557505166),
      ("col_decimal_1_0_mostly32", 5.47722557505166),
      ("col_decimal_1_0_runlength", 5.47722557505166),
      ("col_decimal_1_0_zstd", 5.47722557505166)
    )

    inputList.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      val lower = expected_res - epsilon
      val upper = expected_res + epsilon

      checkAnswer(
        sqlContext.sql(
          s"""SELECT STDDEV_POP (DISTINCT $column_name) BETWEEN ${lower} AND ${upper}
             | FROM test_table""".stripMargin),
        Seq(Row(true)))

      checkSqlStatement(
        s"""SELECT ( ( ( STDDEV_POP ( DISTINCT CAST ( "SQ_1"."SQ_1_COL_0" AS FLOAT8 ) )
           | >= ${lower} )
           | AND ( STDDEV_POP ( DISTINCT CAST ( "SQ_1"."SQ_1_COL_0" AS FLOAT8 ) )
           | <= ${upper} ) ) ) AS "SQ_2_COL_0"
           | FROM ( SELECT ( "SQ_0"."$column_name" ) AS "SQ_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
           | AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("stddev_pop operator - float8 type for ALL values", PreloadTest) {

    // column name, expected stddev_pop result.
    val inputList = List(
      ("col_float8_raw", 34.94185696484288),
      ("col_float8_bytedict", 34.70182946286753),
      ("col_float8_runlength", 34.58720473368259),
      ("col_float8_zstd", 34.6333109600992)
    )

    inputList.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT STDDEV_POP ($column_name) FROM test_table"""),
        Seq(Row(expected_res)))

      checkSqlStatement(
        s"""SELECT ( STDDEV_POP ("SQ_1"."SQ_1_COL_0") )
           | AS "SQ_2_COL_0"
           | FROM ( SELECT ( "SQ_0"."$column_name" ) AS "SQ_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
           | AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }


  test("stddev_pop operator - float8 type for DISTINCT values", PreloadTest) {
    val epsilon = 1E-6

    // column name, expected stddev_pop result.
    val inputList = List(
      ("col_float8_raw", 34.94185696484294),
      ("col_float8_bytedict", 34.70182946286752),
      ("col_float8_runlength", 34.58720473368256),
      ("col_float8_zstd", 34.63331096009913)
    )

    inputList.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      val lower = expected_res - epsilon
      val upper = expected_res + epsilon

      checkAnswer(
        sqlContext.sql(
          s"""SELECT STDDEV_POP (DISTINCT $column_name) BETWEEN ${lower} and ${upper}
             | FROM test_table""".stripMargin),
        Seq(Row(true)))

      checkSqlStatement(
        s"""SELECT ( ( ( STDDEV_POP ( DISTINCT "SQ_1"."SQ_1_COL_0" ) >= ${lower} )
           | AND ( STDDEV_POP ( DISTINCT "SQ_1"."SQ_1_COL_0" ) <= ${upper} ) ) )
           | AS "SQ_2_COL_0"
           | FROM ( SELECT ( "SQ_0"."$column_name" ) AS "SQ_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
           | AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }
}
