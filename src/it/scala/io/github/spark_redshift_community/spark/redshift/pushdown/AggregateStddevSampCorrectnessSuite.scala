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

trait AggregateStddevSampCorrectnessSuite extends IntegrationPushdownSuiteBase {
  test("stddev_samp operator - ALL values", PreloadTest) {
    val epsilon = 1E-6

    // int, small int, big int, decimal, float4 types
    // column name, expected stddev_samp result.
    val inputList = List(
      ("col_smallint_raw", 116.16037195258335),
      ("col_smallint_bytedict", 116.00144171662721),
      ("col_smallint_delta", 115.50554050788054),
      ("col_smallint_lzo", 115.64506581125407),
      ("col_smallint_mostly8", 115.61233812697243),
      ("col_smallint_runlength", 115.32973668847481),
      ("col_smallint_zstd", 116.10397213062474),
      ("col_int_raw", 287.933839742351),
      ("col_int_bytedict", 292.44628664777946),
      ("col_int_delta", 291.8050607653293),
      ("col_int_delta32k", 292.33118910418136),
      ("col_int_lzo", 288.5725418010805),
      ("col_int_mostly8", 290.26951011406703),
      ("col_int_mostly16", 291.04657968660194),
      ("col_int_runlength", 288.3987447367842),
      ("col_int_zstd", 291.47039239135125),
      ("col_bigint_raw", 5715.159208945079),
      ("col_bigint_bytedict", 5726.489483146266),
      ("col_bigint_delta", 5777.787176532445),
      ("col_bigint_delta32k", 5721.187480170992),
      ("col_bigint_lzo", 5778.793838328808),
      ("col_bigint_mostly8", 5782.348541277775),
      ("col_bigint_mostly16", 5717.108420547444),
      ("col_bigint_mostly32", 5796.6238783268755),
      ("col_bigint_runlength", 5747.595934767548),
      ("col_bigint_zstd", 5736.827554477366),
      ("col_decimal_18_0_raw", 57471.20159529684),
      ("col_decimal_18_18_raw", 0.26179877495718606),
      ("col_decimal_38_0_raw", 57186459.76044026),
      ("col_decimal_38_37_raw", 0.2608912039888681),
      ("col_decimal_18_0_bytedict", 57626.36293021388),
      ("col_decimal_18_18_bytedict", 0.2592523559385067),
      ("col_decimal_38_0_bytedict", 57197510.75312983),
      ("col_decimal_38_37_bytedict", 0.25689445778504716),
      ("col_decimal_18_0_delta", 57624.060455377985),
      ("col_decimal_18_18_delta", 0.26190685146543025),
      ("col_decimal_38_0_delta", 57838024.54706894),
      ("col_decimal_38_37_delta", 0.2597018719430955),
      ("col_decimal_18_0_delta32k", 57381.486231290364),
      ("col_decimal_18_18_delta32k", 0.2617496519797598),
      ("col_decimal_38_0_delta32k", 58276361.588333294),
      ("col_decimal_38_37_delta32k", 0.2589278379538425),
      ("col_decimal_18_0_lzo", 58003.48801054944),
      ("col_decimal_18_18_lzo", 0.2574115655331656),
      ("col_decimal_38_0_lzo", 57290778.645189375),
      ("col_decimal_38_37_lzo", 0.257063681146515),
      ("col_decimal_18_0_mostly8", 57873.3479372908),
      ("col_decimal_18_18_mostly8", 0.25907736672411874),
      ("col_decimal_38_0_mostly8", 57570615.3638413),
      ("col_decimal_38_37_mostly8", 0.25993621163965347),
      ("col_decimal_18_0_mostly16", 57382.88235666811),
      ("col_decimal_18_18_mostly16", 0.260386350810818),
      ("col_decimal_38_0_mostly16", 57613604.95575315),
      ("col_decimal_38_37_mostly16", 0.26008118839410577),
      ("col_decimal_18_0_mostly32", 57709.61201243624),
      ("col_decimal_18_18_mostly32", 0.25984567265228076),
      ("col_decimal_38_0_mostly32", 57758331.974975556),
      ("col_decimal_38_37_mostly32", 0.258180631090474),
      ("col_decimal_18_0_runlength", 57625.44005608079),
      ("col_decimal_18_18_runlength", 0.26009916591888044),
      ("col_decimal_38_0_runlength", 57628765.177303515),
      ("col_decimal_38_37_runlength", 0.26092137975731977),
      ("col_decimal_18_0_zstd", 57152.05698895537),
      ("col_decimal_18_18_zstd", 0.2585878472958894),
      ("col_decimal_38_0_zstd", 57480774.89375911),
      ("col_decimal_38_37_zstd", 0.258234013431271),
      ("col_float4_raw", 22.84105357777019),
      ("col_float4_bytedict", 23.192941995897954),
      ("col_float4_runlength", 23.041016532400583),
      ("col_float4_zstd", 22.85129411618325),
      ("col_decimal_1_0_raw", 5.293801377706317),
      ("col_decimal_1_0_bytedict", 5.325789974057333),
      ("col_decimal_1_0_delta", 5.293509468065375),
      ("col_decimal_1_0_delta32k", 5.306120894192741),
      ("col_decimal_1_0_lzo", 5.343986118762609),
      ("col_decimal_1_0_mostly8", 5.34794018785276),
      ("col_decimal_1_0_mostly16", 5.385814697888509),
      ("col_decimal_1_0_mostly32", 5.316651174935512),
      ("col_decimal_1_0_runlength", 5.33789936528657),
      ("col_decimal_1_0_zstd", 5.31013709344592)
    )

    inputList.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      val lower = expected_res - epsilon
      val upper = expected_res + epsilon

      checkAnswer(
        sqlContext.sql(
          s"""SELECT (STDDEV_SAMP ($column_name)) BETWEEN ${lower} AND ${upper}
             |FROM test_table""".stripMargin),
        Seq(Row(true)))

      checkSqlStatement(
        s"""SELECT ( ( ( STDDEV_SAMP ( CAST ( "SQ_1"."SQ_1_COL_0" AS FLOAT8 ) )
           | >= ${lower} ) AND ( STDDEV_SAMP ( CAST ( "SQ_1"."SQ_1_COL_0" AS FLOAT8 ) )
           | <= ${upper} ) ) ) AS "SQ_2_COL_0"
           | FROM ( SELECT ( "SQ_0"."$column_name" ) AS "SQ_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
           | AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("stddev_samp operator - DISTINCT values", PreloadTest) {
    val epsilon = 1E-5

    // int, small int, big int, decimal, float4 types
    // STDDEV_SAMP and STDDEV are synonyms for the same function in Spark and Redshift.
    // column name, expected stddev result.
    val inputList = List(
      ("col_smallint_raw", 115.90297666583031),
      ("col_smallint_bytedict", 115.90297666583031),
      ("col_smallint_delta", 115.90297666583028),
      ("col_smallint_lzo", 115.90297666583031),
      ("col_smallint_mostly8", 115.90297666583028),
      ("col_smallint_runlength", 115.90297666583028),
      ("col_smallint_zstd", 115.90297666583028),
      ("col_int_raw", 289.359944020772),
      ("col_int_bytedict", 289.7514902772823),
      ("col_int_delta", 289.3303692177942),
      ("col_int_delta32k", 289.11132739563084),
      ("col_int_lzo", 289.7432767441728),
      ("col_int_mostly8", 289.1223192531556),
      ("col_int_mostly16", 289.14390503591517),
      ("col_int_runlength", 288.3925674698775),
      ("col_int_zstd", 289.085533489715),
      ("col_bigint_raw", 5730.879751032732),
      ("col_bigint_bytedict", 5743.001831717281),
      ("col_bigint_delta", 5766.5140601880785),
      ("col_bigint_delta32k", 5729.969296388055),
      ("col_bigint_lzo", 5782.13311057907),
      ("col_bigint_mostly8", 5779.10457808909),
      ("col_bigint_mostly16", 5723.839323934276),
      ("col_bigint_mostly32", 5801.768640136994),
      ("col_bigint_runlength", 5757.298762326689),
      ("col_bigint_zstd", 5729.110768157231),
      ("col_decimal_18_0_raw", 57491.34009172059),
      ("col_decimal_18_18_raw", 0.2624182480812872),
      ("col_decimal_38_0_raw", 57186459.76044031),
      ("col_decimal_38_37_raw", 0.2608912039888681),
      ("col_decimal_18_0_bytedict", 57645.09395229583),
      ("col_decimal_18_18_bytedict", 0.2593672139885595),
      ("col_decimal_38_0_bytedict", 57197510.75312999),
      ("col_decimal_38_37_bytedict", 0.2568944577850472),
      ("col_decimal_18_0_delta", 57593.854136760616),
      ("col_decimal_18_18_delta", 0.2619423674037972),
      ("col_decimal_38_0_delta", 57838024.54706895),
      ("col_decimal_38_37_delta", 0.2597160650079589),
      ("col_decimal_18_0_delta32k", 57410.57673696367),
      ("col_decimal_18_18_delta32k", 0.26183201249828425),
      ("col_decimal_38_0_delta32k", 58276361.58833325),
      ("col_decimal_38_37_delta32k", 0.25892783795384294),
      ("col_decimal_18_0_lzo", 58010.19703440482),
      ("col_decimal_18_18_lzo", 0.2578280272708609),
      ("col_decimal_38_0_lzo", 57290778.64518934),
      ("col_decimal_38_37_lzo", 0.25706368114651507),
      ("col_decimal_18_0_mostly8", 57808.72665012308),
      ("col_decimal_18_18_mostly8", 0.259265707608501),
      ("col_decimal_38_0_mostly8", 57570615.36384134),
      ("col_decimal_38_37_mostly8", 0.25993621163965314),
      ("col_decimal_18_0_mostly16", 57397.40744938004),
      ("col_decimal_18_18_mostly16", 0.2607552613015436),
      ("col_decimal_38_0_mostly16", 57613604.95575324),
      ("col_decimal_38_37_mostly16", 0.26008118839410593),
      ("col_decimal_18_0_mostly32", 57687.30048596754),
      ("col_decimal_18_18_mostly32", 0.25966285574899506),
      ("col_decimal_38_0_mostly32", 57758331.97497555),
      ("col_decimal_38_37_mostly32", 0.258180631090474),
      ("col_decimal_18_0_runlength", 57589.52254716128),
      ("col_decimal_18_18_runlength", 0.25992094406813093),
      ("col_decimal_38_0_runlength", 57628765.17730344),
      ("col_decimal_38_37_runlength", 0.26092137975731977),
      ("col_decimal_18_0_zstd", 57194.99056222153),
      ("col_decimal_18_18_zstd", 0.2589069789113333),
      ("col_decimal_38_0_zstd", 57480774.89375912),
      ("col_decimal_38_37_zstd", 0.2582340134312706),
      ("col_float4_raw", 22.842236478945043),
      ("col_float4_bytedict", 23.192941995897925),
      ("col_float4_runlength", 23.04101653240061),
      ("col_float4_zstd", 22.848659613963274),
      ("col_decimal_1_0_raw", 5.627314338711377),
      ("col_decimal_1_0_bytedict", 5.627314338711376),
      ("col_decimal_1_0_delta", 5.627314338711376),
      ("col_decimal_1_0_delta32k", 5.627314338711376),
      ("col_decimal_1_0_lzo", 5.627314338711376),
      ("col_decimal_1_0_mostly8", 5.627314338711376),
      ("col_decimal_1_0_mostly16", 5.627314338711376),
      ("col_decimal_1_0_mostly32", 5.627314338711377),
      ("col_decimal_1_0_runlength", 5.627314338711376),
      ("col_decimal_1_0_zstd", 5.627314338711376)
    )

    inputList.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      val lower = expected_res - epsilon
      val upper = expected_res + epsilon

      checkAnswer(
        sqlContext.sql(
          s"""SELECT (STDDEV (DISTINCT $column_name)) BETWEEN ${lower} and ${upper}
             | FROM test_table""".stripMargin),
        Seq(Row(true)))

      checkSqlStatement(
        s"""SELECT ( ( ( STDDEV ( DISTINCT CAST ( "SQ_1"."SQ_1_COL_0" AS FLOAT8 ) )
           | >= ${lower} ) AND
           | ( STDDEV ( DISTINCT CAST ( "SQ_1"."SQ_1_COL_0" AS FLOAT8 ) )
           |  <= ${upper} ) ) ) AS "SQ_2_COL_0"
           | FROM ( SELECT ( "SQ_0"."$column_name" ) AS "SQ_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
           | AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("stddev_samp operator - float8 type for ALL values", PreloadTest) {

    // STDDEV_SAMP and STDDEV are synonyms for the same function in Spark and Redshift.
    // column name, expected stddev result.
    val inputList = List(
      ("col_float8_raw", 34.94535167475458),
      ("col_float8_bytedict", 34.70530016642803),
      ("col_float8_runlength", 34.59066397305051),
      ("col_float8_zstd", 34.636774810781475)
    )

    inputList.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT STDDEV_SAMP ($column_name) FROM test_table"""),
        Seq(Row(expected_res)))

      checkSqlStatement(
        s"""SELECT ( STDDEV_SAMP ("SQ_1"."SQ_1_COL_0") )
           | AS "SQ_2_COL_0"
           | FROM ( SELECT ( "SQ_0"."$column_name" ) AS "SQ_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
           | AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("stddev_samp operator - float8 type for DISTINCT values", PreloadTest) {
    val epsilon = 1E-6

    // STDDEV_SAMP and STDDEV are synonyms for the same function in Spark and Redshift.
    // column name, expected stddev result.
    val inputList = List(
      ("col_float8_raw", 34.94535167475465),
      ("col_float8_bytedict", 34.705300166428025),
      ("col_float8_runlength", 34.59066397305049),
      ("col_float8_zstd", 34.63677481078141)
    )

    inputList.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      val lower = expected_res - epsilon
      val upper = expected_res + epsilon

      checkAnswer(
        sqlContext.sql(
          s"""SELECT STDDEV (DISTINCT $column_name) BETWEEN ${lower} and ${upper}
             | FROM test_table""".stripMargin),
        Seq(Row(true)))

      checkSqlStatement(
        s"""SELECT ( ( ( STDDEV ( DISTINCT "SQ_1"."SQ_1_COL_0" )
           | >= ${lower} )
           | AND ( STDDEV ( DISTINCT "SQ_1"."SQ_1_COL_0" ) <= ${upper} ) ) )
           | AS "SQ_2_COL_0"
           | FROM ( SELECT ( "SQ_0"."$column_name" ) AS "SQ_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
           | AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }
}
