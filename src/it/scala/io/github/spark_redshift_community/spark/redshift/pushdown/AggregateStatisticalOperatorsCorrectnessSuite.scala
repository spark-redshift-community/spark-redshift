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

abstract class AggregateStatisticalOperatorsCorrectnessSuite
  extends IntegrationPushdownSuiteBase{

  override protected val preloaded_data: String = "true"
  override def setTestTableName(): String = """"PUBLIC"."all_shapes_dist_all_sort_compound_12col""""

}

abstract class PushdownAggregateStatisticalOperatorsCorrectnessSuite
  extends AggregateStatisticalOperatorsCorrectnessSuite {

  // Sample Standard deviation

  test("stddev_samp operator - ALL values", PreloadTest) {

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
      ("col_float4_zstd", 22.85129411618325)
      // Redshift results for these data types are unstable and
      // produce different answers due to round-off errors on each run
      // Issue is tracked in SIM [P89520613] & [Redshift-14972]
      // ("col_decimal_1_0_raw", 5.293801377706317),
      // ("col_decimal_1_0_bytedict", 5.325789974057333),
      // ("col_decimal_1_0_delta", 5.293509468065375),
      // ("col_decimal_1_0_delta32k", 5.306120894192741),
      // ("col_decimal_1_0_lzo", 5.343986118762609),
      // ("col_decimal_1_0_mostly8", 5.34794018785276),
      // ("col_decimal_1_0_mostly16", 5.385814697888509),
      // ("col_decimal_1_0_mostly32", 5.316651174935512),
      // ("col_decimal_1_0_runlength", 5.33789936528657),
      // ("col_decimal_1_0_zstd", 5.31013709344592)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT STDDEV_SAMP ($column_name) FROM test_table"""),
        Seq(Row(expected_res)))

      checkSqlStatement(
        s"""SELECT ( STDDEV_SAMP ( CAST ( "SUBQUERY_1"."SUBQUERY_1_COL_0" AS FLOAT8 ) ) )
           | AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }

  test("stddev_samp operator - DISTINCT values", PreloadTest) {

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
      ("col_float4_zstd", 22.848659613963274)
      // Redshift results for these data types are unstable and
      // produce different answers due to round-off errors on each run
      // Issue is tracked in SIM [P89520613] & [Redshift-14972]
      // ("col_decimal_1_0_raw", 5.627314338711377),
      // ("col_decimal_1_0_bytedict", 5.627314338711376)
      // ("col_decimal_1_0_delta", 5.627314338711376),
      // ("col_decimal_1_0_delta32k", 5.627314338711376),
      // ("col_decimal_1_0_lzo", 5.627314338711376),
      // ("col_decimal_1_0_mostly8", 5.627314338711376),
      // ("col_decimal_1_0_mostly16", 5.627314338711376),
      // ("col_decimal_1_0_mostly32", 5.627314338711377),
      // ("col_decimal_1_0_runlength", 5.627314338711376),
      // ("col_decimal_1_0_zstd", 5.627314338711376)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT STDDEV (DISTINCT $column_name) FROM test_table"""),
        Seq(Row(expected_res)))

      checkSqlStatement(
        s"""SELECT ( STDDEV ( DISTINCT CAST ( "SUBQUERY_1"."SUBQUERY_1_COL_0" AS FLOAT8 ) ) )
           | AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | AS "SUBQUERY_1" LIMIT 1""".stripMargin)
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

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT STDDEV_SAMP ($column_name) FROM test_table"""),
        Seq(Row(expected_res)))

      checkSqlStatement(
        s"""SELECT ( STDDEV_SAMP ("SUBQUERY_1"."SUBQUERY_1_COL_0") )
           | AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }

  test("stddev_samp operator - float8 type for DISTINCT values", PreloadTest) {

    // STDDEV_SAMP and STDDEV are synonyms for the same function in Spark and Redshift.
    // column name, expected stddev result.
    val inputList = List(
      ("col_float8_raw", 34.94535167475465),
      ("col_float8_bytedict", 34.705300166428025),
      ("col_float8_runlength", 34.59066397305049),
      ("col_float8_zstd", 34.63677481078141)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT STDDEV (DISTINCT $column_name) FROM test_table"""),
        Seq(Row(expected_res)))

      checkSqlStatement(
        s"""SELECT ( STDDEV ( DISTINCT "SUBQUERY_1"."SUBQUERY_1_COL_0" ) )
           | AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }

  // Population Standard deviation

  test("stddev_pop operator - ALL values", PreloadTest) {

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
      ("col_float4_zstd", 22.849008872503735)
      // When running queries with these columns results are mismatched in Redshift
      // Issue is tracked in SIM [P89520613] & [Redshift-14972]
      // ("col_decimal_1_0_raw", 5.2932719710968925),
      // ("col_decimal_1_0_bytedict", 5.325257368428314),
      // ("col_decimal_1_0_delta", 5.292980090648374),
      // ("col_decimal_1_0_delta32k", 5.305590255570064),
      // ("col_decimal_1_0_lzo", 5.34345169342813),
      // ("col_decimal_1_0_mostly8", 5.347405367091599),
      // ("col_decimal_1_0_mostly16", 5.385276089486953),
      // ("col_decimal_1_0_mostly32", 5.316119483232104),
      // ("col_decimal_1_0_runlength", 5.337365548657876),
      // ("col_decimal_1_0_zstd", 5.309606053183235)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT STDDEV_POP ($column_name) FROM test_table"""),
        Seq(Row(expected_res)))

      checkSqlStatement(
        s"""SELECT ( STDDEV_POP ( CAST ( "SUBQUERY_1"."SUBQUERY_1_COL_0" AS FLOAT8 ) ) )
           | AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }

  test("stddev_pop operator - DISTINCT values", PreloadTest) {

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
      ("col_float4_zstd", 22.84637417663683)
      // When running queries with these columns results are mismatched in Redshift
      // Issue is tracked in SIM [P89520613] & [Redshift-14972]
      // ("col_decimal_1_0_raw", 5.477225575051661),
      // ("col_decimal_1_0_bytedict", 5.47722557505166),
      // ("col_decimal_1_0_delta", 5.47722557505166),
      // ("col_decimal_1_0_delta32k", 5.47722557505166),
      // ("col_decimal_1_0_lzo", 5.47722557505166),
      // ("col_decimal_1_0_mostly8", 5.47722557505166),
      // ("col_decimal_1_0_mostly16", 5.47722557505166),
      // ("col_decimal_1_0_mostly32", 5.47722557505166),
      // ("col_decimal_1_0_runlength", 5.47722557505166),
      // ("col_decimal_1_0_zstd", 5.47722557505166)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT STDDEV_POP (DISTINCT $column_name) FROM test_table"""),
        Seq(Row(expected_res)))

      checkSqlStatement(
        s"""SELECT ( STDDEV_POP ( DISTINCT CAST ( "SUBQUERY_1"."SUBQUERY_1_COL_0" AS FLOAT8 ) ) )
           | AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | AS "SUBQUERY_1" LIMIT 1""".stripMargin)
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

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT STDDEV_POP ($column_name) FROM test_table"""),
        Seq(Row(expected_res)))

      checkSqlStatement(
        s"""SELECT ( STDDEV_POP ("SUBQUERY_1"."SUBQUERY_1_COL_0") )
           | AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }

  test("stddev_pop operator - float8 type for DISTINCT values", PreloadTest) {

    // column name, expected stddev_pop result.
    val inputList = List(
      ("col_float8_raw", 34.94185696484294),
      ("col_float8_bytedict", 34.70182946286752),
      ("col_float8_runlength", 34.58720473368256),
      ("col_float8_zstd", 34.63331096009913)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT STDDEV_POP (DISTINCT $column_name) FROM test_table"""),
        Seq(Row(expected_res)))

      checkSqlStatement(
        s"""SELECT ( STDDEV_POP ( DISTINCT "SUBQUERY_1"."SUBQUERY_1_COL_0" ) )
           | AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }

  // Sample Variance

  test("var_samp operator - ALL values", PreloadTest) {

    // int, small int, big int, decimal, float4 types.
    // column name, expected var_samp result.
    val inputList = List(
      ("col_smallint_raw", 13493.23201216251),
      ("col_smallint_bytedict", 13456.334480336061),
      ("col_smallint_delta", 13341.529888017632),
      ("col_smallint_lzo", 13373.781246489287),
      ("col_smallint_mostly8", 13366.212727185404),
      ("col_smallint_runlength", 13300.948164632933),
      ("col_smallint_zstd", 13480.132344508887),
      ("col_int_raw", 82905.89606877387),
      ("col_int_bytedict", 85524.83057407518),
      ("col_int_delta", 85150.19348825752),
      ("col_int_delta32k", 85457.52412306465),
      ("col_int_lzo", 83274.11188153636),
      ("col_int_mostly8", 84256.38850186045),
      ("col_int_mostly16", 84708.11154726952),
      ("col_int_runlength", 83173.8359657528),
      ("col_int_zstd", 84954.98964076828),
      ("col_bigint_raw", 32663044.783589743),
      ("col_bigint_bytedict", 32792681.800584793),
      ("col_bigint_delta", 33382824.657302767),
      ("col_bigint_delta32k", 32731986.183265302),
      ("col_bigint_lzo", 33394458.225906998),
      ("col_bigint_mostly8", 33435554.65281722),
      ("col_bigint_mostly16", 32685328.692294497),
      ("col_bigint_mostly32", 33600848.38678931),
      ("col_bigint_runlength", 33034859.029356446),
      ("col_bigint_zstd", 32911190.389810756),
      ("col_decimal_18_0_raw", 3302939012.80725),
      ("col_decimal_18_18_raw", 0.06853859856908336),
      ("col_decimal_38_0_raw", 3270291179932453L),
      ("col_decimal_38_37_raw", 0.06806422031876119),
      ("col_decimal_18_0_bytedict", 3320797704.5647287),
      ("col_decimal_18_18_bytedict", 0.06721178405966619),
      ("col_decimal_38_0_bytedict", 3271555236354402.5),
      ("col_decimal_38_37_bytedict", 0.06599476244067337),
      ("col_decimal_18_0_delta", 3320532343.3650565),
      ("col_decimal_18_18_delta", 0.06859519884453494),
      ("col_decimal_38_0_delta", 3345237083507349L),
      ("col_decimal_38_37_delta", 0.067445062290748),
      ("col_decimal_18_0_delta32k", 3292634962.1117654),
      ("col_decimal_18_18_delta32k", 0.06851288031152539),
      ("col_decimal_38_0_delta32k", 3396134319974168.5),
      ("col_decimal_38_37_delta32k", 0.06704362526745133),
      ("col_decimal_18_0_lzo", 3364404621.3899527),
      ("col_decimal_18_18_lzo", 0.06626071407023519),
      ("col_decimal_38_0_lzo", 3282233317772087L),
      ("col_decimal_38_37_lzo", 0.06608173616459714),
      ("col_decimal_18_0_mostly8", 3349324401.470721),
      ("col_decimal_18_18_mostly8", 0.0671210819487035),
      ("col_decimal_38_0_mostly8", 3314375753371360.5),
      ("col_decimal_38_37_mostly8", 0.06756683412157471),
      ("col_decimal_18_0_mostly16", 3292795187.559212),
      ("col_decimal_18_18_mostly16", 0.06780105168857439),
      ("col_decimal_38_0_mostly16", 3319327475997583.5),
      ("col_decimal_38_37_mostly16", 0.06764222455649033),
      ("col_decimal_18_0_mostly32", 3330399318.6259255),
      ("col_decimal_18_18_mostly32", 0.06751977359611624),
      ("col_decimal_38_0_mostly32", 3336024912531483.5),
      ("col_decimal_38_37_mostly32", 0.06665723827027543),
      ("col_decimal_18_0_runlength", 3320691341.656961),
      ("col_decimal_18_18_runlength", 0.0676515761116973),
      ("col_decimal_38_0_runlength", 3321074575860790.5),
      ("col_decimal_38_37_runlength", 0.06807996641446348),
      ("col_decimal_18_0_zstd", 3266357618.068803),
      ("col_decimal_18_18_zstd", 0.06686767476912223),
      ("col_decimal_38_0_zstd", 3304039482387007.5),
      ("col_decimal_38_37_zstd", 0.06668480569282186),
      ("col_float4_raw", 521.7137285425684),
      ("col_float4_bytedict", 537.9125584250869),
      ("col_float4_runlength", 530.888442846357),
      ("col_float4_zstd", 522.1816427843112)
      // When running queries with these columns, results are being mismatched in Redshift
      // Issue is tracked in SIM [P89520613] & [Redshift-14972]
      // ("col_decimal_1_0_raw", 28.024333026605298),
      // ("col_decimal_1_0_bytedict", 28.36403884776961),
      // ("col_decimal_1_0_delta", 28.02124248849777),
      // ("col_decimal_1_0_delta32k", 28.154918943788775),
      // ("col_decimal_1_0_lzo", 28.558187637527453),
      // ("col_decimal_1_0_mostly8", 28.600464252850614),
      // ("col_decimal_1_0_mostly16", 29.00699995999189),
      // ("col_decimal_1_0_mostly32", 28.266779715943155),
      // ("col_decimal_1_0_runlength", 28.49316963392677),
      // ("col_decimal_1_0_zstd", 28.197555951190292)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT VAR_SAMP ($column_name) FROM test_table"""),
        Seq(Row(expected_res)))

      checkSqlStatement(
        s"""SELECT ( VAR_SAMP ( CAST ( "SUBQUERY_1"."SUBQUERY_1_COL_0" AS FLOAT8 ) ) )
           | AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }

  test("var_samp operator - DISTINCT values", PreloadTest) {

    // int, small int, big int, decimal, float4 types.
    // column name, expected variance.
    // VAR_SAMP and VARIANCE are synonyms for the same function in Spark and Redshift.
    val inputList = List(
      ("col_smallint_raw", 13433.500000000005),
      ("col_smallint_bytedict", 13433.500000000005),
      ("col_smallint_delta", 13433.499999999998),
      ("col_smallint_lzo", 13433.500000000005),
      ("col_smallint_mostly8", 13433.5),
      ("col_smallint_runlength", 13433.499999999998),
      ("col_smallint_zstd", 13433.5),
      ("col_int_raw", 83729.1772037043),
      ("col_int_bytedict", 83955.92611790601),
      ("col_int_delta", 83712.06255170514),
      ("col_int_delta32k", 83585.35962846363),
      ("col_int_lzo", 83951.1664184503),
      ("col_int_mostly8", 83591.71549032361),
      ("col_int_mostly16", 83604.19781941832),
      ("col_int_runlength", 83170.27297186785),
      ("col_int_zstd", 83570.44567303311),
      ("col_bigint_raw", 32842982.720796987),
      ("col_bigint_bytedict", 32982070.039108045),
      ("col_bigint_delta", 33252684.4063468),
      ("col_bigint_delta32k", 32832548.13754982),
      ("col_bigint_lzo", 33433063.308454785),
      ("col_bigint_mostly8", 33398049.72449028),
      ("col_bigint_mostly16", 32762336.606216386),
      ("col_bigint_mostly32", 33660519.353677064),
      ("col_bigint_runlength", 33146489.03868842),
      ("col_bigint_zstd", 32822710.19381514),
      ("col_decimal_18_0_raw", 3305254185.541879),
      ("col_decimal_18_18_raw", 0.06886333692605201),
      ("col_decimal_38_0_raw", 3270291179932459L),
      ("col_decimal_38_37_raw", 0.06806422031876119),
      ("col_decimal_18_0_bytedict", 3322956856.7690134),
      ("col_decimal_18_18_bytedict", 0.06727135169218722),
      ("col_decimal_38_0_bytedict", 3271555236354421L),
      ("col_decimal_38_37_bytedict", 0.0659947624406734),
      ("col_decimal_18_0_delta", 3317052034.3264575),
      ("col_decimal_18_18_delta", 0.06861380384110588),
      ("col_decimal_38_0_delta", 3345237083507351L),
      ("col_decimal_38_37_delta", 0.06745243442321834),
      ("col_decimal_18_0_delta32k", 3295974321.2707934),
      ("col_decimal_18_18_delta32k", 0.06855600276890168),
      ("col_decimal_38_0_delta32k", 3396134319974163L),
      ("col_decimal_38_37_delta32k", 0.06704362526745156),
      ("col_decimal_18_0_lzo", 3365182959.9704695),
      ("col_decimal_18_18_lzo", 0.06647529164638379),
      ("col_decimal_38_0_lzo", 3282233317772083L),
      ("col_decimal_38_37_lzo", 0.06608173616459717),
      ("col_decimal_18_0_mostly8", 3341848876.9086504),
      ("col_decimal_18_18_mostly8", 0.06721870714173672),
      ("col_decimal_38_0_mostly8", 3314375753371364.5),
      ("col_decimal_38_37_mostly8", 0.06756683412157453),
      ("col_decimal_18_0_mostly16", 3294462381.9101477),
      ("col_decimal_18_18_mostly16", 0.06799330629643627),
      ("col_decimal_38_0_mostly16", 3319327475997594L),
      ("col_decimal_38_37_mostly16", 0.06764222455649041),
      ("col_decimal_18_0_mostly32", 3327824637.3583107),
      ("col_decimal_18_18_mostly32", 0.06742479865572341),
      ("col_decimal_38_0_mostly32", 3336024912531482.5),
      ("col_decimal_38_37_mostly32", 0.06665723827027543),
      ("col_decimal_18_0_runlength", 3316553107.2099977),
      ("col_decimal_18_18_runlength", 0.06755889716526844),
      ("col_decimal_38_0_runlength", 3321074575860781.5),
      ("col_decimal_38_37_runlength", 0.06807996641446348),
      ("col_decimal_18_0_zstd", 3271266945.41261),
      ("col_decimal_18_18_zstd", 0.06703282372899359),
      ("col_decimal_38_0_zstd", 3304039482387008L),
      ("col_decimal_38_37_zstd", 0.06668480569282165),
      ("col_float4_raw", 521.7677673600476),
      ("col_float4_bytedict", 537.9125584250856),
      ("col_float4_runlength", 530.8884428463583),
      ("col_float4_zstd", 522.0612461547563)
      // When running queries with these columns, results are being mismatched in Redshift
      // Issue is tracked in SIM [P89520613] & [Redshift-14972]
      // ("col_decimal_1_0_raw", 31.666666666666668),
      // ("col_decimal_1_0_bytedict", 31.666666666666668),
      // ("col_decimal_1_0_delta", 31.66666666666666),
      // ("col_decimal_1_0_delta32k", 31.66666666666666),
      // ("col_decimal_1_0_lzo", 31.66666666666666),
      // ("col_decimal_1_0_mostly8", 31.66666666666666),
      // ("col_decimal_1_0_mostly16", 31.66666666666666),
      // ("col_decimal_1_0_mostly32", 31.66666666666666),
      // ("col_decimal_1_0_runlength", 31.66666666666666),
      // ("col_decimal_1_0_zstd", 31.66666666666666)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT VARIANCE (DISTINCT $column_name) FROM test_table"""),
        Seq(Row(expected_res)))

      checkSqlStatement(
        s"""SELECT ( VARIANCE ( DISTINCT CAST ( "SUBQUERY_1"."SUBQUERY_1_COL_0" AS FLOAT8 ) ) )
           | AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | AS "SUBQUERY_1" LIMIT 1""".stripMargin)
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

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT VAR_SAMP ($column_name) FROM test_table"""),
        Seq(Row(expected_res)))

      checkSqlStatement(
        s"""SELECT ( VAR_SAMP ("SUBQUERY_1"."SUBQUERY_1_COL_0") )
           | AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }

  test("var_samp operator - float8 type for DISTINCT values", PreloadTest) {

    // column name, expected var_samp result.
    // VAR_SAMP and VARIANCE are synonyms for the same function in Spark and Redshift.
    val inputList = List(
      ("col_float8_raw", 1221.1776036722774),
      ("col_float8_bytedict", 1204.457859641869),
      ("col_float8_runlength", 1196.5140340964929),
      ("col_float8_zstd", 1199.7061692927814)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT VARIANCE (DISTINCT $column_name) FROM test_table"""),
        Seq(Row(expected_res)))

      checkSqlStatement(
        s"""SELECT ( VARIANCE ( DISTINCT "SUBQUERY_1"."SUBQUERY_1_COL_0" ) )
           | AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }

  // Population Variance

  test("var_pop operator - ALL values", PreloadTest) {

    // int, small int, big int, decimal, float4 types
    // column name, expected var_pop result.
    val inputList = List(
      ("col_smallint_raw", 13490.533365760079),
      ("col_smallint_bytedict", 13453.643213439995),
      ("col_smallint_delta", 13338.861582040028),
      ("col_smallint_lzo", 13371.106490239988),
      ("col_smallint_mostly8", 13363.539484639967),
      ("col_smallint_runlength", 13298.287975000007),
      ("col_smallint_zstd", 13477.436318039987),
      ("col_int_raw", 82889.31488956012),
      ("col_int_bytedict", 85507.72560796037),
      ("col_int_delta", 85133.16344955987),
      ("col_int_delta32k", 85440.43261824004),
      ("col_int_lzo", 83257.45705916005),
      ("col_int_mostly8", 84239.53722416007),
      ("col_int_mostly16", 84691.16992496006),
      ("col_int_runlength", 83157.20119855965),
      ("col_int_zstd", 84937.99864284012),
      ("col_bigint_raw", 32656512.174633026),
      ("col_bigint_bytedict", 32786123.26422468),
      ("col_bigint_delta", 33376148.092371307),
      ("col_bigint_delta32k", 32725439.78602865),
      ("col_bigint_lzo", 33387779.334261816),
      ("col_bigint_mostly8", 33428867.541886657),
      ("col_bigint_mostly16", 32678791.626556035),
      ("col_bigint_mostly32", 33594128.21711195),
      ("col_bigint_runlength", 33028252.057550576),
      ("col_bigint_zstd", 32904608.151732795),
      ("col_decimal_18_0_raw", 3302278425.0046887),
      ("col_decimal_18_18_raw", 0.06852489084936954),
      ("col_decimal_38_0_raw", 3269637121696466.5),
      ("col_decimal_38_37_raw", 0.06805060747469743),
      ("col_decimal_18_0_bytedict", 3320133545.023816),
      ("col_decimal_18_18_bytedict", 0.06719834170285426),
      ("col_decimal_38_0_bytedict", 3270900925307131.5),
      ("col_decimal_38_37_bytedict", 0.06598156348818524),
      ("col_decimal_18_0_delta", 3319868236.896384),
      ("col_decimal_18_18_delta", 0.06858147980476602),
      ("col_decimal_38_0_delta", 3344568036090648L),
      ("col_decimal_38_37_delta", 0.06743157327828986),
      ("col_decimal_18_0_delta32k", 3291976435.119343),
      ("col_decimal_18_18_delta32k", 0.06849917773546309),
      ("col_decimal_38_0_delta32k", 3395455093110173.5),
      ("col_decimal_38_37_delta32k", 0.06703021654239784),
      ("col_decimal_18_0_lzo", 3363731740.4656744),
      ("col_decimal_18_18_lzo", 0.06624746192742115),
      ("col_decimal_38_0_lzo", 3281576871108532.5),
      ("col_decimal_38_37_lzo", 0.06606851981736422),
      ("col_decimal_18_0_mostly8", 3348654536.5904264),
      ("col_decimal_18_18_mostly8", 0.06710765773231377),
      ("col_decimal_38_0_mostly8", 3313712878220686L),
      ("col_decimal_38_37_mostly8", 0.06755332075475039),
      ("col_decimal_18_0_mostly16", 3292136628.5217004),
      ("col_decimal_18_18_mostly16", 0.06778749147823668),
      ("col_decimal_38_0_mostly16", 3318663610502384L),
      ("col_decimal_38_37_mostly16", 0.06762869611157903),
      ("col_decimal_18_0_mostly32", 3329733238.7622004),
      ("col_decimal_18_18_mostly32", 0.06750626964139703),
      ("col_decimal_38_0_mostly32", 3335357707548977L),
      ("col_decimal_38_37_mostly32", 0.06664390682262138),
      ("col_decimal_18_0_runlength", 3320027203.38863),
      ("col_decimal_18_18_runlength", 0.06763804579647495),
      ("col_decimal_38_0_runlength", 3320410360945618.5),
      ("col_decimal_38_37_runlength", 0.0680663504211806),
      ("col_decimal_18_0_zstd", 3265704346.545189),
      ("col_decimal_18_18_zstd", 0.06685430123416841),
      ("col_decimal_38_0_zstd", 3303378674490530L),
      ("col_decimal_38_37_zstd", 0.06667146873168328),
      ("col_float4_raw", 521.6093857968599),
      ("col_float4_bytedict", 537.8049759134019),
      ("col_float4_runlength", 530.7822651577877),
      ("col_float4_zstd", 522.0772064557543)
      // When running queries with these columns, results are being mismatched in Redshift
      // Issue is tracked in SIM [P89520613] & [Redshift-14972]
      // ("col_decimal_1_0_zstd", 28.191916440000053),
      // ("col_decimal_1_0_raw", 28.018728159999977),
      // ("col_decimal_1_0_runlength", 28.487470999999985),
      // ("col_decimal_1_0_mostly32", 28.261126359999967),
      // ("col_decimal_1_0_mostly16", 29.001198559999892),
      // ("col_decimal_1_0_mostly8", 28.594744160000044),
      // ("col_decimal_1_0_lzo", 28.55247599999995),
      // ("col_decimal_1_0_delta32k", 28.149287960000017),
      // ("col_decimal_1_0_delta", 28.01563824000007),
      // ("col_decimal_1_0_bytedict", 28.358366040000057)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT VAR_POP ($column_name) FROM test_table"""),
        Seq(Row(expected_res)))

      checkSqlStatement(
        s"""SELECT ( VAR_POP ( CAST ( "SUBQUERY_1"."SUBQUERY_1_COL_0" AS FLOAT8 ) ) )
           | AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }

  test("var_pop operator - DISTINCT values", PreloadTest) {

    // int, small int, big int, decimal, float4 types.
    // column name, expected var_pop result.
    val inputList = List(
      ("col_smallint_raw", 13400.000000000005),
      ("col_smallint_bytedict", 13400.000000000005),
      ("col_smallint_delta", 13399.999999999998),
      ("col_smallint_lzo", 13400.000000000005),
      ("col_smallint_mostly8", 13400),
      ("col_smallint_runlength", 13399.999999999998),
      ("col_smallint_zstd", 13400),
      ("col_int_raw", 83644.77279120056),
      ("col_int_bytedict", 83871.37835746503),
      ("col_int_delta", 83627.50491276401),
      ("col_int_delta32k", 83501.43858465995),
      ("col_int_lzo", 83866.70850454844),
      ("col_int_mostly8", 83507.61919707379),
      ("col_int_mostly16", 83520.2578617683),
      ("col_int_runlength", 83086.43197088814),
      ("col_int_zstd", 83486.45527537177),
      ("col_bigint_raw", 32835612.213005066),
      ("col_bigint_bytedict", 32974655.00537623),
      ("col_bigint_delta", 33245179.85107568),
      ("col_bigint_delta32k", 32825153.41950082),
      ("col_bigint_lzo", 33425541.809397765),
      ("col_bigint_mostly8", 33390508.95914269),
      ("col_bigint_mostly16", 32754873.659381256),
      ("col_bigint_mostly32", 33652951.79087273),
      ("col_bigint_runlength", 33139008.44987355),
      ("col_bigint_zstd", 32815321.01997592),
      ("col_decimal_18_0_raw", 3304584427.8548393),
      ("col_decimal_18_18_raw", 0.0688491587632095),
      ("col_decimal_38_0_raw", 3269637121696472.5),
      ("col_decimal_38_37_raw", 0.06805060747469743),
      ("col_decimal_18_0_bytedict", 3322283511.9145517),
      ("col_decimal_18_18_bytedict", 0.06725744403213094),
      ("col_decimal_38_0_bytedict", 3270900925307150L),
      ("col_decimal_38_37_bytedict", 0.06598156348818528),
      ("col_decimal_18_0_delta", 3316380702.2005057),
      ("col_decimal_18_18_delta", 0.06859971476434179),
      ("col_decimal_38_0_delta", 3344568036090649.5),
      ("col_decimal_38_37_delta", 0.06743894123769659),
      ("col_decimal_18_0_delta32k", 3295306849.7598033),
      ("col_decimal_18_18_delta32k", 0.0685418733371686),
      ("col_decimal_38_0_delta32k", 3395455093110168L),
      ("col_decimal_38_37_delta32k", 0.06703021654239807),
      ("col_decimal_18_0_lzo", 3364500367.0779805),
      ("col_decimal_18_18_lzo", 0.06646159951628154),
      ("col_decimal_38_0_lzo", 3281576871108528.5),
      ("col_decimal_38_37_lzo", 0.06606851981736425),
      ("col_decimal_18_0_mostly8", 3341173892.2839465),
      ("col_decimal_18_18_mostly8", 0.0672048504715364),
      ("col_decimal_38_0_mostly8", 3313712878220690L),
      ("col_decimal_38_37_mostly8", 0.06755332075475022),
      ("col_decimal_18_0_mostly16", 3293794404.9470677),
      ("col_decimal_18_18_mostly16", 0.06797931302492023),
      ("col_decimal_38_0_mostly16", 3318663610502394.5),
      ("col_decimal_38_37_mostly16", 0.06762869611157911),
      ("col_decimal_18_0_mostly32", 3327150715.8035603),
      ("col_decimal_18_18_mostly32", 0.06741095941133596),
      ("col_decimal_38_0_mostly32", 3335357707548976.5),
      ("col_decimal_38_37_mostly32", 0.06664390682262139),
      ("col_decimal_18_0_runlength", 3315882554.6205897),
      ("col_decimal_18_18_runlength", 0.06754504177208444),
      ("col_decimal_38_0_runlength", 3320410360945609.5),
      ("col_decimal_38_37_runlength", 0.0680663504211806),
      ("col_decimal_18_0_zstd", 3270605415.1910906),
      ("col_decimal_18_18_zstd", 0.06701900252822472),
      ("col_decimal_38_0_zstd", 3303378674490530.5),
      ("col_decimal_38_37_zstd", 0.06667146873168309),
      ("col_float4_raw", 521.6633929316899),
      ("col_float4_bytedict", 537.8049759134005),
      ("col_float4_runlength", 530.782265157789),
      ("col_float4_zstd", 521.9568130188982)
      // When running queries with these columns, results are being mismatched in Redshift
      // Issue is tracked in SIM [P89520613] & [Redshift-14972]
      // ("col_decimal_1_0_raw", 30),
      // ("col_decimal_1_0_bytedict", 29.999999999999993),
      // ("col_decimal_1_0_delta", 29.999999999999993),
      // ("col_decimal_1_0_delta32k", 29.999999999999993),
      // ("col_decimal_1_0_lzo", 29.999999999999993),
      // ("col_decimal_1_0_mostly8", 30),
      // ("col_decimal_1_0_mostly16", 29.999999999999993),
      // ("col_decimal_1_0_mostly32", 29.999999999999993),
      // ("col_decimal_1_0_runlength", 29.999999999999993),
      // ("col_decimal_1_0_zstd", 30)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT VAR_POP (DISTINCT $column_name) FROM test_table"""),
        Seq(Row(expected_res)))

      checkSqlStatement(
        s"""SELECT ( VAR_POP ( DISTINCT CAST ( "SUBQUERY_1"."SUBQUERY_1_COL_0" AS FLOAT8 ) ) )
           | AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }

  test("var_pop operator - float8 type for ALL values", PreloadTest) {

    // column name, expected var_pop result.
    val inputList = List(
      ("col_float8_raw", 1220.9333681515386),
      ("col_float8_bytedict", 1204.216968069941),
      ("col_float8_runlength", 1196.2747312896752),
      ("col_float8_zstd", 1199.4662280589275)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT VAR_POP ($column_name) FROM test_table"""),
        Seq(Row(expected_res)))

      checkSqlStatement(
        s"""SELECT ( VAR_POP ("SUBQUERY_1"."SUBQUERY_1_COL_0") )
           | AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }

  test("var_pop operator - float8 type for DISTINCT values", PreloadTest) {

    // column name, expected var_pop result.
    val inputList = List(
      ("col_float8_raw", 1220.933368151543),
      ("col_float8_bytedict", 1204.2169680699405),
      ("col_float8_runlength", 1196.2747312896736),
      ("col_float8_zstd", 1199.4662280589228)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT VAR_POP (DISTINCT $column_name) FROM test_table"""),
        Seq(Row(expected_res)))

      checkSqlStatement(
        s"""SELECT ( VAR_POP ( DISTINCT "SUBQUERY_1"."SUBQUERY_1_COL_0" ) )
           | AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | AS "SUBQUERY_1" LIMIT 1""".stripMargin)
    })
  }

}

class TextAggregateStatisticalOperatorsCorrectnessSuite
  extends PushdownAggregateStatisticalOperatorsCorrectnessSuite {
  override protected val s3format: String = "TEXT"
}

class ParquetAggregateStatisticalOperatorsCorrectnessSuite
  extends PushdownAggregateStatisticalOperatorsCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
}

class TextPushdownNoCacheAggregateStatisticalOperatorsCorrectnessSuite
  extends TextAggregateStatisticalOperatorsCorrectnessSuite {
  override protected val s3_result_cache = "false"
}

class ParquetPushdownNoCacheAggregateStatisticalOperatorsCorrectnessSuite
  extends ParquetAggregateStatisticalOperatorsCorrectnessSuite {
  override protected val s3_result_cache = "false"
}

// Precision value returns for No pushdown is different.
// As per the already created issue in SIM [Redshift-7037].
abstract class NoPushdownAggregateStatisticalOperatorsCorrectnessSuite
  extends AggregateStatisticalOperatorsCorrectnessSuite {

  test("stddev_samp operator - ALL values", PreloadTest) {

    // int, small int, big int, decimal, float4 types.
    // column name, expected stddev_samp result.
    val inputList = List(
      ("col_smallint_raw", 116.16037195258302),
      ("col_smallint_bytedict", 116.00144171662721),
      ("col_smallint_delta", 115.50554050788054),
      ("col_smallint_lzo", 115.64506581125407),
      ("col_smallint_mostly8", 115.61233812697242),
      ("col_smallint_runlength", 115.32973668847481),
      ("col_smallint_zstd", 116.10397213062474),
      ("col_int_raw", 287.933839742351),
      ("col_int_bytedict", 292.4462866477794),
      ("col_int_delta", 291.80506076532924),
      ("col_int_delta32k", 292.33118910418136),
      ("col_int_lzo", 288.57254180108055),
      ("col_int_mostly8", 290.26951011406703),
      ("col_int_mostly16", 291.04657968660194),
      ("col_int_runlength", 288.3987447367842),
      ("col_int_zstd", 291.47039239135125),
      ("col_bigint_raw", 5715.15920894508),
      ("col_bigint_bytedict", 5726.489483146266),
      ("col_bigint_delta", 5777.787176532445),
      ("col_bigint_delta32k", 5721.187480170993),
      ("col_bigint_lzo", 5778.793838328808),
      ("col_bigint_mostly8", 5782.348541277775),
      ("col_bigint_mostly16", 5717.1084205474435),
      ("col_bigint_mostly32", 5796.6238783268755),
      ("col_bigint_runlength", 5747.595934767549),
      ("col_bigint_zstd", 5736.827554477366),
      ("col_decimal_1_0_raw", 5.293801377706317),
      ("col_decimal_18_0_raw", 57471.20159529684),
      ("col_decimal_18_18_raw", 0.261798774957186),
      ("col_decimal_38_0_raw", 57186459.76044026),
      ("col_decimal_38_37_raw", 0.2608912039888677),
      ("col_decimal_1_0_bytedict", 5.325789974057333),
      ("col_decimal_18_0_bytedict", 57626.36293021389),
      ("col_decimal_18_18_bytedict", 0.25925235593850676),
      ("col_decimal_38_0_bytedict", 5.7197510753129825E7),
      ("col_decimal_38_37_bytedict", 0.2568944577850471),
      ("col_decimal_1_0_delta", 5.293509468065375),
      ("col_decimal_18_0_delta", 57624.060455377985),
      ("col_decimal_18_18_delta", 0.26190685146543025),
      ("col_decimal_38_0_delta", 57838024.54706894),
      ("col_decimal_38_37_delta", 0.25970187194309563),
      ("col_decimal_1_0_delta32k", 5.30612089419274),
      ("col_decimal_18_0_delta32k", 57381.486231290364),
      ("col_decimal_18_18_delta32k", 0.26174965197975986),
      ("col_decimal_38_0_delta32k", 58276361.588333294),
      ("col_decimal_38_37_delta32k", 0.2589278379538424),
      ("col_decimal_1_0_lzo", 5.343986118762609),
      ("col_decimal_18_0_lzo", 58003.48801054943),
      ("col_decimal_18_18_lzo", 0.2574115655331655),
      ("col_decimal_38_0_lzo", 57290778.645189375),
      ("col_decimal_38_37_lzo", 0.25706368114651523),
      ("col_decimal_1_0_mostly8", 5.34794018785276),
      ("col_decimal_18_0_mostly8", 57873.3479372908),
      ("col_decimal_18_18_mostly8", 0.2590773667241187),
      ("col_decimal_38_0_mostly8", 57570615.3638413),
      ("col_decimal_38_37_mostly8", 0.25993621163965336),
      ("col_decimal_1_0_mostly16", 5.385814697888509),
      ("col_decimal_18_0_mostly16", 57382.88235666811),
      ("col_decimal_18_18_mostly16", 0.26038635081081796),
      ("col_decimal_38_0_mostly16", 57613604.95575315),
      ("col_decimal_38_37_mostly16", 0.2600811883941057),
      ("col_decimal_1_0_mostly32", 5.316651174935511),
      ("col_decimal_18_0_mostly32", 57709.61201243625),
      ("col_decimal_18_18_mostly32", 0.2598456726522807),
      ("col_decimal_38_0_mostly32", 57758331.974975556),
      ("col_decimal_38_37_mostly32", 0.258180631090474),
      ("col_decimal_1_0_runlength", 5.33789936528657),
      ("col_decimal_18_0_runlength", 57625.44005608079),
      ("col_decimal_18_18_runlength", 0.2600991659188806),
      ("col_decimal_38_0_runlength", 57628765.177303515),
      ("col_decimal_38_37_runlength", 0.26092137975732),
      ("col_decimal_1_0_zstd", 5.31013709344592),
      ("col_decimal_18_0_zstd", 57152.05698895538),
      ("col_decimal_18_18_zstd", 0.25858784729588946),
      ("col_decimal_38_0_zstd", 57480774.89375911),
      ("col_decimal_38_37_zstd", 0.25823401343127106),
      ("col_float4_raw", 22.84105357777019),
      ("col_float4_bytedict", 23.19294199589795),
      ("col_float4_runlength", 23.041016532400583),
      ("col_float4_zstd", 22.85129411618325)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT STDDEV_SAMP ($column_name) FROM test_table"""),
        Seq(Row(expected_res)))
    })
  }

  test("stddev_samp operator - DISTINCT values", PreloadTest) {

    // int, small int, big int, decimal, float4 types.
    // column name, expected stddev_samp result
    val inputList = List(
      ("col_smallint_raw", 115.90297666583021),
      ("col_smallint_bytedict", 115.90297666583025),
      ("col_smallint_delta", 115.90297666583034),
      ("col_smallint_lzo", 115.90297666583025),
      ("col_smallint_mostly8", 115.90297666583027),
      ("col_smallint_runlength", 115.90297666583025),
      ("col_smallint_zstd", 115.90297666583032),
      ("col_int_raw", 289.35994402077216),
      ("col_int_bytedict", 289.7514902772822),
      ("col_int_delta", 289.330369217794),
      ("col_int_delta32k", 289.1113273956305),
      ("col_int_lzo", 289.7432767441725),
      ("col_int_mostly8", 289.1223192531555),
      ("col_int_mostly16", 289.14390503591534),
      ("col_int_runlength", 288.39256746987724),
      ("col_int_zstd", 289.08553348971486),
      ("col_bigint_raw", 5730.879751032728),
      ("col_bigint_bytedict", 5743.001831717287),
      ("col_bigint_delta", 5766.5140601880685),
      ("col_bigint_delta32k", 5729.969296388055),
      ("col_bigint_lzo", 5782.1331105790605),
      ("col_bigint_mostly8", 5779.104578089083),
      ("col_bigint_mostly16", 5723.839323934267),
      ("col_bigint_mostly32", 5801.768640136993),
      ("col_bigint_runlength", 5757.298762326687),
      ("col_bigint_zstd", 5729.110768157229),
      ("col_decimal_1_0_raw", 5.627314338711376),
      ("col_decimal_18_0_raw", 57491.34009172057),
      ("col_decimal_18_18_raw", 0.26241824808128766),
      ("col_decimal_38_0_raw", 5.718645976044032E7),
      ("col_decimal_38_37_raw", 0.2608912039888675),
      ("col_decimal_1_0_bytedict", 5.627314338711376),
      ("col_decimal_18_0_bytedict", 57645.09395229585),
      ("col_decimal_18_18_bytedict", 0.25936721398855905),
      ("col_decimal_38_0_bytedict", 5.719751075312985E7),
      ("col_decimal_38_37_bytedict", 0.25689445778504694),
      ("col_decimal_1_0_delta", 5.627314338711376),
      ("col_decimal_18_0_delta", 57593.85413676051),
      ("col_decimal_18_18_delta", 0.26194236740379734),
      ("col_decimal_38_0_delta", 5.783802454706896E7),
      ("col_decimal_38_37_delta", 0.2597160650079586),
      ("col_decimal_1_0_delta32k", 5.627314338711376),
      ("col_decimal_18_0_delta32k", 57410.57673696365),
      ("col_decimal_18_18_delta32k", 0.26183201249828425),
      ("col_decimal_38_0_delta32k", 5.8276361588333264E7),
      ("col_decimal_38_37_delta32k", 0.25892783795384283),
      ("col_decimal_1_0_lzo", 5.627314338711376),
      ("col_decimal_18_0_lzo", 58010.197034404795),
      ("col_decimal_18_18_lzo", 0.25782802727086135),
      ("col_decimal_38_0_lzo", 5.7290778645189464E7),
      ("col_decimal_38_37_lzo", 0.2570636811465149),
      ("col_decimal_1_0_mostly8", 5.627314338711376),
      ("col_decimal_18_0_mostly8", 57808.72665012299),
      ("col_decimal_18_18_mostly8", 0.25926570760850054),
      ("col_decimal_38_0_mostly8", 5.757061536384129E7),
      ("col_decimal_38_37_mostly8", 0.25993621163965347),
      ("col_decimal_18_0_mostly16", 57397.40744938),
      ("col_decimal_1_0_mostly16", 5.627314338711376),
      ("col_decimal_18_18_mostly16", 0.2607552613015436),
      ("col_decimal_38_0_mostly16", 5.76136049557532E7),
      ("col_decimal_38_37_mostly16", 0.2600811883941058),
      ("col_decimal_1_0_mostly32", 5.627314338711376),
      ("col_decimal_18_0_mostly32", 57687.30048596756),
      ("col_decimal_18_18_mostly32", 0.2596628557489954),
      ("col_decimal_38_0_mostly32", 5.7758331974975474E7),
      ("col_decimal_38_37_mostly32", 0.2581806310904738),
      ("col_decimal_1_0_runlength", 5.627314338711376),
      ("col_decimal_18_0_runlength", 57589.52254716128),
      ("col_decimal_18_18_runlength", 0.2599209440681311),
      ("col_decimal_38_0_runlength", 5.762876517730349E7),
      ("col_decimal_38_37_runlength", 0.26092137975732044),
      ("col_decimal_1_0_zstd", 5.627314338711376),
      ("col_decimal_18_0_zstd", 57194.99056222147),
      ("col_decimal_18_18_zstd", 0.2589069789113333),
      ("col_decimal_38_0_zstd", 5.74807748937592E7),
      ("col_decimal_38_37_zstd", 0.2582340134312707),
      ("col_float4_raw", 22.84223647894503),
      ("col_float4_bytedict", 23.192941995897865),
      ("col_float4_runlength", 23.041016532400608),
      ("col_float4_zstd", 22.848659613963317)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT STDDEV (DISTINCT $column_name) FROM test_table"""),
        Seq(Row(expected_res)))
    })
  }

  test("stddev_samp operator - float8 type for ALL values", PreloadTest) {

    // column name, expected stddev result.
     val inputList = List(
       ("col_float8_raw", 34.94535167475458),
       ("col_float8_bytedict", 34.70530016642803),
       ("col_float8_runlength", 34.59066397305051),
       ("col_float8_zstd", 34.636774810781475)
     )

     inputList.foreach(test_case => {
       val column_name = test_case._1.toUpperCase
       val expected_res = test_case._2

       checkAnswer(
         sqlContext.sql(s"""SELECT STDDEV_SAMP ($column_name) FROM test_table"""),
         Seq(Row(expected_res)))
     })
   }

  test("stddev_samp operator - float8 type for DISTINCT values", PreloadTest) {

    // column name, expected stddev result.
    val inputList = List(
      ("col_float8_raw", 34.94535167475466),
      ("col_float8_bytedict", 34.70530016642799),
      ("col_float8_runlength", 34.59066397305041),
      ("col_float8_zstd", 34.63677481078146)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT STDDEV (DISTINCT $column_name) FROM test_table"""),
        Seq(Row(expected_res)))
    })
  }

  // Population Standard deviation

  test("stddev_pop operator - ALL values", PreloadTest) {

    // int, small int, big int, decimal, float4 types.
    // column name, expected stddev_pop result.
    val inputList = List(
      ("col_smallint_raw", 116.14875533452782),
      ("col_smallint_bytedict", 115.98984099239034),
      ("col_smallint_delta", 115.49398937624429),
      ("col_smallint_lzo", 115.6335007263898),
      ("col_smallint_mostly8", 115.60077631504022),
      ("col_smallint_runlength", 115.3182031380996),
      ("col_smallint_zstd", 116.09236115283377),
      ("col_int_raw", 287.9050449185636),
      ("col_int_bytedict", 292.417040556737),
      ("col_int_delta", 291.7758788000815),
      ("col_int_delta32k", 292.30195452346885),
      ("col_int_lzo", 288.5436831038934),
      ("col_int_mostly8", 290.2404817115629),
      ("col_int_mostly16", 291.0174735732548),
      ("col_int_runlength", 288.36990342017253),
      ("col_int_zstd", 291.4412438946144),
      ("col_bigint_raw", 5714.587664445531),
      ("col_bigint_bytedict", 5725.916805562641),
      ("col_bigint_delta", 5777.209368922967),
      ("col_bigint_delta32k", 5720.615332814177),
      ("col_bigint_lzo", 5778.215930048116),
      ("col_bigint_mostly8", 5781.770277509014),
      ("col_bigint_mostly16", 5716.536681116988),
      ("col_bigint_mostly32", 5796.044186953025),
      ("col_bigint_runlength", 5747.021146433218),
      ("col_bigint_zstd", 5736.253843034912),
      ("col_decimal_1_0_raw", 5.2932719710968925),
      ("col_decimal_18_0_raw", 57465.45418775257),
      ("col_decimal_18_18_raw", 0.2617725937705655),
      ("col_decimal_38_0_raw", 57180740.82850332),
      ("col_decimal_38_37_raw", 0.26086511356388237),
      ("col_decimal_1_0_bytedict", 5.325257368428314),
      ("col_decimal_18_0_bytedict", 57620.600005760236),
      ("col_decimal_18_18_bytedict", 0.2592264294065215),
      ("col_decimal_38_0_bytedict", 5.7191790716038354E7),
      ("col_decimal_38_37_bytedict", 0.2568687670546678),
      ("col_decimal_1_0_delta", 5.292980090648374),
      ("col_decimal_18_0_delta", 57618.29776118333),
      ("col_decimal_18_18_delta", 0.26188065947061845),
      ("col_decimal_38_0_delta", 57832240.45539519),
      ("col_decimal_38_37_delta", 0.25967590045726213),
      ("col_decimal_1_0_delta32k", 5.305590255570063),
      ("col_decimal_18_0_delta32k", 57375.747795731106),
      ("col_decimal_18_18_delta32k", 0.2617234757056828),
      ("col_decimal_38_0_delta32k", 58270533.66076351),
      ("col_decimal_38_37_delta32k", 0.2589019438752783),
      ("col_decimal_1_0_lzo", 5.34345169342813),
      ("col_decimal_18_0_lzo", 57997.68737170193),
      ("col_decimal_18_18_lzo", 0.2573858230894257),
      ("col_decimal_38_0_lzo", 57285049.28084231),
      ("col_decimal_38_37_lzo", 0.25703797349295365),
      ("col_decimal_1_0_mostly8", 5.347405367091599),
      ("col_decimal_18_0_mostly8", 57867.560313101385),
      ("col_decimal_18_18_mostly8", 0.2590514576919299),
      ("col_decimal_38_0_mostly8", 57564858.01442305),
      ("col_decimal_38_37_mostly8", 0.25991021671867837),
      ("col_decimal_1_0_mostly16", 5.385276089486953),
      ("col_decimal_18_0_mostly16", 57377.143781489336),
      ("col_decimal_18_18_mostly16", 0.26036031087367495),
      ("col_decimal_38_0_mostly16", 57607843.307160735),
      ("col_decimal_38_37_mostly16", 0.2600551789747303),
      ("col_decimal_1_0_mostly32", 5.316119483232103),
      ("col_decimal_18_0_mostly32", 57703.84076265809),
      ("col_decimal_18_18_mostly32", 0.2598196867856572),
      ("col_decimal_38_0_mostly32", 57752555.85295751),
      ("col_decimal_38_37_mostly32", 0.2581548117363327),
      ("col_decimal_1_0_runlength", 5.337365548657876),
      ("col_decimal_18_0_runlength", 57619.67722391917),
      ("col_decimal_18_18_runlength", 0.2600731547016628),
      ("col_decimal_38_0_runlength", 5.762300201261313E7),
      ("col_decimal_38_37_runlength", 0.2608952863146069),
      ("col_decimal_1_0_zstd", 5.309606053183235),
      ("col_decimal_18_0_zstd", 57146.34149746762),
      ("col_decimal_18_18_zstd", 0.2585619872180913),
      ("col_decimal_38_0_zstd", 57475026.528837115),
      ("col_decimal_38_37_zstd", 0.2582081887386287),
      ("col_float4_raw", 22.838769358195723),
      ("col_float4_bytedict", 23.190622585722053),
      ("col_float4_runlength", 23.03871231553074),
      ("col_float4_zstd", 22.849008872503735)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT STDDEV_POP ($column_name) FROM test_table"""),
        Seq(Row(expected_res)))
    })
  }

  test("stddev_pop operator - DISTINCT values", PreloadTest) {

    // int, small int, big int, decimal, float4 types.
    // column name, expected stddev_pop result.
    val inputList = List(
      ("col_smallint_raw", 115.75836902790218),
      ("col_smallint_bytedict", 115.75836902790222),
      ("col_smallint_delta", 115.7583690279023),
      ("col_smallint_lzo", 115.75836902790222),
      ("col_smallint_mostly8", 115.75836902790223),
      ("col_smallint_runlength", 115.75836902790222),
      ("col_smallint_zstd", 115.75836902790229),
      ("col_int_raw", 289.21406050052383),
      ("col_int_bytedict", 289.6055565030909),
      ("col_int_delta", 289.1842058494272),
      ("col_int_delta32k", 288.96615473902784),
      ("col_int_lzo", 289.597493954192),
      ("col_int_mostly8", 288.9768488946368),
      ("col_int_mostly16", 288.9987160209685),
      ("col_int_runlength", 288.2471716615585),
      ("col_int_zstd", 288.94022785927837),
      ("col_bigint_raw", 5730.236662914109),
      ("col_bigint_bytedict", 5742.356224179784),
      ("col_bigint_delta", 5765.8633222680155),
      ("col_bigint_delta32k", 5729.323993238716),
      ("col_bigint_lzo", 5781.4826653201735),
      ("col_bigint_mostly8", 5778.452124846463),
      ("col_bigint_mostly16", 5723.187368886428),
      ("col_bigint_mostly32", 5801.116426246996),
      ("col_bigint_runlength", 5756.649064331917),
      ("col_bigint_zstd", 5728.465852213479),
      ("col_decimal_1_0_raw", 5.47722557505166),
      ("col_decimal_18_0_raw", 57485.51493945964),
      ("col_decimal_18_18_raw", 0.2623912322529275),
      ("col_decimal_38_0_raw", 5.718074082850338E7),
      ("col_decimal_38_37_raw", 0.26086511356388214),
      ("col_decimal_1_0_bytedict", 5.47722557505166),
      ("col_decimal_18_0_bytedict", 57639.253221346946),
      ("col_decimal_18_18_bytedict", 0.2593404018507929),
      ("col_decimal_38_0_bytedict", 5.7191790716038376E7),
      ("col_decimal_38_37_bytedict", 0.2568687670546677),
      ("col_decimal_1_0_delta", 5.47722557505166),
      ("col_decimal_18_0_delta", 57588.025684168875),
      ("col_decimal_18_18_delta", 0.26191547255620823),
      ("col_decimal_38_0_delta", 5.783224045539521E7),
      ("col_decimal_38_37_delta", 0.2596900869068675),
      ("col_decimal_1_0_delta32k", 5.47722557505166),
      ("col_decimal_18_0_delta32k", 57404.76330201006),
      ("col_decimal_18_18_delta32k", 0.26180502924345933),
      ("col_decimal_38_0_delta32k", 5.827053366076348E7),
      ("col_decimal_38_37_delta32k", 0.25890194387527876),
      ("col_decimal_1_0_lzo", 5.47722557505166),
      ("col_decimal_18_0_lzo", 58004.31334890517),
      ("col_decimal_18_18_lzo", 0.25780147306848694),
      ("col_decimal_38_0_lzo", 5.72850492808424E7),
      ("col_decimal_38_37_lzo", 0.2570379734929533),
      ("col_decimal_1_0_mostly8", 5.47722557505166),
      ("col_decimal_18_0_mostly8", 57802.88826939305),
      ("col_decimal_18_18_mostly8", 0.25923898331758705),
      ("col_decimal_38_0_mostly8", 5.7564858014423035E7),
      ("col_decimal_38_37_mostly8", 0.2599102167186784),
      ("col_decimal_1_0_mostly16", 5.47722557505166),
      ("col_decimal_18_0_mostly16", 57391.588276916176),
      ("col_decimal_18_18_mostly16", 0.26072842772685956),
      ("col_decimal_38_0_mostly16", 5.760784330716079E7),
      ("col_decimal_38_37_mostly16", 0.26005517897473046),
      ("col_decimal_1_0_mostly32", 5.47722557505166),
      ("col_decimal_18_0_mostly32", 57681.45902977457),
      ("col_decimal_18_18_mostly32", 0.2596362058945865),
      ("col_decimal_38_0_mostly32", 5.7752555852957435E7),
      ("col_decimal_38_37_mostly32", 0.2581548117363325),
      ("col_decimal_1_0_runlength", 5.47722557505166),
      ("col_decimal_18_0_runlength", 57583.700424864925),
      ("col_decimal_18_18_runlength", 0.25989428961038086),
      ("col_decimal_38_0_runlength", 5.762300201261312E7),
      ("col_decimal_38_37_runlength", 0.2608952863146073),
      ("col_decimal_1_0_zstd", 5.47722557505166),
      ("col_decimal_18_0_zstd", 57189.207156517594),
      ("col_decimal_18_18_zstd", 0.2588802860942191),
      ("col_decimal_38_0_zstd", 5.7475026528837204E7),
      ("col_decimal_38_37_zstd", 0.2582081887386284),
      ("col_float4_raw", 22.83995168409271),
      ("col_float4_bytedict", 23.190622585721968),
      ("col_float4_runlength", 23.038712315530763),
      ("col_float4_zstd", 22.846374176636875)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT STDDEV_POP (DISTINCT $column_name) FROM test_table"""),
        Seq(Row(expected_res)))
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

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT STDDEV_POP ($column_name) FROM test_table"""),
        Seq(Row(expected_res)))
    })
  }

  test("stddev_pop operator - float8 type for DISTINCT values", PreloadTest) {

    // column name, expected stddev_pop result.
    val inputList = List(
      ("col_float8_raw", 34.941856964842955),
      ("col_float8_bytedict", 34.701829462867494),
      ("col_float8_runlength", 34.58720473368249),
      ("col_float8_zstd", 34.63331096009919)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT STDDEV_POP (DISTINCT $column_name) FROM test_table"""),
        Seq(Row(expected_res)))
    })
  }

  // Sample Variance

  test("var_samp operator - ALL values", PreloadTest) {

    // int, small int, big int, decimal, float4 types.
    // column name, expected var_samp result.
    val inputList = List(
      ("col_smallint_raw", 13493.232012162436),
      ("col_smallint_bytedict", 13456.334480336061),
      ("col_smallint_delta", 13341.529888017632),
      ("col_smallint_lzo", 13373.781246489287),
      ("col_smallint_mostly8", 13366.2127271854),
      ("col_smallint_runlength", 13300.948164632933),
      ("col_smallint_zstd", 13480.132344508887),
      ("col_int_raw", 82905.89606877387),
      ("col_int_bytedict", 85524.83057407517),
      ("col_int_delta", 85150.1934882575),
      ("col_int_delta32k", 85457.52412306465),
      ("col_int_lzo", 83274.11188153637),
      ("col_int_mostly8", 84256.38850186045),
      ("col_int_mostly16", 84708.11154726952),
      ("col_int_runlength", 83173.8359657528),
      ("col_int_zstd", 84954.98964076828),
      ("col_bigint_raw", 3.266304478358975E7),
      ("col_bigint_bytedict", 32792681.800584793),
      ("col_bigint_delta", 33382824.657302767),
      ("col_bigint_delta32k", 3.273198618326531E7),
      ("col_bigint_lzo", 33394458.225906998),
      ("col_bigint_mostly8", 33435554.65281722),
      ("col_bigint_mostly16", 3.268532869229449E7),
      ("col_bigint_mostly32", 33600848.38678931),
      ("col_bigint_runlength", 3.303485902935645E7),
      ("col_bigint_zstd", 32911190.389810756),
      ("col_decimal_1_0_raw", 28.024333026605298),
      ("col_decimal_18_0_raw", 3.3029390128072505E9),
      ("col_decimal_18_18_raw", 0.06853859856908331),
      ("col_decimal_38_0_raw", 3270291179932453L),
      ("col_decimal_38_37_raw", 0.068064220318761),
      ("col_decimal_1_0_bytedict", 28.36403884776961),
      ("col_decimal_18_0_bytedict", 3.320797704564729E9),
      ("col_decimal_18_18_bytedict", 0.06721178405966621),
      ("col_decimal_38_0_bytedict", 3.271555236354402E15),
      ("col_decimal_38_37_bytedict", 0.06599476244067334),
      ("col_decimal_1_0_delta", 28.02124248849777),
      ("col_decimal_18_0_delta", 3320532343.3650565),
      ("col_decimal_18_18_delta", 0.06859519884453494),
      ("col_decimal_38_0_delta", 3345237083507349L),
      ("col_decimal_38_37_delta", 0.06744506229074805),
      ("col_decimal_1_0_delta32k", 28.15491894378877),
      ("col_decimal_18_0_delta32k", 3292634962.1117654),
      ("col_decimal_18_18_delta32k", 0.06851288031152541),
      ("col_decimal_38_0_delta32k", 3396134319974168.5),
      ("col_decimal_38_37_delta32k", 0.06704362526745127),
      ("col_decimal_1_0_lzo", 28.558187637527453),
      ("col_decimal_18_0_lzo", 3.3644046213899517E9),
      ("col_decimal_18_18_lzo", 0.06626071407023518),
      ("col_decimal_38_0_lzo", 3282233317772087L),
      ("col_decimal_38_37_lzo", 0.06608173616459725),
      ("col_decimal_1_0_mostly8", 28.600464252850614),
      ("col_decimal_18_0_mostly8", 3349324401.470721),
      ("col_decimal_18_18_mostly8", 0.06712108194870349),
      ("col_decimal_38_0_mostly8", 3314375753371360.5),
      ("col_decimal_38_37_mostly8", 0.06756683412157466),
      ("col_decimal_1_0_mostly16", 29.00699995999189),
      ("col_decimal_18_0_mostly16", 3292795187.559212),
      ("col_decimal_18_18_mostly16", 0.06780105168857437),
      ("col_decimal_38_0_mostly16", 3319327475997583.5),
      ("col_decimal_38_37_mostly16", 0.0676422245564903),
      ("col_decimal_1_0_mostly32", 28.266779715943148),
      ("col_decimal_18_0_mostly32", 3.3303993186259265E9),
      ("col_decimal_18_18_mostly32", 0.06751977359611622),
      ("col_decimal_38_0_mostly32", 3336024912531483.5),
      ("col_decimal_38_37_mostly32", 0.06665723827027543),
      ("col_decimal_1_0_runlength", 28.49316963392677),
      ("col_decimal_18_0_runlength", 3320691341.656961),
      ("col_decimal_18_18_runlength", 0.06765157611169738),
      ("col_decimal_38_0_runlength", 3.32107457586079E15),
      ("col_decimal_38_37_runlength", 0.06807996641446361),
      ("col_decimal_1_0_zstd", 28.197555951190292),
      ("col_decimal_18_0_zstd", 3.2663576180688033E9),
      ("col_decimal_18_18_zstd", 0.06686767476912225),
      ("col_decimal_38_0_zstd", 3304039482387007.5),
      ("col_decimal_38_37_zstd", 0.06668480569282188),
      ("col_float4_raw", 521.7137285425684),
      ("col_float4_bytedict", 537.9125584250868),
      ("col_float4_runlength", 530.8884428463571),
      ("col_float4_zstd", 522.1816427843112)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_all_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT VAR_SAMP ($column_name) FROM test_table"""),
        Seq(Row(expected_all_res)))
    })
  }

  test("var_samp operator - DISTINCT values", PreloadTest) {

    // int, small int, big int, decimal, float4 types.
    // column name, expected var_samp result.
    // VAR_SAMP and VARIANCE are synonyms for the same function in Spark and Redshift.
    val inputList = List(
      ("col_smallint_raw", 13433.499999999982),
      ("col_smallint_bytedict", 13433.49999999999),
      ("col_smallint_delta", 13433.500000000011),
      ("col_smallint_lzo", 13433.49999999999),
      ("col_smallint_mostly8", 13433.499999999995),
      ("col_smallint_runlength", 13433.499999999993),
      ("col_smallint_zstd", 13433.500000000007),
      ("col_int_raw", 83729.17720370438),
      ("col_int_bytedict", 83955.92611790597),
      ("col_int_delta", 83712.06255170499),
      ("col_int_delta32k", 83585.35962846346),
      ("col_int_lzo", 83951.16641845013),
      ("col_int_mostly8", 83591.7154903236),
      ("col_int_mostly16", 83604.19781941843),
      ("col_int_runlength", 83170.2729718677),
      ("col_int_zstd", 83570.44567303304),
      ("col_bigint_raw", 3.284298272079694E7),
      ("col_bigint_bytedict", 3.2982070039108116E7),
      ("col_bigint_delta", 3.325268440634668E7),
      ("col_bigint_delta32k", 3.2832548137549825E7),
      ("col_bigint_lzo", 3.3433063308454677E7),
      ("col_bigint_mostly8", 3.33980497244902E7),
      ("col_bigint_mostly16", 3.276233660621629E7),
      ("col_bigint_mostly32", 3.366051935367705E7),
      ("col_bigint_runlength", 3.3146489038688403E7),
      ("col_bigint_zstd", 3.282271019381512E7),
      ("col_decimal_1_0_raw", 31.66666666666666),
      ("col_decimal_18_0_raw", 3.3052541855418773E9),
      ("col_decimal_18_18_raw", 0.06886333692605223),
      ("col_decimal_38_0_raw", 3.2702911799324595E15),
      ("col_decimal_38_37_raw", 0.06806422031876089),
      ("col_decimal_1_0_bytedict", 31.66666666666666),
      ("col_decimal_18_0_bytedict", 3.322956856769016E9),
      ("col_decimal_18_18_bytedict", 0.067271351692187),
      ("col_decimal_38_0_bytedict", 3.2715552363544045E15),
      ("col_decimal_38_37_bytedict", 0.06599476244067327),
      ("col_decimal_1_0_delta", 31.66666666666666),
      ("col_decimal_18_0_delta", 3.3170520343264456E9),
      ("col_decimal_18_18_delta", 0.06861380384110596),
      ("col_decimal_38_0_delta", 3.3452370835073515E15),
      ("col_decimal_38_37_delta", 0.06745243442321816),
      ("col_decimal_1_0_delta32k", 31.66666666666666),
      ("col_decimal_18_0_delta32k", 3.295974321270792E9),
      ("col_decimal_18_18_delta32k", 0.06855600276890168),
      ("col_decimal_38_0_delta32k", 3.396134319974165E15),
      ("col_decimal_38_37_delta32k", 0.06704362526745149),
      ("col_decimal_1_0_lzo", 31.66666666666666),
      ("col_decimal_18_0_lzo", 3.365182959970467E9),
      ("col_decimal_18_18_lzo", 0.06647529164638404),
      ("col_decimal_38_0_lzo", 3.282233317772097E15),
      ("col_decimal_38_37_lzo", 0.06608173616459709),
      ("col_decimal_1_0_mostly8", 31.66666666666666),
      ("col_decimal_18_0_mostly8", 3.3418488769086404E9),
      ("col_decimal_18_18_mostly8", 0.06721870714173649),
      ("col_decimal_38_0_mostly8", 3.3143757533713585E15),
      ("col_decimal_38_37_mostly8", 0.06756683412157471),
      ("col_decimal_1_0_mostly16", 31.66666666666666),
      ("col_decimal_18_0_mostly16", 3.2944623819101424E9),
      ("col_decimal_18_18_mostly16", 0.06799330629643627),
      ("col_decimal_38_0_mostly16", 3.3193274759975895E15),
      ("col_decimal_38_37_mostly16", 0.06764222455649037),
      ("col_decimal_1_0_mostly32", 31.66666666666666),
      ("col_decimal_18_0_mostly32", 3.3278246373583126E9),
      ("col_decimal_18_18_mostly32", 0.06742479865572358),
      ("col_decimal_38_0_mostly32", 3.3360249125314745E15),
      ("col_decimal_38_37_mostly32", 0.06665723827027532),
      ("col_decimal_1_0_runlength", 31.66666666666666),
      ("col_decimal_18_0_runlength", 3.316553107209997E9),
      ("col_decimal_18_18_runlength", 0.06755889716526853),
      ("col_decimal_38_0_runlength", 3.3210745758607875E15),
      ("col_decimal_38_37_runlength", 0.06807996641446382),
      ("col_decimal_1_0_zstd", 31.66666666666666),
      ("col_decimal_18_0_zstd", 3.271266945412603E9),
      ("col_decimal_18_18_zstd", 0.06703282372899358),
      ("col_decimal_38_0_zstd", 3.304039482387018E15),
      ("col_decimal_38_37_zstd", 0.06668480569282172),
      ("col_float4_raw", 521.7677673600471),
      ("col_float4_bytedict", 537.9125584250828),
      ("col_float4_runlength", 530.8884428463581),
      ("col_float4_zstd", 522.0612461547584)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT VARIANCE (DISTINCT $column_name) FROM test_table"""),
        Seq(Row(expected_res)))
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

     inputList.foreach(test_case => {
       val column_name = test_case._1.toUpperCase
       val expected_res = test_case._2

       checkAnswer(
         sqlContext.sql(s"""SELECT VAR_SAMP ($column_name) FROM test_table"""),
         Seq(Row(expected_res)))
     })
   }

  test("var_samp operator - float8 type for DISTINCT values", PreloadTest) {

    // column name, expected var_samp result.
    // VAR_SAMP and VARIANCE are synonyms for the same function in Spark and Redshift.
    val inputList = List(
      ("col_float8_raw", 1221.1776036722786),
      ("col_float8_bytedict", 1204.4578596418667),
      ("col_float8_runlength", 1196.5140340964876),
      ("col_float8_zstd", 1199.706169292785)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT VARIANCE (DISTINCT $column_name) FROM test_table"""),
        Seq(Row(expected_res)))
    })
  }

  // Population Variance

  test("var_pop operator - ALL values", PreloadTest) {

    // int, small int, big int, decimal, float4 types.
    // column name, expected var_pop result.
    val inputList = List(
      ("col_smallint_raw", 13490.533365760004),
      ("col_smallint_bytedict", 13453.643213439995),
      ("col_smallint_delta", 13338.861582040028),
      ("col_smallint_lzo", 13371.106490239988),
      ("col_smallint_mostly8", 13363.539484639963),
      ("col_smallint_runlength", 13298.287975000007),
      ("col_smallint_zstd", 13477.436318039987),
      ("col_int_raw", 82889.31488956012),
      ("col_int_bytedict", 85507.72560796035),
      ("col_int_delta", 85133.16344955986),
      ("col_int_delta32k", 85440.43261824004),
      ("col_int_lzo", 83257.45705916006),
      ("col_int_mostly8", 84239.53722416007),
      ("col_int_mostly16", 84691.16992496006),
      ("col_int_runlength", 83157.20119855965),
      ("col_int_zstd", 84937.99864284012),
      ("col_bigint_raw", 3.2656512174633034E7),
      ("col_bigint_bytedict", 32786123.26422468),
      ("col_bigint_delta", 33376148.092371307),
      ("col_bigint_delta32k", 3.2725439786028657E7),
      ("col_bigint_lzo", 33387779.334261816),
      ("col_bigint_mostly8", 33428867.541886657),
      ("col_bigint_mostly16", 3.267879162655603E7),
      ("col_bigint_mostly32", 33594128.21711195),
      ("col_bigint_runlength", 3.302825205755058E7),
      ("col_bigint_zstd", 32904608.151732795),
      ("col_decimal_1_0_raw", 28.018728159999977),
      ("col_decimal_18_0_raw", 3.302278425004689E9),
      ("col_decimal_18_18_raw", 0.0685248908493695),
      ("col_decimal_38_0_raw", 3269637121696466.5),
      ("col_decimal_38_37_raw", 0.06805060747469724),
      ("col_decimal_1_0_bytedict", 28.358366040000057),
      ("col_decimal_18_0_bytedict", 3.3201335450238166E9),
      ("col_decimal_18_18_bytedict", 0.06719834170285428),
      ("col_decimal_38_0_bytedict", 3.270900925307131E15),
      ("col_decimal_38_37_bytedict", 0.06598156348818521),
      ("col_decimal_1_0_delta", 28.01563824000007),
      ("col_decimal_18_0_delta", 3319868236.896384),
      ("col_decimal_18_18_delta", 0.06858147980476603),
      ("col_decimal_38_0_delta", 3344568036090648L),
      ("col_decimal_38_37_delta", 0.0674315732782899),
      ("col_decimal_1_0_delta32k", 28.14928796000001),
      ("col_decimal_18_0_delta32k", 3291976435.119343),
      ("col_decimal_18_18_delta32k", 0.06849917773546312),
      ("col_decimal_38_0_delta32k", 3395455093110173.5),
      ("col_decimal_38_37_delta32k", 0.06703021654239777),
      ("col_decimal_1_0_lzo", 28.55247599999995),
      ("col_decimal_18_0_lzo", 3.363731740465674E9),
      ("col_decimal_18_18_lzo", 0.06624746192742113),
      ("col_decimal_38_0_lzo", 3281576871108532.5),
      ("col_decimal_38_37_lzo", 0.06606851981736434),
      ("col_decimal_1_0_mostly8", 28.594744160000044),
      ("col_decimal_18_0_mostly8", 3348654536.5904264),
      ("col_decimal_18_18_mostly8", 0.06710765773231375),
      ("col_decimal_38_0_mostly8", 3313712878220686L),
      ("col_decimal_38_37_mostly8", 0.06755332075475035),
      ("col_decimal_1_0_mostly16", 29.001198559999892),
      ("col_decimal_18_0_mostly16", 3292136628.5217004),
      ("col_decimal_18_18_mostly16", 0.06778749147823665),
      ("col_decimal_38_0_mostly16", 3318663610502384L),
      ("col_decimal_38_37_mostly16", 0.06762869611157901),
      ("col_decimal_1_0_mostly32", 28.26112635999996),
      ("col_decimal_18_0_mostly32", 3.3297332387622013E9),
      ("col_decimal_18_18_mostly32", 0.067506269641397),
      ("col_decimal_38_0_mostly32", 3335357707548977L),
      ("col_decimal_38_37_mostly32", 0.06664390682262138),
      ("col_decimal_1_0_runlength", 28.487470999999985),
      ("col_decimal_18_0_runlength", 3320027203.38863),
      ("col_decimal_18_18_runlength", 0.06763804579647503),
      ("col_decimal_38_0_runlength", 3.3204103609456175E15),
      ("col_decimal_38_37_runlength", 0.06806635042118071),
      ("col_decimal_1_0_zstd", 28.191916440000053),
      ("col_decimal_18_0_zstd", 3.2657043465451894E9),
      ("col_decimal_18_18_zstd", 0.06685430123416843),
      ("col_decimal_38_0_zstd", 3303378674490530L),
      ("col_decimal_38_37_zstd", 0.06667146873168332),
      ("col_float4_raw", 521.6093857968599),
      ("col_float4_bytedict", 537.8049759134018),
      ("col_float4_runlength", 530.7822651577878),
      ("col_float4_zstd", 522.0772064557543)
     )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT VAR_POP ($column_name) FROM test_table"""),
        Seq(Row(expected_res)))
    })
  }

  test("var_pop operator - DISTINCT values", PreloadTest) {

    // int, small int, big int, decimal, float4 types.
    // column name, expected var_pop result.
    val inputList = List(
      ("col_smallint_raw", 13399.999999999982),
      ("col_smallint_bytedict", 13399.99999999999),
      ("col_smallint_delta", 13400.000000000011),
      ("col_smallint_lzo", 13399.99999999999),
      ("col_smallint_mostly8", 13399.999999999995),
      ("col_smallint_runlength", 13399.999999999993),
      ("col_smallint_zstd", 13400.000000000007),
      ("col_int_raw", 83644.77279120065),
      ("col_int_bytedict", 83871.37835746497),
      ("col_int_delta", 83627.50491276388),
      ("col_int_delta32k", 83501.43858465977),
      ("col_int_lzo", 83866.70850454827),
      ("col_int_mostly8", 83507.61919707377),
      ("col_int_mostly16", 83520.25786176842),
      ("col_int_runlength", 83086.43197088799),
      ("col_int_zstd", 83486.4552753717),
      ("col_bigint_raw", 3.2835612213005017E7),
      ("col_bigint_bytedict", 3.29746550053763E7),
      ("col_bigint_delta", 3.3245179851075556E7),
      ("col_bigint_delta32k", 3.2825153419500828E7),
      ("col_bigint_lzo", 3.3425541809397656E7),
      ("col_bigint_mostly8", 3.3390508959142607E7),
      ("col_bigint_mostly16", 3.275487365938116E7),
      ("col_bigint_mostly32", 3.3652951790872715E7),
      ("col_bigint_runlength", 3.3139008449873533E7),
      ("col_bigint_zstd", 3.2815321019975897E7),
      ("col_decimal_1_0_raw", 29.999999999999993),
      ("col_decimal_18_0_raw", 3.3045844278548374E9),
      ("col_decimal_18_18_raw", 0.06884915876320973),
      ("col_decimal_38_0_raw", 3.269637121696473E15),
      ("col_decimal_38_37_raw", 0.06805060747469714),
      ("col_decimal_1_0_bytedict", 29.999999999999993),
      ("col_decimal_18_0_bytedict", 3.322283511914554E9),
      ("col_decimal_18_18_bytedict", 0.06725744403213073),
      ("col_decimal_38_0_bytedict", 3.2709009253071335E15),
      ("col_decimal_38_37_bytedict", 0.06598156348818515),
      ("col_decimal_1_0_delta", 29.999999999999993),
      ("col_decimal_18_0_delta", 3.316380702200494E9),
      ("col_decimal_18_18_delta", 0.06859971476434187),
      ("col_decimal_38_0_delta", 3.34456803609065E15),
      ("col_decimal_38_37_delta", 0.0674389412376964),
      ("col_decimal_1_0_delta32k", 29.999999999999993),
      ("col_decimal_18_0_delta32k", 3.2953068497598014E9),
      ("col_decimal_18_18_delta32k", 0.0685418733371686),
      ("col_decimal_38_0_delta32k", 3.39545509311017E15),
      ("col_decimal_38_37_delta32k", 0.067030216542398),
      ("col_decimal_1_0_lzo", 29.999999999999993),
      ("col_decimal_18_0_lzo", 3.364500367077978E9),
      ("col_decimal_18_18_lzo", 0.0664615995162818),
      ("col_decimal_38_0_lzo", 3.2815768711085425E15),
      ("col_decimal_38_37_lzo", 0.06606851981736417),
      ("col_decimal_1_0_mostly8", 29.999999999999993),
      ("col_decimal_18_0_mostly8", 3.3411738922839365E9),
      ("col_decimal_18_18_mostly8", 0.06720485047153617),
      ("col_decimal_38_0_mostly8", 3.313712878220684E15),
      ("col_decimal_38_37_mostly8", 0.06755332075475039),
      ("col_decimal_1_0_mostly16", 29.999999999999993),
      ("col_decimal_18_0_mostly16", 3.2937944049470625E9),
      ("col_decimal_18_18_mostly16", 0.06797931302492023),
      ("col_decimal_38_0_mostly16", 3.31866361050239E15),
      ("col_decimal_38_37_mostly16", 0.06762869611157908),
      ("col_decimal_1_0_mostly32", 29.999999999999993),
      ("col_decimal_18_0_mostly32", 3.327150715803562E9),
      ("col_decimal_18_18_mostly32", 0.06741095941133612),
      ("col_decimal_38_0_mostly32", 3.335357707548968E15),
      ("col_decimal_38_37_mostly32", 0.06664390682262128),
      ("col_decimal_1_0_runlength", 29.999999999999993),
      ("col_decimal_18_0_runlength", 3.3158825546205893E9),
      ("col_decimal_18_18_runlength", 0.06754504177208451),
      ("col_decimal_38_0_runlength", 3.3204103609456155E15),
      ("col_decimal_38_37_runlength", 0.06806635042118092),
      ("col_decimal_18_0_zstd", 3.2706054151910834E9),
      ("col_decimal_1_0_zstd", 29.999999999999993),
      ("col_decimal_18_18_zstd", 0.06701900252822472),
      ("col_decimal_38_0_zstd", 3.3033786744905405E15),
      ("col_decimal_38_37_zstd", 0.06667146873168314),
      ("col_float4_raw", 521.6633929316894),
      ("col_float4_bytedict", 537.8049759133978),
      ("col_float4_runlength", 530.7822651577889),
      ("col_float4_zstd", 521.9568130189002)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT VAR_POP (DISTINCT $column_name) FROM test_table"""),
        Seq(Row(expected_res)))
    })
  }

  test("var_pop operator - float8 type for ALL values", PreloadTest) {

    // column name, expected var_pop result
    val inputList = List(
      ("col_float8_raw", 1220.9333681515386),
      ("col_float8_bytedict", 1204.216968069941),
      ("col_float8_runlength", 1196.2747312896752),
      ("col_float8_zstd", 1199.4662280589275)
     )

     inputList.foreach(test_case => {
       val column_name = test_case._1.toUpperCase
       val expected_res = test_case._2

       checkAnswer(
         sqlContext.sql(s"""SELECT VAR_POP ($column_name) FROM test_table"""),
         Seq(Row(expected_res)))
     })
   }

  test("var_pop operator - float8 type for DISTINCT values", PreloadTest) {

    // column name, expected var_pop result
    val inputList = List(
      ("col_float8_raw", 1220.933368151544),
      ("col_float8_bytedict", 1204.2169680699383),
      ("col_float8_runlength", 1196.2747312896684),
      ("col_float8_zstd", 1199.4662280589264)
    )

    inputList.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT VAR_POP (DISTINCT $column_name) FROM test_table"""),
        Seq(Row(expected_res)))
    })
  }
}

class ParquetNoPushdownAggregateStatisticalOperatorsCorrectnessSuite
  extends NoPushdownAggregateStatisticalOperatorsCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}

class TextNoPushdownAggregateStatisticalOperatorsCorrectnessSuite
  extends NoPushdownAggregateStatisticalOperatorsCorrectnessSuite {
  override protected val s3format: String = "Text"
  override protected val auto_pushdown: String = "false"
}
