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

trait AggregateVarPopCorrectnessSuite extends IntegrationPushdownSuiteBase {
  test("var_pop operator - ALL values", PreloadTest) {
    // int, small int, big int, decimal, float4 types
    // column name, expected var_pop result.
    val inputList = List(
      (1E-6, "col_smallint_raw", 13490.533366),
      (1E-6, "col_smallint_bytedict", 13453.643213439995),
      (1E-6, "col_smallint_delta", 13338.861582040028),
      (1E-6, "col_smallint_lzo", 13371.106490239988),
      (1E-6, "col_smallint_mostly8", 13363.539484639967),
      (1E-6, "col_smallint_runlength", 13298.287975000007),
      (1E-6, "col_smallint_zstd", 13477.436318039987),
      (1E-6, "col_int_raw", 82889.31488956012),
      (1E-6, "col_int_bytedict", 85507.72560796037),
      (1E-6, "col_int_delta", 85133.16344955987),
      (1E-6, "col_int_delta32k", 85440.43261824004),
      (1E-6, "col_int_lzo", 83257.45705916005),
      (1E-6, "col_int_mostly8", 84239.53722416007),
      (1E-6, "col_int_mostly16", 84691.16992496006),
      (1E-6, "col_int_runlength", 83157.20119855965),
      (1E-6, "col_int_zstd", 84937.99864284012),
      (1E-6, "col_bigint_raw", 32656512.174633026),
      (1E-6, "col_bigint_bytedict", 32786123.26422468),
      (1E-6, "col_bigint_delta", 33376148.092371307),
      (1E-6, "col_bigint_delta32k", 32725439.78602865),
      (1E-6, "col_bigint_lzo", 33387779.334261816),
      (1E-6, "col_bigint_mostly8", 33428867.541886657),
      (1E-6, "col_bigint_mostly16", 32678791.626556035),
      (1E-6, "col_bigint_mostly32", 33594128.21711195),
      (1E-6, "col_bigint_runlength", 33028252.057550576),
      (1E-6, "col_bigint_zstd", 32904608.151732795),
      (1E-6, "col_decimal_18_0_raw", 3302278425.0046887),
      (1E-6, "col_decimal_18_18_raw", 0.06852489084936954),
      (1D, "col_decimal_38_0_raw", 3269637121696466.5),
      (1E-6, "col_decimal_38_37_raw", 0.06805060747469743),
      (1E-5, "col_decimal_18_0_bytedict", 3320133545.023816),
      (1E-6, "col_decimal_18_18_bytedict", 0.06719834170285426),
      (1D, "col_decimal_38_0_bytedict", 3270900925307131.5),
      (1E-6, "col_decimal_38_37_bytedict", 0.06598156348818524),
      (1E-6, "col_decimal_18_0_delta", 3319868236.896384),
      (1E-6, "col_decimal_18_18_delta", 0.06858147980476602),
      (1D, "col_decimal_38_0_delta", 3344568036090648D),
      (1E-6, "col_decimal_38_37_delta", 0.06743157327828986),
      (1E-6, "col_decimal_18_0_delta32k", 3291976435.119343),
      (1E-6, "col_decimal_18_18_delta32k", 0.06849917773546309),
      (1D, "col_decimal_38_0_delta32k", 3395455093110173.5),
      (1E-6, "col_decimal_38_37_delta32k", 0.06703021654239784),
      (1E-5, "col_decimal_18_0_lzo", 3363731740.4656744),
      (1E-6, "col_decimal_18_18_lzo", 0.06624746192742115),
      (1D, "col_decimal_38_0_lzo", 3281576871108532.5),
      (1E-6, "col_decimal_38_37_lzo", 0.06606851981736422),
      (1E-5, "col_decimal_18_0_mostly8", 3348654536.5904264),
      (1E-6, "col_decimal_18_18_mostly8", 0.06710765773231377),
      (1D, "col_decimal_38_0_mostly8", 3313712878220686D),
      (1E-6, "col_decimal_38_37_mostly8", 0.06755332075475039),
      (1E-5, "col_decimal_18_0_mostly16", 3292136628.5217004),
      (1E-6, "col_decimal_18_18_mostly16", 0.06778749147823668),
      (1D, "col_decimal_38_0_mostly16", 3318663610502384D),
      (1E-6, "col_decimal_38_37_mostly16", 0.06762869611157903),
      (1E-5, "col_decimal_18_0_mostly32", 3329733238.7622004),
      (1E-6, "col_decimal_18_18_mostly32", 0.06750626964139703),
      (1D, "col_decimal_38_0_mostly32", 3335357707548977D),
      (1E-6, "col_decimal_38_37_mostly32", 0.06664390682262138),
      (1E-6, "col_decimal_18_0_runlength", 3320027203.38863),
      (1E-6, "col_decimal_18_18_runlength", 0.06763804579647495),
      (1D, "col_decimal_38_0_runlength", 3320410360945618.5),
      (1E-6, "col_decimal_38_37_runlength", 0.0680663504211806),
      (1E-6, "col_decimal_18_0_zstd", 3265704346.545189),
      (1E-6, "col_decimal_18_18_zstd", 0.06685430123416841),
      (1D, "col_decimal_38_0_zstd", 3303378674490530D),
      (1E-6, "col_decimal_38_37_zstd", 0.06667146873168328),
      (1E-6, "col_float4_raw", 521.6093857968599),
      (1E-6, "col_float4_bytedict", 537.8049759134019),
      (1E-6, "col_float4_runlength", 530.7822651577877),
      (1E-6, "col_float4_zstd", 522.0772064557543),
      (1E-6, "col_decimal_1_0_zstd", 28.191916440000053),
      (1E-6, "col_decimal_1_0_raw", 28.018728159999977),
      (1E-6, "col_decimal_1_0_runlength", 28.487470999999985),
      (1E-6, "col_decimal_1_0_mostly32", 28.261126359999967),
      (1E-6, "col_decimal_1_0_mostly16", 29.001198559999892),
      (1E-6, "col_decimal_1_0_mostly8", 28.594744160000044),
      (1E-6, "col_decimal_1_0_lzo", 28.55247599999995),
      (1E-6, "col_decimal_1_0_delta32k", 28.149287960000017),
      (1E-6, "col_decimal_1_0_delta", 28.01563824000007),
      (1E-6, "col_decimal_1_0_bytedict", 28.358366040000057)
    )

    inputList.par.foreach(test_case => {
      val epsilon = test_case._1
      val column_name = test_case._2.toUpperCase
      val expected_res = test_case._3

      val lower = expected_res - epsilon
      val upper = expected_res + epsilon

      checkAnswer(
        sqlContext.sql(
          s"""SELECT (VAR_POP ($column_name) ) BETWEEN ${lower} and ${upper}
             |FROM test_table""".stripMargin),
        Seq(Row(true)))

      checkSqlStatement(
        s"""SELECT ( ( ( VAR_POP ( CAST ( "SQ_1"."SQ_1_COL_0" AS FLOAT8 ) )
           | >= ${lower} )
           | AND ( VAR_POP ( CAST ( "SQ_1"."SQ_1_COL_0" AS FLOAT8 ) ) <= ${upper} ) ) )
           | AS "SQ_2_COL_0"
           | FROM ( SELECT ( "SQ_0"."$column_name" ) AS "SQ_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
           | AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("var_pop operator - DISTINCT values", PreloadTest) {

    // int, small int, big int, decimal, float4 types.
    // column name, expected var_pop result.
    val inputList = List(
      (1E-6, "col_smallint_raw", 13400.000000000005),
      (1E-6, "col_smallint_bytedict", 13400.000000000005),
      (1E-6, "col_smallint_delta", 13399.999999999998),
      (1E-6, "col_smallint_lzo", 13400.000000000005),
      (1E-6, "col_smallint_mostly8", 13400D),
      (1E-6, "col_smallint_runlength", 13399.999999999998),
      (1E-6, "col_smallint_zstd", 13400D),
      (1E-6, "col_int_raw", 83644.77279120056),
      (1E-6, "col_int_bytedict", 83871.37835746503),
      (1E-6, "col_int_delta", 83627.50491276401),
      (1E-6, "col_int_delta32k", 83501.43858465995),
      (1E-6, "col_int_lzo", 83866.70850454844),
      (1E-6, "col_int_mostly8", 83507.61919707379),
      (1E-6, "col_int_mostly16", 83520.2578617683),
      (1E-6, "col_int_runlength", 83086.43197088814),
      (1E-6, "col_int_zstd", 83486.45527537177),
      (1E-6, "col_bigint_raw", 32835612.213005066),
      (1E-6, "col_bigint_bytedict", 32974655.00537623),
      (1E-6, "col_bigint_delta", 33245179.85107568),
      (1E-6, "col_bigint_delta32k", 32825153.41950082),
      (1E-6, "col_bigint_lzo", 33425541.809397765),
      (1E-6, "col_bigint_mostly8", 33390508.95914269),
      (1E-6, "col_bigint_mostly16", 32754873.659381256),
      (1E-6, "col_bigint_mostly32", 33652951.79087273),
      (1E-6, "col_bigint_runlength", 33139008.44987355),
      (1E-6, "col_bigint_zstd", 32815321.01997592),
      (1E-0, "col_decimal_18_0_raw", 3304584427.8548393),
      (1E-6, "col_decimal_18_18_raw", 0.0688491587632095),
      (1E1, "col_decimal_38_0_raw", 3269637121696472.5),
      (1E-6, "col_decimal_38_37_raw", 0.06805060747469743),
      (1E-0, "col_decimal_18_0_bytedict", 3322283511.9145517),
      (1E-6, "col_decimal_18_18_bytedict", 0.06725744403213094),
      (1E2, "col_decimal_38_0_bytedict", 3270900925307150D),
      (1E-6, "col_decimal_38_37_bytedict", 0.06598156348818528),
      (1E-0, "col_decimal_18_0_delta", 3316380702.2005057),
      (1E-6, "col_decimal_18_18_delta", 0.06859971476434179),
      (1E1, "col_decimal_38_0_delta", 3344568036090649.5),
      (1E-6, "col_decimal_38_37_delta", 0.06743894123769659),
      (1E-0, "col_decimal_18_0_delta32k", 3295306849.7598033),
      (1E-6, "col_decimal_18_18_delta32k", 0.0685418733371686),
      (1E1, "col_decimal_38_0_delta32k", 3395455093110168D),
      (1E-6, "col_decimal_38_37_delta32k", 0.06703021654239807),
      (1E-0, "col_decimal_18_0_lzo", 3364500367.0779805),
      (1E-6, "col_decimal_18_18_lzo", 0.06646159951628154),
      (1E2, "col_decimal_38_0_lzo", 3281576871108528.5),
      (1E-6, "col_decimal_38_37_lzo", 0.06606851981736425),
      (1E-0, "col_decimal_18_0_mostly8", 3341173892.2839465),
      (1E-6, "col_decimal_18_18_mostly8", 0.0672048504715364),
      (1E1, "col_decimal_38_0_mostly8", 3313712878220690D),
      (1E-6, "col_decimal_38_37_mostly8", 0.06755332075475022),
      (1E-0, "col_decimal_18_0_mostly16", 3293794404.9470677),
      (1E-6, "col_decimal_18_18_mostly16", 0.06797931302492023),
      (1E1, "col_decimal_38_0_mostly16", 3318663610502394.5),
      (1E-6, "col_decimal_38_37_mostly16", 0.06762869611157911),
      (1E-0, "col_decimal_18_0_mostly32", 3327150715.8035603),
      (1E-6, "col_decimal_18_18_mostly32", 0.06741095941133596),
      (1E1, "col_decimal_38_0_mostly32", 3335357707548976.5),
      (1E-6, "col_decimal_38_37_mostly32", 0.06664390682262139),
      (1E-0, "col_decimal_18_0_runlength", 3315882554.6205897),
      (1E-6, "col_decimal_18_18_runlength", 0.06754504177208444),
      (1E1, "col_decimal_38_0_runlength", 3320410360945609.5),
      (1E-6, "col_decimal_38_37_runlength", 0.0680663504211806),
      (1E-0, "col_decimal_18_0_zstd", 3270605415.1910906),
      (1E-6, "col_decimal_18_18_zstd", 0.06701900252822472),
      (1E1, "col_decimal_38_0_zstd", 3303378674490530.5),
      (1E-6, "col_decimal_38_37_zstd", 0.06667146873168309),
      (1E-6, "col_float4_raw", 521.6633929316899),
      (1E-6, "col_float4_bytedict", 537.8049759134005),
      (1E-6, "col_float4_runlength", 530.782265157789),
      (1E-6, "col_float4_zstd", 521.9568130188982),
      (1E-6, "col_decimal_1_0_raw", 30D),
      (1E-6, "col_decimal_1_0_bytedict", 29.999999999999993),
      (1E-6, "col_decimal_1_0_delta", 29.999999999999993),
      (1E-6, "col_decimal_1_0_delta32k", 29.999999999999993),
      (1E-6, "col_decimal_1_0_lzo", 29.999999999999993),
      (1E-6, "col_decimal_1_0_mostly8", 30D),
      (1E-6, "col_decimal_1_0_mostly16", 29.999999999999993),
      (1E-6, "col_decimal_1_0_mostly32", 29.999999999999993),
      (1E-6, "col_decimal_1_0_runlength", 29.999999999999993),
      (1E-6, "col_decimal_1_0_zstd", 30D)
    )

    inputList.par.foreach(test_case => {
      val epsilon = test_case._1
      val column_name = test_case._2.toUpperCase
      val expected_res = test_case._3

      val lower = expected_res - epsilon
      val upper = expected_res + epsilon

      checkAnswer(
        sqlContext.sql(
          s"""SELECT VAR_POP (DISTINCT $column_name) BETWEEN ${lower} AND ${upper}
             | FROM test_table""".stripMargin),
        Seq(Row(true)))

      checkSqlStatement(
        s"""SELECT ( ( ( VAR_POP ( DISTINCT CAST ( "SQ_1"."SQ_1_COL_0" AS FLOAT8 ) )
           | >= ${lower} )
           | AND ( VAR_POP ( DISTINCT CAST ( "SQ_1"."SQ_1_COL_0" AS FLOAT8 ) )
           | <= ${upper} ) ) )
           | AS "SQ_2_COL_0"
           | FROM ( SELECT ( "SQ_0"."$column_name" ) AS "SQ_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
           | AS "SQ_1" LIMIT 1""".stripMargin)
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

    inputList.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(s"""SELECT VAR_POP ($column_name) FROM test_table"""),
        Seq(Row(expected_res)))

      checkSqlStatement(
        s"""SELECT ( VAR_POP ("SQ_1"."SQ_1_COL_0") )
           | AS "SQ_2_COL_0"
           | FROM ( SELECT ( "SQ_0"."$column_name" ) AS "SQ_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
           | AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }

  test("var_pop operator - float8 type for DISTINCT values", PreloadTest) {
    val epsilon = 1E-6

    // column name, expected var_pop result.
    val inputList = List(
      ("col_float8_raw", 1220.933368151543),
      ("col_float8_bytedict", 1204.2169680699405),
      ("col_float8_runlength", 1196.2747312896736),
      ("col_float8_zstd", 1199.4662280589228)
    )

    inputList.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      val lower = expected_res - epsilon
      val upper = expected_res + epsilon

      checkAnswer(
        sqlContext.sql(
          s"""SELECT VAR_POP (DISTINCT $column_name) BETWEEN ${lower} and ${upper}
             | FROM test_table""".stripMargin),
        Seq(Row(true)))

      checkSqlStatement(
        s"""SELECT ( ( ( VAR_POP ( DISTINCT "SQ_1"."SQ_1_COL_0" ) >= ${lower} )
           | AND ( VAR_POP ( DISTINCT "SQ_1"."SQ_1_COL_0" ) <= ${upper} ) ) )
           | AS "SQ_2_COL_0"
           | FROM ( SELECT ( "SQ_0"."$column_name" ) AS "SQ_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RCQ_ALIAS" ) AS "SQ_0" )
           | AS "SQ_1" LIMIT 1""".stripMargin)
    })
  }
}
