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
import org.apache.spark.sql.types.{IntegerType, MapType, StringType, StructField, StructType}

import java.sql.{Date, Timestamp}

abstract class PushdownLogicalPlanOperatorSuite extends IntegrationPushdownSuiteBase {
  private val test_table_2: String = s""""PUBLIC"."pushdown_suite_test_table2_$randomSuffix""""
  override def beforeAll(): Unit = {
    super.beforeAll()
    if (!preloaded_data.toBoolean) {
      redshiftWrapper.executeUpdate(conn, s"drop table if exists $test_table_2")
      createMoreDataInRedshift(test_table_2)
    }
  }

  override def afterAll(): Unit = {
    try {
      if (!preloaded_data.toBoolean) {
        redshiftWrapper.executeUpdate(conn, s"drop table if exists $test_table_2")
      }
    } finally {
      super.afterAll()
    }
  }
  override def beforeEach(): Unit = {
    super.beforeEach()

    read
      .option("dbtable", test_table_2)
      .load()
      .createOrReplaceTempView("test_table_2")
  }

  protected def createMoreDataInRedshift(tableName: String): Unit = {
    redshiftWrapper.executeUpdate(conn,
      s"""
         |create table $tableName (
         |testbyte int2,
         |testbool boolean,
         |testdate date,
         |testdouble float8,
         |testfloat float4,
         |testint int4,
         |testlong int8,
         |testshort int2,
         |teststring varchar(256),
         |testtimestamp timestamp
         |)
      """.stripMargin
    )
    // scalastyle:off
    redshiftWrapper.executeUpdate(conn,
      s"""
         |insert into $tableName values
         |(null, null, null, null, null, null, null, null, null, null),
         |(0, null, '2015-07-02', 0.0, -1.0, 216, 54321, null, 'f', '2015-07-03 12:34:56.000'),
         |(1, false, null, -1234152.12312498, 100.0, null, 1239012341823719, 24, '___|_123', null),
         |(null, true, '2015-07-01', 12345.12345678, 55.12, 365, 1239012341823716, 56, '_____', '2016-07-07 07:07:07'),
         |(2, false, '2015-07-03', 1.1, 0.0, 42, 1239012341823715, -13, 'asdf', '2015-07-02 00:00:00.000'),
         |(42, true, '2015-07-05', 2.2, 5.0, 45, 1239012341823718, 23, 'acbdef', '2015-07-03 12:34:56.000'),
         |(3, true, '2015-07-04', 1234152.12312498, 2.0, 42, 1239012341823717, 23, 'Unicode''s樂趣', '2015-07-01 00:00:00.001')
         """.stripMargin
    )
    // scalastyle:on
  }

  test("Test JOIN logical plan operator", P1Test) {
    // "Column name" and result set
    val input = List(
      ("testbyte", Seq(Row(0), Row(0), Row(1), Row(1))),
      ("testbool", Seq(Row(false), Row(false), Row(false), Row(false),
        Row(true), Row(true), Row(true))),
      ("testdate", Seq(Row(Date.valueOf("2015-07-01")), Row(Date.valueOf("2015-07-02")),
        Row(Date.valueOf("2015-07-03")))),
      ("testint", Seq(Row(42), Row(42), Row(42), Row(42))),
      ("testlong", Seq(Row(1239012341823719L), Row(1239012341823719L), Row(1239012341823719L),
        Row(1239012341823719L))),
      ("testshort", Seq(Row(-13), Row(23), Row(23), Row(24))),
      ("teststring", Seq(Row("Unicode's樂趣"), Row("___|_123"), Row("asdf"), Row("f"))),
      ("testtimestamp", Seq(Row(Timestamp.valueOf("2015-07-01 00:00:00.001")),
        Row(Timestamp.valueOf("2015-07-02 00:00:00")),
        Row(Timestamp.valueOf("2015-07-03 12:34:56")),
        Row(Timestamp.valueOf("2015-07-03 12:34:56"))))
    )
    input.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT test_table_2.$column_name FROM test_table JOIN test_table_2
             | ON test_table.$column_name = test_table_2.$column_name order by 1""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT * FROM ( SELECT ( "SUBQUERY_6"."SUBQUERY_6_COL_1" ) AS "SUBQUERY_7_COL_0"
           | FROM ( SELECT ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) AS "SUBQUERY_6_COL_0",
           | ( "SUBQUERY_5"."SUBQUERY_5_COL_0" ) AS "SUBQUERY_6_COL_1"
           | FROM (SELECT ( "SUBQUERY_1"."$column_name" ) AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
           | AS "SUBQUERY_0" WHERE ( "SUBQUERY_0"."$column_name" IS NOT NULL ) ) AS"SUBQUERY_1" )
           | AS "SUBQUERY_2" INNER JOIN ( SELECT ( "SUBQUERY_4"."$column_name" )
           | AS "SUBQUERY_5_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table_2
           | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_3" WHERE
           | ( "SUBQUERY_3"."$column_name" IS NOT NULL ) ) AS "SUBQUERY_4" ) AS "SUBQUERY_5"
           | ON ( "SUBQUERY_2"."SUBQUERY_2_COL_0" = "SUBQUERY_5"."SUBQUERY_5_COL_0" ) )
           | AS "SUBQUERY_6" ) AS "SUBQUERY_7" ORDER BY ( "SUBQUERY_7"."SUBQUERY_7_COL_0" ) ASC
           | NULLS FIRST""".stripMargin)
    })
  }

  // DISTINCT on joined result cannot be pushed down.
  test("Test JOIN logical plan operator with DISTINCT") {
    // "Column name" and result set
    val input = List(
      ("testbyte", Seq(Row(0), Row(1))),
      ("testbool", Seq(Row(false), Row(true))),
      ("testdate", Seq(Row(Date.valueOf("2015-07-01")), Row(Date.valueOf("2015-07-02")),
        Row(Date.valueOf("2015-07-03")))),
      ("testint", Seq(Row(42))),
      ("testlong", Seq(Row(1239012341823719L))),
      ("testshort", Seq(Row(-13), Row(23), Row(24))),
      ("teststring", Seq(Row("Unicode's樂趣"), Row("___|_123"), Row("asdf"), Row("f"))),
      ("testtimestamp", Seq(Row(Timestamp.valueOf("2015-07-01 00:00:00.001")),
        Row(Timestamp.valueOf("2015-07-02 00:00:00")),
        Row(Timestamp.valueOf("2015-07-03 12:34:56"))))
    )
    input.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT DISTINCT test_table_2.$column_name FROM test_table JOIN test_table_2
             | ON test_table.$column_name = test_table_2.$column_name order by 1""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT * FROM ( SELECT ( "SUBQUERY_7"."SUBQUERY_7_COL_0" ) AS "SUBQUERY_8_COL_0"
           | FROM ( SELECT ( "SUBQUERY_6"."SUBQUERY_6_COL_1" ) AS "SUBQUERY_7_COL_0"
           | FROM ( SELECT ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) AS "SUBQUERY_6_COL_0",
           | ( "SUBQUERY_5"."SUBQUERY_5_COL_0" ) AS "SUBQUERY_6_COL_1" FROM (
           | SELECT ( "SUBQUERY_1"."$column_name" ) AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM (
           | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
           | WHERE ( "SUBQUERY_0"."$column_name" IS NOT NULL ) ) AS "SUBQUERY_1" ) AS "SUBQUERY_2"
           | INNER JOIN ( SELECT ( "SUBQUERY_4"."$column_name" ) AS "SUBQUERY_5_COL_0" FROM (
           | SELECT * FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" )
           | AS "SUBQUERY_3" WHERE ( "SUBQUERY_3"."$column_name" IS NOT NULL ) ) AS "SUBQUERY_4" )
           | AS "SUBQUERY_5" ON ( "SUBQUERY_2"."SUBQUERY_2_COL_0" = "SUBQUERY_5"."SUBQUERY_5_COL_0" ) )
           | AS "SUBQUERY_6" ) AS "SUBQUERY_7" GROUP BY "SUBQUERY_7"."SUBQUERY_7_COL_0" )
           | AS "SUBQUERY_8" ORDER BY ( "SUBQUERY_8"."SUBQUERY_8_COL_0" ) ASC
           | NULLS FIRST""".stripMargin)
    })
  }

  test("Test INNER JOIN logical plan operator") {
    // "Column name" and result set
    val input = List(
      ("testbyte", Seq(Row(0), Row(0), Row(1), Row(1))),
      ("testbool", Seq(Row(false), Row(false), Row(false), Row(false),
        Row(true), Row(true), Row(true))),
      ("testdate", Seq(Row(Date.valueOf("2015-07-01")), Row(Date.valueOf("2015-07-02")),
        Row(Date.valueOf("2015-07-03")))),
      ("testint", Seq(Row(42), Row(42), Row(42), Row(42))),
      ("testlong", Seq(Row(1239012341823719L), Row(1239012341823719L), Row(1239012341823719L),
        Row(1239012341823719L))),
      ("testshort", Seq(Row(-13), Row(23), Row(23), Row(24))),
      ("teststring", Seq(Row("Unicode's樂趣"), Row("___|_123"), Row("asdf"), Row("f"))),
      ("testtimestamp", Seq(Row(Timestamp.valueOf("2015-07-01 00:00:00.001")),
        Row(Timestamp.valueOf("2015-07-02 00:00:00")),
        Row(Timestamp.valueOf("2015-07-03 12:34:56")),
        Row(Timestamp.valueOf("2015-07-03 12:34:56"))))
    )
    input.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT test_table_2.$column_name FROM test_table INNER JOIN test_table_2
             | ON test_table.$column_name = test_table_2.$column_name order by 1""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT * FROM ( SELECT ( "SUBQUERY_6"."SUBQUERY_6_COL_1" ) AS "SUBQUERY_7_COL_0"
           | FROM ( SELECT ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) AS "SUBQUERY_6_COL_0",
           | ( "SUBQUERY_5"."SUBQUERY_5_COL_0" ) AS "SUBQUERY_6_COL_1"
           | FROM (SELECT ( "SUBQUERY_1"."$column_name" ) AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
           | AS "SUBQUERY_0" WHERE ( "SUBQUERY_0"."$column_name" IS NOT NULL ) ) AS"SUBQUERY_1" )
           | AS "SUBQUERY_2" INNER JOIN ( SELECT ( "SUBQUERY_4"."$column_name" )
           | AS "SUBQUERY_5_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table_2
           | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_3" WHERE
           | ( "SUBQUERY_3"."$column_name" IS NOT NULL ) ) AS "SUBQUERY_4" ) AS "SUBQUERY_5"
           | ON ( "SUBQUERY_2"."SUBQUERY_2_COL_0" = "SUBQUERY_5"."SUBQUERY_5_COL_0" ) )
           | AS "SUBQUERY_6" ) AS "SUBQUERY_7" ORDER BY ( "SUBQUERY_7"."SUBQUERY_7_COL_0" ) ASC
           | NULLS FIRST""".stripMargin)
    })
  }

  test("Test CROSS JOIN logical plan operator") {

    val testByteSeq = Seq(0, 1, 2, 3, 42, null, null).flatMap
    { e1 => Seq(0, 0, 1, 1, null).map(e2 => Row(e1, e2)) }

    val testBoolSeq = Seq(null, null, false, false, true, true, true).flatMap
    { e1 => Seq(null, null, true, false, false).map(e2 => Row(e1, e2))}

    val testDateSeq = Seq(null, null, Date.valueOf("2015-07-01"), Date.valueOf("2015-07-02"),
      Date.valueOf("2015-07-03"), Date.valueOf("2015-07-04"), Date.valueOf("2015-07-05")).
      flatMap { e1 =>
        Seq(null, null, Date.valueOf("2015-07-01"), Date.valueOf("2015-07-02"),
          Date.valueOf("2015-07-03")).map(e2 => Row(e1, e2))
      }

    val testIntSeq = Seq(216, 365, 42, 42, 45, null, null).flatMap
    {e1 => Seq(4141214, 42, 42, null, null).map(e2 => Row(e1, e2))}

    val testLongSeq = Seq(1239012341823715L, 1239012341823716L, 1239012341823717L,
      1239012341823718L, 1239012341823719L, 54321L, null).flatMap
    {e1 => Seq(1239012341823719L, 1239012341823719L, 1239012341823719L, 1239012341823719L, null).
      map(e2 => Row(e1, e2))}

    val testShortSeq = Seq(-13, 23, 23, 24, 56, null, null).flatMap
    {e1 => Seq(-13, 23, 24, null, null).map(e2 => Row(e1, e2))}

    // scalastyle:off
    val testStringSeq = Seq("Unicode's樂趣", "_____", "___|_123", "acbdef", "asdf", "f", null).flatMap
    {e1 => Seq("Unicode's樂趣", "___|_123", "asdf", "f", null).map(e2 => Row(e1, e2))}
    // scalastyle:on

    val testTimestampSeq = (Seq("2015-07-01 00:00:00.001", "2015-07-02 00:00:00.0",
      "2015-07-03 12:34:56.0", "2015-07-03 12:34:56.0", "2016-07-07 07:07:07.0").
      map(Timestamp.valueOf) ++ Seq(null, null)).flatMap
    {e1 => (Seq("2015-07-01 00:00:00.001", "2015-07-02 00:00:00.0", "2015-07-03 12:34:56.0").
      map(Timestamp.valueOf) ++ Seq(null, null)).map(e2 => Row(e1, e2))}

    val input = List(
      ("TESTBYTE", testByteSeq),
      ("TESTBOOL", testBoolSeq),
      ("TESTDATE", testDateSeq),
      ("TESTINT", testIntSeq),
      ("TESTLONG", testLongSeq),
      ("TESTSHORT", testShortSeq),
      ("TESTSTRING", testStringSeq),
      ("TESTTIMESTAMP", testTimestampSeq)
    )

    input.par.foreach { case (column_name, expected_res) =>
      val df = sqlContext.sql(
        s"""select test_table_2.${column_name}, test_table.${column_name}
           |FROM test_table_2 cross JOIN test_table""".stripMargin)
      checkAnswer(df, expected_res)

      checkSqlStatement(
        s"""SELECT ( "SUBQUERY_4"."SUBQUERY_4_COL_0" ) AS "SUBQUERY_5_COL_0",
           |( "SUBQUERY_4"."SUBQUERY_4_COL_1" ) AS "SUBQUERY_5_COL_1" FROM
           |( SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_4_COL_0",
           |( "SUBQUERY_3"."SUBQUERY_3_COL_0" ) AS "SUBQUERY_4_COL_1" FROM
           |( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           |FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" )
           |AS "SUBQUERY_0" ) AS "SUBQUERY_1" CROSS JOIN
           |( SELECT ( "SUBQUERY_2"."$column_name" ) AS "SUBQUERY_3_COL_0" FROM
           |( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_2" )
           |AS "SUBQUERY_3" ) AS "SUBQUERY_4"""".stripMargin)
    }
  }

  test("Test implicit CROSS JOIN logical plan operator") {

    val testByteSeq = Seq(0, 1, 2, 3, 42, null, null).flatMap
    { e1 => Seq(0, 0, 1, 1, null).map(e2 => Row(e1, e2)) }

    val testBoolSeq = Seq(null, null, false, false, true, true, true).flatMap
    { e1 => Seq(null, null, true, false, false).map(e2 => Row(e1, e2))}

    val testDateSeq = Seq(null, null, Date.valueOf("2015-07-01"), Date.valueOf("2015-07-02"),
      Date.valueOf("2015-07-03"), Date.valueOf("2015-07-04"), Date.valueOf("2015-07-05")).
      flatMap { e1 =>
        Seq(null, null, Date.valueOf("2015-07-01"), Date.valueOf("2015-07-02"),
          Date.valueOf("2015-07-03")).map(e2 => Row(e1, e2))
      }

    val testIntSeq = Seq(216, 365, 42, 42, 45, null, null).flatMap
    {e1 => Seq(4141214, 42, 42, null, null).map(e2 => Row(e1, e2))}

    val testLongSeq = Seq(1239012341823715L, 1239012341823716L, 1239012341823717L,
      1239012341823718L, 1239012341823719L, 54321L, null).flatMap
    {e1 => Seq(1239012341823719L, 1239012341823719L, 1239012341823719L, 1239012341823719L, null).
      map(e2 => Row(e1, e2))}

    val testShortSeq = Seq(-13, 23, 23, 24, 56, null, null).flatMap
    {e1 => Seq(-13, 23, 24, null, null).map(e2 => Row(e1, e2))}

    // scalastyle:off
    val testStringSeq = Seq("Unicode's樂趣", "_____", "___|_123", "acbdef", "asdf", "f", null).flatMap
    {e1 => Seq("Unicode's樂趣", "___|_123", "asdf", "f", null).map(e2 => Row(e1, e2))}
    // scalastyle:on

    val testTimestampSeq = (Seq("2015-07-01 00:00:00.001", "2015-07-02 00:00:00.0",
      "2015-07-03 12:34:56.0", "2015-07-03 12:34:56.0", "2016-07-07 07:07:07.0").
      map(Timestamp.valueOf) ++ Seq(null, null)).flatMap
    {e1 => (Seq("2015-07-01 00:00:00.001", "2015-07-02 00:00:00.0", "2015-07-03 12:34:56.0").
      map(Timestamp.valueOf) ++ Seq(null, null)).map(e2 => Row(e1, e2))}

    val input = List(
      ("TESTBYTE", testByteSeq),
      ("TESTBOOL", testBoolSeq),
      ("TESTDATE", testDateSeq),
      ("TESTINT", testIntSeq),
      ("TESTLONG", testLongSeq),
      ("TESTSHORT", testShortSeq),
      ("TESTSTRING", testStringSeq),
      ("TESTTIMESTAMP", testTimestampSeq)
    )

    input.par.foreach { case (column_name, expected_res) =>
      val df = sqlContext.sql(
        s"""select test_table_2.${column_name}, test_table.${column_name}
           |FROM test_table_2, test_table""".stripMargin)
      checkAnswer(df, expected_res)

      checkSqlStatement(
        s"""SELECT ( "SUBQUERY_4"."SUBQUERY_4_COL_0" ) AS "SUBQUERY_5_COL_0",
           |( "SUBQUERY_4"."SUBQUERY_4_COL_1" ) AS "SUBQUERY_5_COL_1" FROM
           |( SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_4_COL_0",
           |( "SUBQUERY_3"."SUBQUERY_3_COL_0" ) AS "SUBQUERY_4_COL_1" FROM
           |( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           |FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" )
           |AS "SUBQUERY_0" ) AS "SUBQUERY_1" CROSS JOIN
           |( SELECT ( "SUBQUERY_2"."$column_name" ) AS "SUBQUERY_3_COL_0" FROM
           |( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_2" )
           |AS "SUBQUERY_3" ) AS "SUBQUERY_4"""".stripMargin)
    }
  }

  test("Test CROSS JOIN with duplicate column names and complex types") {
    val schema1 = StructType(StructField("a", MapType(StringType, IntegerType))::Nil)
    val schema2 = StructType(StructField("a", IntegerType)::Nil)
    val df1 = read.option("query", """select '{"number": 1}' as a""").schema(schema1).load
    val df2 = read.option("query", """select 1 as a""").schema(schema2).load

    val df = df1.crossJoin(df2)
    checkAnswer(df, Seq(Row(Map("number" -> 1), 1)))

    checkSqlStatement(
      """SELECT ( "SUBQUERY_0"."A" ) AS "SUBQUERY_2_COL_0" ,
        |( "SUBQUERY_1"."A" ) AS "SUBQUERY_2_COL_1"
        |FROM ( SELECT * FROM ( select \'{"number": 1}\' as a ) AS "RS_CONNECTOR_QUERY_ALIAS" )
        |AS "SUBQUERY_0" CROSS JOIN ( SELECT * FROM ( select 1 as a ) AS "RS_CONNECTOR_QUERY_ALIAS" )
        |AS "SUBQUERY_1" """.stripMargin)
  }

  test("Test FULL OUTER JOIN logical plan operator") {
    // "Column name" and result set
    val input = List(
      ("testbyte", Seq(Row(null), Row(null), Row(null), Row(0), Row(0), Row(1), Row(1),
        Row(2), Row(3), Row(42))),
      ("testbool", Seq(Row(null), Row(null), Row(null), Row(null), Row(false), Row(false),
        Row(false), Row(false), Row(true), Row(true), Row(true))),
      ("testdate", Seq(Row(null), Row(null), Row(null), Row(null),
        Row(Date.valueOf("2015-07-01")), Row(Date.valueOf("2015-07-02")),
        Row(Date.valueOf("2015-07-03")), Row(Date.valueOf("2015-07-04")),
        Row(Date.valueOf("2015-07-05")))),
      ("testint", Seq(Row(null), Row(null), Row(null), Row(null), Row(null), Row(42), Row(42),
        Row(42), Row(42), Row(45), Row(216), Row(365))),
      ("testlong", Seq(Row(null), Row(null), Row(54321), Row(1239012341823715L),
        Row(1239012341823716L), Row(1239012341823717L), Row(1239012341823718L),
        Row(1239012341823719L), Row(1239012341823719L), Row(1239012341823719L),
        Row(1239012341823719L))),
      ("testshort", Seq(Row(null), Row(null), Row(null), Row(null), Row(-13), Row(23), Row(23),
        Row(24), Row(56))),
      ("teststring", Seq(Row(null), Row(null), Row("Unicode's樂趣"), Row("_____"), Row("___|_123"),
        Row("acbdef"), Row("asdf"), Row("f"))),
      ("testtimestamp", Seq(Row(null), Row(null), Row(null), Row(null),
        Row(Timestamp.valueOf("2015-07-01 00:00:00.001")),
        Row(Timestamp.valueOf("2015-07-02 00:00:00")),
        Row(Timestamp.valueOf("2015-07-03 12:34:56")),
        Row(Timestamp.valueOf("2015-07-03 12:34:56")),
        Row(Timestamp.valueOf("2016-07-07 07:07:07"))))
    )
    input.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT test_table_2.$column_name FROM test_table FULL OUTER JOIN test_table_2
             | ON test_table.$column_name = test_table_2.$column_name order by 1""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT * FROM ( SELECT ( "SUBQUERY_4"."SUBQUERY_4_COL_1" ) AS "SUBQUERY_5_COL_0"
           | FROM ( SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_4_COL_0",
           | ( "SUBQUERY_3"."SUBQUERY_3_COL_0" ) AS "SUBQUERY_4_COL_1"
           | FROM ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
           | AS "SUBQUERY_0" ) AS "SUBQUERY_1" FULL OUTER JOIN ( SELECT ( "SUBQUERY_2"."$column_name" )
           | AS "SUBQUERY_3_COL_0" FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" )
           | AS "SUBQUERY_2" ) AS "SUBQUERY_3" ON
           | ( "SUBQUERY_1"."SUBQUERY_1_COL_0" = "SUBQUERY_3"."SUBQUERY_3_COL_0" ) ) AS "SUBQUERY_4" )
           | AS "SUBQUERY_5" ORDER BY ( "SUBQUERY_5"."SUBQUERY_5_COL_0" ) ASC NULLS FIRST
           | """.stripMargin)
    })
  }

  test("Test LEFT OUTER JOIN logical plan operator") {
    // "Column name" and result set
    val input = List(
      ("testbyte", Seq(Row(null), Row(0), Row(0), Row(1), Row(1))),
      ("testbool", Seq(Row(null), Row(null), Row(false), Row(false), Row(false), Row(false),
        Row(true), Row(true), Row(true))),
      ("testdate", Seq(Row(null), Row(null),
        Row(Date.valueOf("2015-07-01")), Row(Date.valueOf("2015-07-02")),
        Row(Date.valueOf("2015-07-03")))),
      ("testint", Seq(Row(null), Row(null), Row(null), Row(42), Row(42), Row(42), Row(42))),
      ("testlong", Seq(Row(null), Row(1239012341823719L), Row(1239012341823719L),
        Row(1239012341823719L), Row(1239012341823719L))),
      ("testshort", Seq(Row(null), Row(null), Row(-13), Row(23), Row(23), Row(24))),
      ("teststring", Seq(Row(null), Row("Unicode's樂趣"), Row("___|_123"), Row("asdf"), Row("f"))),
      ("testtimestamp", Seq(Row(null), Row(null),
        Row(Timestamp.valueOf("2015-07-01 00:00:00.001")),
        Row(Timestamp.valueOf("2015-07-02 00:00:00")),
        Row(Timestamp.valueOf("2015-07-03 12:34:56")),
        Row(Timestamp.valueOf("2015-07-03 12:34:56"))))
    )
    input.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT test_table_2.$column_name FROM test_table LEFT OUTER JOIN test_table_2
             | ON test_table.$column_name = test_table_2.$column_name order by 1""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT * FROM ( SELECT ( "SUBQUERY_5"."SUBQUERY_5_COL_1" ) AS "SUBQUERY_6_COL_0"
           | FROM ( SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_5_COL_0",
           | ( "SUBQUERY_4"."SUBQUERY_4_COL_0" ) AS "SUBQUERY_5_COL_1"
           | FROM ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | AS "SUBQUERY_1" LEFT OUTER JOIN ( SELECT ( "SUBQUERY_3"."$column_name" )
           | AS "SUBQUERY_4_COL_0" FROM ( SELECT * FROM ( SELECT * FROM $test_table_2
           | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_2" WHERE
           | ( "SUBQUERY_2"."$column_name" IS NOT NULL ) ) AS "SUBQUERY_3" ) AS "SUBQUERY_4"
           | ON ( "SUBQUERY_1"."SUBQUERY_1_COL_0" = "SUBQUERY_4"."SUBQUERY_4_COL_0" ) )
           | AS "SUBQUERY_5" ) AS "SUBQUERY_6" ORDER BY ( "SUBQUERY_6"."SUBQUERY_6_COL_0" )
           | ASC NULLS FIRST""".stripMargin)
    })
  }

  test("Test RIGHT OUTER JOIN logical plan operator") {
    // "Column name" and result set
    val input = List(
      ("testbyte", Seq(Row(null), Row(null), Row(0), Row(0), Row(1), Row(1),
        Row(2), Row(3), Row(42))),
      ("testbool", Seq(Row(null), Row(null), Row(false), Row(false), Row(false), Row(false),
        Row(true), Row(true), Row(true))),
      ("testdate", Seq(Row(null), Row(null),
        Row(Date.valueOf("2015-07-01")), Row(Date.valueOf("2015-07-02")),
        Row(Date.valueOf("2015-07-03")), Row(Date.valueOf("2015-07-04")),
        Row(Date.valueOf("2015-07-05")))),
      ("testint", Seq(Row(null), Row(null), Row(42), Row(42), Row(42), Row(42), Row(45),
        Row(216), Row(365))),
      ("testlong", Seq(Row(null), Row(54321), Row(1239012341823715L),
        Row(1239012341823716L), Row(1239012341823717L), Row(1239012341823718L),
        Row(1239012341823719L), Row(1239012341823719L), Row(1239012341823719L),
        Row(1239012341823719L))),
      ("testshort", Seq(Row(null), Row(null), Row(-13), Row(23), Row(23), Row(24), Row(56))),
      ("teststring", Seq(Row(null), Row("Unicode's樂趣"), Row("_____"), Row("___|_123"),
        Row("acbdef"), Row("asdf"), Row("f"))),
      ("testtimestamp", Seq(Row(null), Row(null),
        Row(Timestamp.valueOf("2015-07-01 00:00:00.001")),
        Row(Timestamp.valueOf("2015-07-02 00:00:00")),
        Row(Timestamp.valueOf("2015-07-03 12:34:56")),
        Row(Timestamp.valueOf("2015-07-03 12:34:56")),
        Row(Timestamp.valueOf("2016-07-07 07:07:07"))))
    )
    input.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT test_table_2.$column_name FROM test_table RIGHT OUTER JOIN test_table_2
             | ON test_table.$column_name = test_table_2.$column_name order by 1""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT * FROM ( SELECT ( "SUBQUERY_5"."SUBQUERY_5_COL_1" ) AS "SUBQUERY_6_COL_0"
           | FROM ( SELECT ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) AS "SUBQUERY_5_COL_0",
           | ( "SUBQUERY_4"."SUBQUERY_4_COL_0" ) AS "SUBQUERY_5_COL_1"
           | FROM ( SELECT ( "SUBQUERY_1"."$column_name" ) AS "SUBQUERY_2_COL_0"
           | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
           | AS "SUBQUERY_0" WHERE ( "SUBQUERY_0"."$column_name" IS NOT NULL ) ) AS "SUBQUERY_1" )
           | AS "SUBQUERY_2" RIGHT OUTER JOIN ( SELECT ( "SUBQUERY_3"."$column_name" )
           | AS "SUBQUERY_4_COL_0" FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" )
           | AS "SUBQUERY_3" ) AS "SUBQUERY_4"
           | ON ( "SUBQUERY_2"."SUBQUERY_2_COL_0" = "SUBQUERY_4"."SUBQUERY_4_COL_0" ) )
           | AS "SUBQUERY_5" ) AS "SUBQUERY_6" ORDER BY ( "SUBQUERY_6"."SUBQUERY_6_COL_0" )
           | ASC NULLS FIRST""".stripMargin)
    })
  }

  // FLOAT8 type is not supported for pushdown.
  val testJoin_float8_unsupported: TestCase = TestCase (
    """SELECT test_table_2.testdouble FROM test_table JOIN test_table_2
      | ON test_table.testdouble = test_table_2.testdouble order by 1""".stripMargin,
    Seq(Row(-1234152.12312498), Row(0.0), Row(0.0), Row(1234152.12312498)),
    s"""SELECT ( "SUBQUERY_1"."TESTDOUBLE" ) AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM (
       | SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | WHERE ( "SUBQUERY_0"."TESTDOUBLE" IS NOT NULL ) ) AS "SUBQUERY_1"""".stripMargin
  )

  // FLOAT4 type is not supported for pushdown.
  val testJoin_float4_unsupported: TestCase = TestCase(
    """SELECT test_table_2.testfloat FROM test_table JOIN test_table_2
      | ON test_table.testfloat = test_table_2.testfloat order by 1""".stripMargin,
    Seq(Row(-1.0), Row(0.0)),
    s"""SELECT ( "SUBQUERY_1"."TESTFLOAT" ) AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM (
       | SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | WHERE ( "SUBQUERY_0"."TESTFLOAT" IS NOT NULL ) ) AS "SUBQUERY_1"""".stripMargin
  )

  test("Test JOIN logical plan operator unsupported pushdown") {
    doTest(sqlContext, testJoin_float8_unsupported)
    doTest(sqlContext, testJoin_float4_unsupported)
  }

  val testJoin01: TestCase = TestCase(
    """SELECT test_table_2.testbyte FROM test_table JOIN test_table_2
      | ON test_table.testint = test_table_2.testbyte order by 1""".stripMargin,
    Seq(Row(42), Row(42)),
    s"""SELECT * FROM ( SELECT ( "SUBQUERY_6"."SUBQUERY_6_COL_1" ) AS "SUBQUERY_7_COL_0"
       | FROM ( SELECT ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) AS "SUBQUERY_6_COL_0",
       | ( "SUBQUERY_5"."SUBQUERY_5_COL_0" ) AS "SUBQUERY_6_COL_1"
       | FROM (SELECT ( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( "SUBQUERY_0"."TESTINT" IS NOT NULL ) ) AS"SUBQUERY_1" )
       | AS "SUBQUERY_2" INNER JOIN ( SELECT ( "SUBQUERY_4"."TESTBYTE" ) AS "SUBQUERY_5_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_3" WHERE ( "SUBQUERY_3"."TESTBYTE" IS NOT NULL ) ) AS "SUBQUERY_4" )
       | AS "SUBQUERY_5" ON ( "SUBQUERY_2"."SUBQUERY_2_COL_0" = CAST ( "SUBQUERY_5"."SUBQUERY_5_COL_0" AS INTEGER ) ) )
       | AS "SUBQUERY_6" ) AS "SUBQUERY_7" ORDER BY ( "SUBQUERY_7"."SUBQUERY_7_COL_0" ) ASC
       | NULLS FIRST""".stripMargin
  )

  val testJoin02: TestCase = TestCase(
    """SELECT DISTINCT test_table_2.testbyte FROM test_table JOIN test_table_2
      | ON test_table.testint = test_table_2.testbyte order by 1""".stripMargin,
    Seq(Row(42)),
    s"""SELECT * FROM ( SELECT ( "SUBQUERY_7"."SUBQUERY_7_COL_0" ) AS "SUBQUERY_8_COL_0"
       | FROM ( SELECT ( "SUBQUERY_6"."SUBQUERY_6_COL_1" ) AS "SUBQUERY_7_COL_0"
       | FROM ( SELECT ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) AS "SUBQUERY_6_COL_0",
       | ( "SUBQUERY_5"."SUBQUERY_5_COL_0" ) AS "SUBQUERY_6_COL_1" FROM (
       | SELECT ( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | WHERE ( "SUBQUERY_0"."TESTINT" IS NOT NULL ) ) AS "SUBQUERY_1" ) AS "SUBQUERY_2"
       | INNER JOIN ( SELECT ( "SUBQUERY_4"."TESTBYTE" ) AS "SUBQUERY_5_COL_0" FROM (
       | SELECT * FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_3" WHERE ( "SUBQUERY_3"."TESTBYTE" IS NOT NULL ) ) AS "SUBQUERY_4" )
       | AS "SUBQUERY_5" ON ( "SUBQUERY_2"."SUBQUERY_2_COL_0" = CAST ( "SUBQUERY_5"."SUBQUERY_5_COL_0" AS INTEGER ) ) )
       | AS "SUBQUERY_6" ) AS "SUBQUERY_7" GROUP BY "SUBQUERY_7"."SUBQUERY_7_COL_0" )
       | AS "SUBQUERY_8" ORDER BY ( "SUBQUERY_8"."SUBQUERY_8_COL_0" ) ASC
       | NULLS FIRST""".stripMargin
  )

  val testJoin03: TestCase = TestCase(
    """SELECT test_table_2.testbyte FROM test_table INNER JOIN test_table_2
      | ON test_table.testint = test_table_2.testbyte order by 1""".stripMargin,
    Seq(Row(42), Row(42)),
    s"""SELECT * FROM ( SELECT ( "SUBQUERY_6"."SUBQUERY_6_COL_1" ) AS "SUBQUERY_7_COL_0"
       | FROM ( SELECT ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) AS "SUBQUERY_6_COL_0",
       | ( "SUBQUERY_5"."SUBQUERY_5_COL_0" ) AS "SUBQUERY_6_COL_1"
       | FROM ( SELECT ( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( "SUBQUERY_0"."TESTINT" IS NOT NULL ) ) AS "SUBQUERY_1" )
       | AS "SUBQUERY_2" INNER JOIN ( SELECT ( "SUBQUERY_4"."TESTBYTE" ) AS "SUBQUERY_5_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_3" WHERE ( "SUBQUERY_3"."TESTBYTE" IS NOT NULL ) ) AS"SUBQUERY_4" )
       | AS "SUBQUERY_5" ON ( "SUBQUERY_2"."SUBQUERY_2_COL_0" = CAST ( "SUBQUERY_5"."SUBQUERY_5_COL_0" AS INTEGER ) ) )
       | AS "SUBQUERY_6" ) AS "SUBQUERY_7" ORDER BY ( "SUBQUERY_7"."SUBQUERY_7_COL_0" )
       | ASC NULLS FIRST""".stripMargin
  )

  val testJoin04: TestCase = TestCase(
    """SELECT test_table_2.testbyte FROM test_table FULL OUTER JOIN test_table_2
      | ON test_table.testint = test_table_2.testbyte order by 1""".stripMargin,
    Seq(Row(null), Row(null), Row(null), Row(null), Row(null), Row(0), Row(1), Row(2), Row(3),
      Row(42), Row(42)),
    s"""SELECT * FROM ( SELECT ( "SUBQUERY_4"."SUBQUERY_4_COL_1" ) AS "SUBQUERY_5_COL_0"
       | FROM ( SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_4_COL_0",
       | ( "SUBQUERY_3"."SUBQUERY_3_COL_0" ) AS "SUBQUERY_4_COL_1"
       | FROM ( SELECT ( "SUBQUERY_0"."TESTINT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" FULL OUTER JOIN ( SELECT ( "SUBQUERY_2"."TESTBYTE" ) AS "SUBQUERY_3_COL_0"
       | FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_2" )
       | AS "SUBQUERY_3" ON ( "SUBQUERY_1"."SUBQUERY_1_COL_0" = CAST ( "SUBQUERY_3"."SUBQUERY_3_COL_0" AS INTEGER ) ) )
       | AS "SUBQUERY_4" ) AS "SUBQUERY_5" ORDER BY ( "SUBQUERY_5"."SUBQUERY_5_COL_0" )
       | ASC NULLS FIRST""".stripMargin
  )

  val testJoin05: TestCase = TestCase(
    """SELECT test_table_2.testbyte FROM test_table LEFT OUTER JOIN test_table_2
      | ON test_table.testint = test_table_2.testbyte order by 1""".stripMargin,
    Seq(Row(null), Row(null), Row(null), Row(42), Row(42)),
    s"""SELECT * FROM ( SELECT ( "SUBQUERY_5"."SUBQUERY_5_COL_1" ) AS "SUBQUERY_6_COL_0"
       | FROM ( SELECT ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_5_COL_0",
       | ( "SUBQUERY_4"."SUBQUERY_4_COL_0" ) AS "SUBQUERY_5_COL_1"
       | FROM ( SELECT ( "SUBQUERY_0"."TESTINT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | AS "SUBQUERY_1" LEFT OUTER JOIN ( SELECT ( "SUBQUERY_3"."TESTBYTE" ) AS "SUBQUERY_4_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_2" WHERE ( "SUBQUERY_2"."TESTBYTE" IS NOT NULL ) ) AS "SUBQUERY_3" )
       | AS "SUBQUERY_4" ON ( "SUBQUERY_1"."SUBQUERY_1_COL_0" = CAST ( "SUBQUERY_4"."SUBQUERY_4_COL_0" AS INTEGER ) ) )
       | AS "SUBQUERY_5" ) AS "SUBQUERY_6" ORDER BY ( "SUBQUERY_6"."SUBQUERY_6_COL_0" )
       | ASC NULLS FIRST""".stripMargin
  )

  val testJoin06: TestCase = TestCase(
    """SELECT test_table_2.testbyte FROM test_table RIGHT OUTER JOIN test_table_2
      | ON test_table.testint = test_table_2.testbyte order by 1""".stripMargin,
    Seq(Row(null), Row(null), Row(0), Row(1), Row(2), Row(3), Row(42), Row(42)),
    s"""SELECT * FROM ( SELECT ( "SUBQUERY_5"."SUBQUERY_5_COL_1" ) AS "SUBQUERY_6_COL_0"
       | FROM ( SELECT ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) AS "SUBQUERY_5_COL_0",
       | ( "SUBQUERY_4"."SUBQUERY_4_COL_0" ) AS "SUBQUERY_5_COL_1"
       | FROM ( SELECT ( "SUBQUERY_1"."TESTINT" ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( "SUBQUERY_0"."TESTINT" IS NOT NULL ) ) AS "SUBQUERY_1" )
       | AS "SUBQUERY_2" RIGHT OUTER JOIN ( SELECT ( "SUBQUERY_3"."TESTBYTE" )
       | AS "SUBQUERY_4_COL_0" FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_3" ) AS "SUBQUERY_4"
       | ON ( "SUBQUERY_2"."SUBQUERY_2_COL_0" = CAST ( "SUBQUERY_4"."SUBQUERY_4_COL_0" AS INTEGER ) ) )
       | AS "SUBQUERY_5" ) AS "SUBQUERY_6" ORDER BY ( "SUBQUERY_6"."SUBQUERY_6_COL_0" )
       | ASC NULLS FIRST""".stripMargin
  )

  // FLOAT4 and FLOAT8 join cannot be pushed down
  val testJoin11: TestCase = TestCase(
    """SELECT test_table_2.testfloat FROM test_table JOIN test_table_2
      | ON test_table.testdouble = test_table_2.testfloat order by 1""".stripMargin,
    Seq(Row(0.0), Row(0.0)),
    s"""SELECT ( "SUBQUERY_1"."TESTFLOAT" ) AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM (
       | SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | WHERE ( "SUBQUERY_0"."TESTFLOAT" IS NOT NULL ) ) AS "SUBQUERY_1"""".stripMargin
  )

  val testJoin12: TestCase = TestCase(
    """SELECT test_table_2.testfloat FROM test_table FULL OUTER JOIN test_table_2
      | ON test_table.testdouble = test_table_2.testfloat order by 1""".stripMargin,
    Seq(Row(null), Row(null), Row(null), Row(null), Row(-1.0.toFloat), Row(0.0.toFloat),
      Row(0.0.toFloat), Row(2.0.toFloat), Row(5.0.toFloat), Row(55.12.toFloat),
      Row(100.0.toFloat)),
    s"""SELECT ( "SUBQUERY_0"."TESTFLOAT" ) AS "SUBQUERY_1_COL_0" FROM ( SELECT * FROM
       | $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"""".stripMargin
  )

  val testJoin13: TestCase = TestCase(
    """SELECT test_table_2.testfloat FROM test_table LEFT OUTER JOIN test_table_2
      | ON test_table.testdouble = test_table_2.testfloat order by 1""".stripMargin,
    Seq(Row(null), Row(null), Row(null), Row(0.0), Row(0.0)),
    s"""SELECT ( "SUBQUERY_1"."TESTFLOAT" ) AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM (
       | SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | WHERE ( "SUBQUERY_0"."TESTFLOAT" IS NOT NULL ) ) AS "SUBQUERY_1"""".stripMargin
  )

  val testJoin14: TestCase = TestCase(
    """SELECT test_table_2.testfloat FROM test_table RIGHT OUTER JOIN test_table_2
      | ON test_table.testdouble = test_table_2.testfloat order by 1""".stripMargin,
    Seq(Row(null), Row(-1.0.toFloat), Row(0.0.toFloat), Row(0.0.toFloat), Row(2.0.toFloat),
      Row(5.0.toFloat), Row(55.12.toFloat), Row(100.0.toFloat)),
    s"""SELECT ( "SUBQUERY_0"."TESTFLOAT" ) AS "SUBQUERY_1_COL_0" FROM ( SELECT * FROM
       | $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"""".stripMargin
  )

  test("Test JOIN logical plan operator with different column type") {
    doTest(sqlContext, testJoin01)
    doTest(sqlContext, testJoin02)
    doTest(sqlContext, testJoin03)
    doTest(sqlContext, testJoin04)
    doTest(sqlContext, testJoin05)
    doTest(sqlContext, testJoin06)
    doTest(sqlContext, testJoin11)
    doTest(sqlContext, testJoin12)
    doTest(sqlContext, testJoin13)
    doTest(sqlContext, testJoin14)
  }

  test("Test UNION logical plan operator", P1Test) {
    // "Column name" and result set
    val input = List(
      ("testbyte", Seq(Row(null), Row(0), Row(1), Row(2), Row(3), Row(42))),
      ("testbool", Seq(Row(null), Row(false), Row(true))),
      ("testdate", Seq(Row(null), Row(Date.valueOf("2015-07-01")),
        Row(Date.valueOf("2015-07-02")), Row(Date.valueOf("2015-07-03")),
        Row(Date.valueOf("2015-07-04")), Row(Date.valueOf("2015-07-05")))),
      ("testdouble", Seq(Row(null), Row(-1234152.12312498), Row(0.0), Row(1.1),
        Row(2.2), Row(12345.12345678), Row(1234152.12312498))),
      ("testfloat", Seq(Row(null), Row(-1.0.toFloat), Row(0.0.toFloat), Row(1.0.toFloat),
        Row(2.0.toFloat), Row(5.0.toFloat), Row(55.12.toFloat), Row(100.0.toFloat),
        Row(100000.0.toFloat))),
      ("testint", Seq(Row(null), Row(42), Row(45), Row(216), Row(365), Row(4141214))),
      ("testlong", Seq(Row(null), Row(54321), Row(1239012341823715L),
        Row(1239012341823716L), Row(1239012341823717L), Row(1239012341823718L),
        Row(1239012341823719L))),
      ("testshort", Seq(Row(null), Row(-13), Row(23), Row(24), Row(56))),
      ("teststring", Seq(Row(null), Row("Unicode's樂趣"), Row("_____"), Row("___|_123"),
        Row("acbdef"), Row("asdf"), Row("f"))),
      ("testtimestamp", Seq(Row(null),
        Row(Timestamp.valueOf("2015-07-01 00:00:00.001")),
        Row(Timestamp.valueOf("2015-07-02 00:00:00")),
        Row(Timestamp.valueOf("2015-07-03 12:34:56")),
        Row(Timestamp.valueOf("2016-07-07 07:07:07"))))
    )
    input.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT test_table.$column_name FROM test_table UNION
             | SELECT test_table_2.$column_name FROM test_table_2
             | ORDER BY 1""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT * FROM ( SELECT ( "SUBQUERY_0"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_1_COL_0"
           | FROM ( ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | UNION ALL ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) )
           | AS "SUBQUERY_0" GROUP BY "SUBQUERY_0"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_1" ORDER BY
           | ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ASC NULLS FIRST""".stripMargin)
    })
  }

  test("Test UNION DISTINCT logical plan operator") {
    // "Column name" and result set
    val input = List(
      ("testbyte", Seq(Row(null), Row(0), Row(1), Row(2), Row(3), Row(42))),
      ("testbool", Seq(Row(null), Row(false), Row(true))),
      ("testdate", Seq(Row(null), Row(Date.valueOf("2015-07-01")),
        Row(Date.valueOf("2015-07-02")), Row(Date.valueOf("2015-07-03")),
        Row(Date.valueOf("2015-07-04")), Row(Date.valueOf("2015-07-05")))),
      ("testint", Seq(Row(null), Row(42), Row(45), Row(216), Row(365), Row(4141214))),
      ("testdouble", Seq(Row(null), Row(-1234152.12312498), Row(0.0), Row(1.1),
        Row(2.2), Row(12345.12345678), Row(1234152.12312498))),
      ("testfloat", Seq(Row(null), Row(-1.0.toFloat), Row(0.0.toFloat), Row(1.0.toFloat),
        Row(2.0.toFloat), Row(5.0.toFloat), Row(55.12.toFloat), Row(100.0.toFloat),
        Row(100000.0.toFloat))),
      ("testlong", Seq(Row(null), Row(54321), Row(1239012341823715L),
        Row(1239012341823716L), Row(1239012341823717L), Row(1239012341823718L),
        Row(1239012341823719L))),
      ("testshort", Seq(Row(null), Row(-13), Row(23), Row(24), Row(56))),
      ("teststring", Seq(Row(null), Row("Unicode's樂趣"), Row("_____"), Row("___|_123"),
        Row("acbdef"), Row("asdf"), Row("f"))),
      ("testtimestamp", Seq(Row(null),
        Row(Timestamp.valueOf("2015-07-01 00:00:00.001")),
        Row(Timestamp.valueOf("2015-07-02 00:00:00")),
        Row(Timestamp.valueOf("2015-07-03 12:34:56")),
        Row(Timestamp.valueOf("2016-07-07 07:07:07"))))
    )
    input.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT test_table.$column_name FROM test_table UNION DISTINCT
             | SELECT test_table_2.$column_name FROM test_table_2
             | ORDER BY 1""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT * FROM ( SELECT ( "SUBQUERY_0"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_1_COL_0"
           | FROM ( ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | UNION ALL ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) )
           | AS "SUBQUERY_0" GROUP BY "SUBQUERY_0"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_1" ORDER BY
           | ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ASC NULLS FIRST""".stripMargin)
    })
  }

  test("Test UNION ALL logical plan operator") {
    // "Column name" and result set
    val input = List(
      ("testbyte", Seq(Row(null), Row(null), Row(null), Row(0), Row(0), Row(0), Row(1), Row(1),
        Row(1), Row(2), Row(3), Row(42))),
      ("testbool", Seq(Row(null), Row(null), Row(null), Row(null), Row(false), Row(false),
        Row(false), Row(false), Row(true), Row(true), Row(true), Row(true))),
      ("testdate", Seq(Row(null), Row(null), Row(null), Row(null),
        Row(Date.valueOf("2015-07-01")), Row(Date.valueOf("2015-07-01")),
        Row(Date.valueOf("2015-07-02")), Row(Date.valueOf("2015-07-02")),
        Row(Date.valueOf("2015-07-03")), Row(Date.valueOf("2015-07-03")),
        Row(Date.valueOf("2015-07-04")), Row(Date.valueOf("2015-07-05")))),
      ("testdouble", Seq(Row(null), Row(null), Row(-1234152.12312498), Row(-1234152.12312498),
        Row(0.0), Row(0.0), Row(0.0), Row(1.1), Row(2.2), Row(12345.12345678),
        Row(1234152.12312498), Row(1234152.12312498))),
      ("testfloat", Seq(Row(null), Row(null), Row(-1.0.toFloat), Row(-1.0.toFloat),
        Row(0.0.toFloat), Row(0.0.toFloat), Row(1.0.toFloat), Row(2.0.toFloat),
        Row(5.0.toFloat), Row(55.12.toFloat), Row(100.0.toFloat),
        Row(100000.0.toFloat))),
      ("testint", Seq(Row(null), Row(null), Row(null), Row(null), Row(42), Row(42),
        Row(42), Row(42), Row(45), Row(216), Row(365), Row(4141214))),
      ("testlong", Seq(Row(null), Row(null), Row(54321), Row(1239012341823715L),
        Row(1239012341823716L), Row(1239012341823717L), Row(1239012341823718L),
        Row(1239012341823719L), Row(1239012341823719L), Row(1239012341823719L),
        Row(1239012341823719L), Row(1239012341823719L))),
      ("testshort", Seq(Row(null), Row(null), Row(null), Row(null), Row(-13), Row(-13), Row(23),
        Row(23), Row(23), Row(24), Row(24), Row(56))),
      ("teststring", Seq(Row(null), Row(null), Row("Unicode's樂趣"), Row("Unicode's樂趣"),
        Row("_____"), Row("___|_123"), Row("___|_123"), Row("acbdef"), Row("asdf"), Row("asdf"),
        Row("f"), Row("f"))),
      ("testtimestamp", Seq(Row(null), Row(null), Row(null), Row(null),
        Row(Timestamp.valueOf("2015-07-01 00:00:00.001")),
        Row(Timestamp.valueOf("2015-07-01 00:00:00.001")),
        Row(Timestamp.valueOf("2015-07-02 00:00:00")),
        Row(Timestamp.valueOf("2015-07-02 00:00:00")),
        Row(Timestamp.valueOf("2015-07-03 12:34:56")),
        Row(Timestamp.valueOf("2015-07-03 12:34:56")),
        Row(Timestamp.valueOf("2015-07-03 12:34:56")),
        Row(Timestamp.valueOf("2016-07-07 07:07:07"))))
    )
    input.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT test_table.$column_name FROM test_table UNION ALL
             | SELECT test_table_2.$column_name FROM test_table_2
             | ORDER BY 1""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT * FROM ( ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | UNION ALL ( SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS"SUBQUERY_0" ) )
           | AS "SUBQUERY_0" ORDER BY ( "SUBQUERY_0"."SUBQUERY_1_COL_0" ) ASC
           | NULLS FIRST""".stripMargin)
    })
  }

  val testUnion01: TestCase = TestCase(
    """SELECT test_table.testint FROM test_table UNION
      | SELECT test_table_2.testbyte FROM test_table_2
      | ORDER BY 1""".stripMargin,
    Seq(Row(null), Row(0), Row(1), Row(2), Row(3), Row(42), Row(4141214)),
    s"""SELECT * FROM ( SELECT ( "SUBQUERY_0"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_1_COL_0"
       | FROM ( ( SELECT ( "SUBQUERY_0"."TESTINT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | UNION ALL ( SELECT ( CAST ( "SUBQUERY_0"."TESTBYTE" AS INTEGER ) ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) )
       | AS "SUBQUERY_0" GROUP BY "SUBQUERY_0"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_1" ORDER BY
       | ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ASC NULLS FIRST""".stripMargin
  )

  val testUnion02: TestCase = TestCase(
    """SELECT test_table.testint FROM test_table UNION DISTINCT
      | SELECT test_table_2.testbyte FROM test_table_2
      | ORDER BY 1""".stripMargin,
    Seq(Row(null), Row(0), Row(1), Row(2), Row(3), Row(42), Row(4141214)),
    s"""SELECT * FROM ( SELECT ( "SUBQUERY_0"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_1_COL_0"
       | FROM ( ( SELECT ( "SUBQUERY_0"."TESTINT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | UNION ALL ( SELECT ( CAST ( "SUBQUERY_0"."TESTBYTE" AS INTEGER ) ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) )
       | AS "SUBQUERY_0" GROUP BY "SUBQUERY_0"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_1" ORDER BY
       | ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ASC NULLS FIRST""".stripMargin
  )

  val testUnion03: TestCase = TestCase(
    """SELECT test_table.testint FROM test_table UNION ALL
      | SELECT test_table_2.testbyte FROM test_table_2
      | ORDER BY 1""".stripMargin,
    Seq(Row(null), Row(null), Row(null), Row(null), Row(0), Row(1), Row(2), Row(3),
      Row(42), Row(42), Row(42), Row(4141214)),
    s"""SELECT * FROM ( ( SELECT ( "SUBQUERY_0"."TESTINT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | UNION ALL ( SELECT ( CAST ( "SUBQUERY_0"."TESTBYTE" AS INTEGER ) ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS"SUBQUERY_0" ) )
       | AS "SUBQUERY_0" ORDER BY ( "SUBQUERY_0"."SUBQUERY_1_COL_0" ) ASC NULLS FIRST""".stripMargin
  )

  val testUnion11: TestCase = TestCase(
    """SELECT test_table.testdouble FROM test_table UNION
      | SELECT test_table_2.testfloat FROM test_table_2
      | ORDER BY 1""".stripMargin,
    Seq(Row(null), Row(-1234152.12312498), Row(-1.0.toFloat), Row(0.0.toFloat),
      Row(2.0.toFloat), Row(5.0.toFloat), Row(55.119998931884766), Row(100.0.toFloat),
      Row(1234152.12312498)),
    s"""SELECT * FROM ( SELECT ( "SUBQUERY_0"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_1_COL_0"
       | FROM ( ( SELECT ( "SUBQUERY_0"."TESTDOUBLE" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | UNION ALL ( SELECT ( CAST ( "SUBQUERY_0"."TESTFLOAT" AS FLOAT8 ) ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) )
       | AS "SUBQUERY_0" GROUP BY "SUBQUERY_0"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_1" ORDER BY
       | ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ASC NULLS FIRST""".stripMargin
  )

  val testUnion12: TestCase = TestCase(
    """SELECT test_table.testdouble FROM test_table UNION DISTINCT
      | SELECT test_table_2.testfloat FROM test_table_2
      | ORDER BY 1""".stripMargin,
    Seq(Row(null), Row(-1234152.12312498), Row(-1.0.toFloat), Row(0.0.toFloat),
      Row(2.0.toFloat), Row(5.0.toFloat), Row(55.119998931884766), Row(100.0.toFloat),
      Row(1234152.12312498)),
    s"""SELECT * FROM ( SELECT ( "SUBQUERY_0"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_1_COL_0"
       | FROM ( ( SELECT ( "SUBQUERY_0"."TESTDOUBLE" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | UNION ALL ( SELECT ( CAST ( "SUBQUERY_0"."TESTFLOAT" AS FLOAT8 ) ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) )
       | AS "SUBQUERY_0" GROUP BY "SUBQUERY_0"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_1" ORDER BY
       | ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ASC NULLS FIRST""".stripMargin
  )

  val testUnion13: TestCase = TestCase(
    """SELECT test_table.testdouble FROM test_table UNION ALL
      | SELECT test_table_2.testfloat FROM test_table_2
      | ORDER BY 1""".stripMargin,
    Seq(Row(null), Row(null), Row(-1234152.12312498), Row(-1.0.toFloat), Row(0.0.toFloat),
      Row(0.0.toFloat), Row(0.0.toFloat), Row(2.0.toFloat), Row(5.0.toFloat),
      Row(55.119998931884766), Row(100.0.toFloat), Row(1234152.12312498)),
    s"""SELECT * FROM ( ( SELECT ( "SUBQUERY_0"."TESTDOUBLE" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | UNION ALL ( SELECT ( CAST ( "SUBQUERY_0"."TESTFLOAT" AS FLOAT8 ) ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS"SUBQUERY_0" ) )
       | AS "SUBQUERY_0" ORDER BY ( "SUBQUERY_0"."SUBQUERY_1_COL_0" ) ASC NULLS FIRST""".stripMargin
  )

  test("Test UNION logical plan operator with different column type") {
    doTest(sqlContext, testUnion01)
    doTest(sqlContext, testUnion02)
    doTest(sqlContext, testUnion03)
    doTest(sqlContext, testUnion11)
    doTest(sqlContext, testUnion12)
    doTest(sqlContext, testUnion13)
  }

  // No push down for except
  test("Test EXCEPT logical plan operator") {
    // "Column name" and result set
    val input = List(
      ("testbyte", Seq(Row(2), Row(3), Row(42))),
      ("testbool", Seq()),
      ("testdate", Seq(Row(Date.valueOf("2015-07-04")), Row(Date.valueOf("2015-07-05")))),
      ("testdouble", Seq(Row(1.1), Row(2.2), Row(12345.12345678))),
      ("testfloat", Seq(Row(2.0.toFloat), Row(5.0.toFloat), Row(55.12.toFloat),
        Row(100.0.toFloat))),
      ("testint", Seq(Row(45), Row(216), Row(365))),
      ("testlong", Seq(Row(54321), Row(1239012341823715L),
        Row(1239012341823716L), Row(1239012341823717L), Row(1239012341823718L))),
      ("testshort", Seq(Row(56))),
      ("teststring", Seq(Row("_____"), Row("acbdef"))),
      ("testtimestamp", Seq(Row(Timestamp.valueOf("2016-07-07 07:07:07"))))
    )
    input.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT test_table_2.$column_name FROM test_table_2 EXCEPT
             | SELECT test_table.$column_name FROM test_table
             | ORDER BY 1""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
           | """.stripMargin)
    })
  }

  // No push down for except distinct
  test("Test EXCEPT DISTINCT logical plan operator") {
    // "Column name" and result set
    val input = List(
      ("testbyte", Seq(Row(2), Row(3), Row(42))),
      ("testbool", Seq()),
      ("testdate", Seq(Row(Date.valueOf("2015-07-04")), Row(Date.valueOf("2015-07-05")))),
      ("testdouble", Seq(Row(1.1), Row(2.2), Row(12345.12345678))),
      ("testfloat", Seq(Row(2.0.toFloat), Row(5.0.toFloat), Row(55.12.toFloat),
        Row(100.0.toFloat))),
      ("testint", Seq(Row(45), Row(216), Row(365))),
      ("testlong", Seq(Row(54321), Row(1239012341823715L),
        Row(1239012341823716L), Row(1239012341823717L), Row(1239012341823718L))),
      ("testshort", Seq(Row(56))),
      ("teststring", Seq(Row("_____"), Row("acbdef"))),
      ("testtimestamp", Seq(Row(Timestamp.valueOf("2016-07-07 07:07:07"))))
    )
    input.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT test_table_2.$column_name FROM test_table_2 EXCEPT DISTINCT
             | SELECT test_table.$column_name FROM test_table
             | ORDER BY 1""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
           | """.stripMargin)
    })
  }

  test("Test EXCEPT ALL logical plan operator") {
    // "Column name" and result set
    val input = List(
      ("testbyte", Seq(Row(null), Row(2), Row(3), Row(42))),
      ("testbool", Seq(Row(true), Row(true))),
      ("testdate", Seq(Row(Date.valueOf("2015-07-04")), Row(Date.valueOf("2015-07-05")))),
      ("testdouble", Seq(Row(1.1), Row(2.2), Row(12345.12345678))),
      ("testfloat", Seq(Row(2.0.toFloat), Row(5.0.toFloat), Row(55.12.toFloat),
        Row(100.0.toFloat))),
      ("testint", Seq(Row(45), Row(216), Row(365))),
      ("testlong", Seq(Row(54321), Row(1239012341823715L),
        Row(1239012341823716L), Row(1239012341823717L), Row(1239012341823718L))),
      ("testshort", Seq(Row(23), Row(56))),
      ("teststring", Seq(Row("_____"), Row("acbdef"))),
      ("testtimestamp", Seq(Row(Timestamp.valueOf("2015-07-03 12:34:56")),
        Row(Timestamp.valueOf("2016-07-07 07:07:07"))))
    )
    input.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT test_table_2.$column_name FROM test_table_2 EXCEPT ALL
             | SELECT test_table.$column_name FROM test_table
             | ORDER BY 1""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT * FROM ( SELECT ( "SUBQUERY_0"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_1_COL_0",
           | ( SUM ( "SUBQUERY_0"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_1_COL_1"
           | FROM ( ( SELECT ( 1 ) AS "SUBQUERY_1_COL_0",
           | ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_1" FROM (
           | SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | UNION ALL ( SELECT ( -1 ) AS "SUBQUERY_1_COL_0",
           | ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_1" FROM (
           | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) )
           | AS "SUBQUERY_0" GROUP BY "SUBQUERY_0"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_1"
           | WHERE ( ( "SUBQUERY_1"."SUBQUERY_1_COL_1" IS NOT NULL )
           | AND ( "SUBQUERY_1"."SUBQUERY_1_COL_1" > 0 ) ) """.stripMargin)
    })
  }

  val testExcept01: TestCase = TestCase(
    """SELECT test_table_2.testbyte FROM test_table_2 EXCEPT
      | SELECT test_table.testint FROM test_table
      | ORDER BY 1""".stripMargin,
    Seq(Row(0), Row(1), Row(2), Row(3)),
    s"""SELECT ( "SUBQUERY_0"."TESTINT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | """.stripMargin
  )

  val testExcept02: TestCase = TestCase(
    """SELECT test_table_2.testbyte FROM test_table_2 EXCEPT DISTINCT
      | SELECT test_table.testint FROM test_table
      | ORDER BY 1""".stripMargin,
    Seq(Row(0), Row(1), Row(2), Row(3)),
    s"""SELECT ( "SUBQUERY_0"."TESTINT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | """.stripMargin
  )

  val testExcept03: TestCase = TestCase(
    """SELECT test_table_2.testbyte FROM test_table_2 EXCEPT ALL
      | SELECT test_table.testint FROM test_table
      | ORDER BY 1""".stripMargin,
    Seq(Row(0), Row(1), Row(2), Row(3)),
    s"""SELECT * FROM ( SELECT ( "SUBQUERY_0"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_1_COL_0",
       | ( SUM ( "SUBQUERY_0"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_1_COL_1"
       | FROM ( ( SELECT ( 1 ) AS "SUBQUERY_1_COL_0",
       | ( CAST ( "SUBQUERY_0"."TESTBYTE" AS INTEGER ) ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | UNION ALL ( SELECT ( -1 ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."TESTINT" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) )
       | AS "SUBQUERY_0" GROUP BY "SUBQUERY_0"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_1"
       | WHERE ( ( "SUBQUERY_1"."SUBQUERY_1_COL_1" IS NOT NULL )
       | AND ( "SUBQUERY_1"."SUBQUERY_1_COL_1" > 0 ) ) """.stripMargin
  )

  val testExcept11: TestCase = TestCase(
    """SELECT test_table_2.testfloat FROM test_table_2 EXCEPT
      | SELECT test_table.testdouble FROM test_table
      | ORDER BY 1""".stripMargin,
    Seq(Row(-1.0.toFloat), Row(2.0.toFloat), Row(5.0.toFloat), Row(55.119998931884766),
      Row(100.0.toFloat)),
    s"""SELECT ( "SUBQUERY_0"."TESTDOUBLE" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | """.stripMargin
  )

  val testExcept12: TestCase = TestCase(
    """SELECT test_table_2.testfloat FROM test_table_2 EXCEPT DISTINCT
      | SELECT test_table.testdouble FROM test_table
      | ORDER BY 1""".stripMargin,
    Seq(Row(-1.0.toFloat), Row(2.0.toFloat), Row(5.0.toFloat), Row(55.119998931884766),
      Row(100.0.toFloat)),
    s"""SELECT ( "SUBQUERY_0"."TESTDOUBLE" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | """.stripMargin
  )

  val testExcept13: TestCase = TestCase(
    """SELECT test_table_2.testfloat FROM test_table_2 EXCEPT ALL
      | SELECT test_table.testdouble FROM test_table
      | ORDER BY 1""".stripMargin,
    Seq(Row(-1.0.toFloat), Row(2.0.toFloat), Row(5.0.toFloat), Row(55.119998931884766),
      Row(100.0.toFloat)),
    s"""SELECT * FROM ( SELECT ( "SUBQUERY_0"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_1_COL_0",
       | ( SUM ( "SUBQUERY_0"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_1_COL_1"
       | FROM ( ( SELECT ( 1 ) AS "SUBQUERY_1_COL_0",
       | ( CAST ( "SUBQUERY_0"."TESTFLOAT" AS FLOAT8 ) ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | UNION ALL ( SELECT ( -1 ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."TESTDOUBLE" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) )
       | AS "SUBQUERY_0" GROUP BY "SUBQUERY_0"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_1"
       | WHERE ( ( "SUBQUERY_1"."SUBQUERY_1_COL_1" IS NOT NULL )
       | AND ( "SUBQUERY_1"."SUBQUERY_1_COL_1" > 0 ) ) """.stripMargin
  )

  test("Test EXCEPT logical plan operator with different column type") {
    doTest(sqlContext, testExcept01)
    doTest(sqlContext, testExcept02)
    doTest(sqlContext, testExcept03)
    doTest(sqlContext, testExcept11)
    doTest(sqlContext, testExcept12)
    doTest(sqlContext, testExcept13)
  }

  // No push down for minus
  test("Test MINUS logical plan operator") {
    // "Column name" and result set
    val input = List(
      ("testbyte", Seq(Row(2), Row(3), Row(42))),
      ("testbool", Seq()),
      ("testdate", Seq(Row(Date.valueOf("2015-07-04")), Row(Date.valueOf("2015-07-05")))),
      ("testdouble", Seq(Row(1.1), Row(2.2), Row(12345.12345678))),
      ("testfloat", Seq(Row(2.0.toFloat), Row(5.0.toFloat), Row(55.12.toFloat),
        Row(100.0.toFloat))),
      ("testint", Seq(Row(45), Row(216), Row(365))),
      ("testlong", Seq(Row(54321), Row(1239012341823715L),
        Row(1239012341823716L), Row(1239012341823717L), Row(1239012341823718L))),
      ("testshort", Seq(Row(56))),
      ("teststring", Seq(Row("_____"), Row("acbdef"))),
      ("testtimestamp", Seq(Row(Timestamp.valueOf("2016-07-07 07:07:07"))))
    )
    input.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT test_table_2.$column_name FROM test_table_2 MINUS
             | SELECT test_table.$column_name FROM test_table
             | ORDER BY 1""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
           | """.stripMargin)
    })
  }

  // No push down for minus distinct
  test("Test MINUS DISTINCT logical plan operator") {
    // "Column name" and result set
    val input = List(
      ("testbyte", Seq(Row(2), Row(3), Row(42))),
      ("testbool", Seq()),
      ("testdate", Seq(Row(Date.valueOf("2015-07-04")), Row(Date.valueOf("2015-07-05")))),
      ("testdouble", Seq(Row(1.1), Row(2.2), Row(12345.12345678))),
      ("testfloat", Seq(Row(2.0.toFloat), Row(5.0.toFloat), Row(55.12.toFloat),
        Row(100.0.toFloat))),
      ("testint", Seq(Row(45), Row(216), Row(365))),
      ("testlong", Seq(Row(54321), Row(1239012341823715L),
        Row(1239012341823716L), Row(1239012341823717L), Row(1239012341823718L))),
      ("testshort", Seq(Row(56))),
      ("teststring", Seq(Row("_____"), Row("acbdef"))),
      ("testtimestamp", Seq(Row(Timestamp.valueOf("2016-07-07 07:07:07"))))
    )
    input.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT test_table_2.$column_name FROM test_table_2 MINUS DISTINCT
             | SELECT test_table.$column_name FROM test_table
             | ORDER BY 1""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
           | """.stripMargin)
    })
  }

  test("Test MINUS ALL logical plan operator") {
    // "Column name" and result set
    val input = List(
      ("testbyte", Seq(Row(null), Row(2), Row(3), Row(42))),
      ("testbool", Seq(Row(true), Row(true))),
      ("testdate", Seq(Row(Date.valueOf("2015-07-04")), Row(Date.valueOf("2015-07-05")))),
      ("testdouble", Seq(Row(1.1), Row(2.2), Row(12345.12345678))),
      ("testfloat", Seq(Row(2.0.toFloat), Row(5.0.toFloat), Row(55.12.toFloat),
        Row(100.0.toFloat))),
      ("testint", Seq(Row(45), Row(216), Row(365))),
      ("testlong", Seq(Row(54321), Row(1239012341823715L),
        Row(1239012341823716L), Row(1239012341823717L), Row(1239012341823718L))),
      ("testshort", Seq(Row(23), Row(56))),
      ("teststring", Seq(Row("_____"), Row("acbdef"))),
      ("testtimestamp", Seq(Row(Timestamp.valueOf("2015-07-03 12:34:56")),
        Row(Timestamp.valueOf("2016-07-07 07:07:07"))))
    )
    input.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT test_table_2.$column_name FROM test_table_2 MINUS ALL
             | SELECT test_table.$column_name FROM test_table
             | ORDER BY 1""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT * FROM ( SELECT ( "SUBQUERY_0"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_1_COL_0",
           | ( SUM ( "SUBQUERY_0"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_1_COL_1"
           | FROM ( ( SELECT ( 1 ) AS "SUBQUERY_1_COL_0",
           | ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_1" FROM (
           | SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | UNION ALL ( SELECT ( -1 ) AS "SUBQUERY_1_COL_0",
           | ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_1" FROM (
           | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) )
           | AS "SUBQUERY_0" GROUP BY "SUBQUERY_0"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_1"
           | WHERE ( ( "SUBQUERY_1"."SUBQUERY_1_COL_1" IS NOT NULL )
           | AND ( "SUBQUERY_1"."SUBQUERY_1_COL_1" > 0 ) ) """.stripMargin)
    })
  }

  val testMinus01: TestCase = TestCase(
    """SELECT test_table_2.testbyte FROM test_table_2 MINUS
      | SELECT test_table.testint FROM test_table
      | ORDER BY 1""".stripMargin,
    Seq(Row(0), Row(1), Row(2), Row(3)),
    s"""SELECT ( "SUBQUERY_0"."TESTINT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | """.stripMargin
  )

  val testMinus02: TestCase = TestCase(
    """SELECT test_table_2.testbyte FROM test_table_2 MINUS DISTINCT
      | SELECT test_table.testint FROM test_table
      | ORDER BY 1""".stripMargin,
    Seq(Row(0), Row(1), Row(2), Row(3)),
    s"""SELECT ( "SUBQUERY_0"."TESTINT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | """.stripMargin
  )

  val testMinus03: TestCase = TestCase(
    """SELECT test_table_2.testbyte FROM test_table_2 MINUS ALL
      | SELECT test_table.testint FROM test_table
      | ORDER BY 1""".stripMargin,
    Seq(Row(0), Row(1), Row(2), Row(3)),
    s"""SELECT * FROM ( SELECT ( "SUBQUERY_0"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_1_COL_0",
       | ( SUM ( "SUBQUERY_0"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_1_COL_1"
       | FROM ( ( SELECT ( 1 ) AS "SUBQUERY_1_COL_0",
       | ( CAST ( "SUBQUERY_0"."TESTBYTE" AS INTEGER ) ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | UNION ALL ( SELECT ( -1 ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."TESTINT" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) )
       | AS "SUBQUERY_0" GROUP BY "SUBQUERY_0"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_1"
       | WHERE ( ( "SUBQUERY_1"."SUBQUERY_1_COL_1" IS NOT NULL )
       | AND ( "SUBQUERY_1"."SUBQUERY_1_COL_1" > 0 ) ) """.stripMargin
  )

  val testMinus11: TestCase = TestCase(
    """SELECT test_table_2.testfloat FROM test_table_2 MINUS
      | SELECT test_table.testdouble FROM test_table
      | ORDER BY 1""".stripMargin,
    Seq(Row(-1.0.toFloat), Row(2.0.toFloat), Row(5.0.toFloat), Row(55.119998931884766),
      Row(100.0.toFloat)),
    s"""SELECT ( "SUBQUERY_0"."TESTDOUBLE" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | """.stripMargin
  )

  val testMinus12: TestCase = TestCase(
    """SELECT test_table_2.testfloat FROM test_table_2 MINUS DISTINCT
      | SELECT test_table.testdouble FROM test_table
      | ORDER BY 1""".stripMargin,
    Seq(Row(-1.0.toFloat), Row(2.0.toFloat), Row(5.0.toFloat), Row(55.119998931884766),
      Row(100.0.toFloat)),
    s"""SELECT ( "SUBQUERY_0"."TESTDOUBLE" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | """.stripMargin
  )

  val testMinus13: TestCase = TestCase(
    """SELECT test_table_2.testfloat FROM test_table_2 MINUS ALL
      | SELECT test_table.testdouble FROM test_table
      | ORDER BY 1""".stripMargin,
    Seq(Row(-1.0.toFloat), Row(2.0.toFloat), Row(5.0.toFloat), Row(55.119998931884766),
      Row(100.0.toFloat)),
    s"""SELECT * FROM ( SELECT ( "SUBQUERY_0"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_1_COL_0",
       | ( SUM ( "SUBQUERY_0"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_1_COL_1"
       | FROM ( ( SELECT ( 1 ) AS "SUBQUERY_1_COL_0",
       | ( CAST ( "SUBQUERY_0"."TESTFLOAT" AS FLOAT8 ) ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | UNION ALL ( SELECT ( -1 ) AS "SUBQUERY_1_COL_0",
       | ( "SUBQUERY_0"."TESTDOUBLE" ) AS "SUBQUERY_1_COL_1" FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) )
       | AS "SUBQUERY_0" GROUP BY "SUBQUERY_0"."SUBQUERY_1_COL_1" ) AS "SUBQUERY_1"
       | WHERE ( ( "SUBQUERY_1"."SUBQUERY_1_COL_1" IS NOT NULL )
       | AND ( "SUBQUERY_1"."SUBQUERY_1_COL_1" > 0 ) ) """.stripMargin
  )

  test("Test MINUS logical plan operator with different column type") {
    doTest(sqlContext, testMinus01)
    doTest(sqlContext, testMinus02)
    doTest(sqlContext, testMinus03)
    doTest(sqlContext, testMinus11)
    doTest(sqlContext, testMinus12)
    doTest(sqlContext, testMinus13)
  }

  // No push down for intersect
  test("Test INTERSECT logical plan operator") {
    // "Column name" and result set
    val input = List(
      ("testbyte", Seq(Row(null), Row(0), Row(1))),
      ("testbool", Seq(Row(null), Row(false), Row(true))),
      ("testdate", Seq(Row(null), Row(Date.valueOf("2015-07-01")),
        Row(Date.valueOf("2015-07-02")), Row(Date.valueOf("2015-07-03")))),
      ("testdouble", Seq(Row(null), Row(-1234152.12312498), Row(0.0),
        Row(1234152.12312498))),
      ("testfloat", Seq(Row(null), Row(-1.0.toFloat), Row(0.0.toFloat))),
      ("testint", Seq(Row(null), Row(42))),
      ("testlong", Seq(Row(null), Row(1239012341823719L))),
      ("testshort", Seq(Row(null), Row(-13), Row(23), Row(24))),
      ("teststring", Seq(Row(null), Row("Unicode's樂趣"), Row("___|_123"),
        Row("asdf"), Row("f"))),
      ("testtimestamp", Seq(Row(null), Row(Timestamp.valueOf("2015-07-01 00:00:00.001")),
        Row(Timestamp.valueOf("2015-07-02 00:00:00")),
        Row(Timestamp.valueOf("2015-07-03 12:34:56"))))
    )
    input.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT test_table_2.$column_name FROM test_table_2 INTERSECT
             | SELECT test_table.$column_name FROM test_table
             | ORDER BY 1""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
           | """.stripMargin)
    })
  }

  // No push down for intersect distinct
  test("Test INTERSECT DISTINCT logical plan operator") {
    // "Column name" and result set
    val input = List(
      ("testbyte", Seq(Row(null), Row(0), Row(1))),
      ("testbool", Seq(Row(null), Row(false), Row(true))),
      ("testdate", Seq(Row(null), Row(Date.valueOf("2015-07-01")),
        Row(Date.valueOf("2015-07-02")), Row(Date.valueOf("2015-07-03")))),
      ("testdouble", Seq(Row(null), Row(-1234152.12312498), Row(0.0),
        Row(1234152.12312498))),
      ("testfloat", Seq(Row(null), Row(-1.0.toFloat), Row(0.0.toFloat))),
      ("testint", Seq(Row(null), Row(42))),
      ("testlong", Seq(Row(null), Row(1239012341823719L))),
      ("testshort", Seq(Row(null), Row(-13), Row(23), Row(24))),
      ("teststring", Seq(Row(null), Row("Unicode's樂趣"), Row("___|_123"),
        Row("asdf"), Row("f"))),
      ("testtimestamp", Seq(Row(null), Row(Timestamp.valueOf("2015-07-01 00:00:00.001")),
        Row(Timestamp.valueOf("2015-07-02 00:00:00")),
        Row(Timestamp.valueOf("2015-07-03 12:34:56"))))
    )
    input.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT test_table_2.$column_name FROM test_table_2 INTERSECT DISTINCT
             | SELECT test_table.$column_name FROM test_table
             | ORDER BY 1""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_0"
           | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
           | """.stripMargin)
    })
  }

  test("Test INTERSECT ALL logical plan operator") {
    // "Column name" and result set
    val input = List(
      ("testbyte", Seq(Row(null), Row(0), Row(1))),
      ("testbool", Seq(Row(null), Row(null), Row(false), Row(false), Row(true))),
      ("testdate", Seq(Row(null), Row(null), Row(Date.valueOf("2015-07-01")),
        Row(Date.valueOf("2015-07-02")), Row(Date.valueOf("2015-07-03")))),
      ("testdouble", Seq(Row(null), Row(-1234152.12312498), Row(0.0),
        Row(1234152.12312498))),
      ("testfloat", Seq(Row(null), Row(-1.0.toFloat), Row(0.0.toFloat))),
      ("testint", Seq(Row(null), Row(null), Row(42), Row(42))),
      ("testlong", Seq(Row(null), Row(1239012341823719L))),
      ("testshort", Seq(Row(null), Row(null), Row(-13), Row(23), Row(24))),
      ("teststring", Seq(Row(null), Row("Unicode's樂趣"), Row("___|_123"),
        Row("asdf"), Row("f"))),
      ("testtimestamp", Seq(Row(null), Row(null),
        Row(Timestamp.valueOf("2015-07-01 00:00:00.001")),
        Row(Timestamp.valueOf("2015-07-02 00:00:00")),
        Row(Timestamp.valueOf("2015-07-03 12:34:56"))))
    )
    input.par.foreach(test_case => {
      val column_name = test_case._1.toUpperCase
      val expected_res = test_case._2

      checkAnswer(
        sqlContext.sql(
          s"""SELECT test_table_2.$column_name FROM test_table_2 INTERSECT ALL
             | SELECT test_table.$column_name FROM test_table
             | ORDER BY 1""".stripMargin),
        expected_res)

      checkSqlStatement(
        s"""SELECT ( "SUBQUERY_2"."SUBQUERY_1_COL_2" ) AS "SUBQUERY_3_COL_0",
           | ( CASE WHEN ( "SUBQUERY_2"."SUBQUERY_1_COL_0" > "SUBQUERY_2"."SUBQUERY_1_COL_1" )
           | THEN "SUBQUERY_2"."SUBQUERY_1_COL_1" ELSE "SUBQUERY_2"."SUBQUERY_1_COL_0" END )
           | AS "SUBQUERY_3_COL_1" FROM ( SELECT * FROM ( SELECT ( COUNT (
           | "SUBQUERY_0"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_1_COL_0",
           | ( COUNT ( "SUBQUERY_0"."SUBQUERY_1_COL_1" ) ) AS "SUBQUERY_1_COL_1",
           | ( "SUBQUERY_0"."SUBQUERY_1_COL_2" ) AS "SUBQUERY_1_COL_2"
           | FROM ( ( SELECT ( true ) AS "SUBQUERY_1_COL_0", ( NULL ) AS "SUBQUERY_1_COL_1",
           | ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_2"
           | FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
           | UNION ALL ( SELECT ( NULL ) AS "SUBQUERY_1_COL_0", ( true ) AS "SUBQUERY_1_COL_1",
           | ( "SUBQUERY_0"."$column_name" ) AS "SUBQUERY_1_COL_2" FROM ( SELECT * FROM $test_table
           | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) ) AS "SUBQUERY_0"
           | GROUP BY "SUBQUERY_0"."SUBQUERY_1_COL_2" ) AS "SUBQUERY_1"
           | WHERE ( ( "SUBQUERY_1"."SUBQUERY_1_COL_0" >= 1 ) AND
           | ( "SUBQUERY_1"."SUBQUERY_1_COL_1 " >= 1 ) ) ) AS "SUBQUERY_2" """.stripMargin)
    })
  }

  val testIntersect01: TestCase = TestCase(
    """SELECT test_table_2.testbyte FROM test_table_2 INTERSECT
      | SELECT test_table.testint FROM test_table
      | ORDER BY 1""".stripMargin,
    Seq(Row(null), Row(42)),
    s"""SELECT ( "SUBQUERY_0"."TESTINT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | """.stripMargin
  )

  val testIntersect02: TestCase = TestCase(
    """SELECT test_table_2.testbyte FROM test_table_2 INTERSECT DISTINCT
      | SELECT test_table.testint FROM test_table
      | ORDER BY 1""".stripMargin,
    Seq(Row(null), Row(42)),
    s"""SELECT ( "SUBQUERY_0"."TESTINT" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | """.stripMargin
  )

  val testIntersect03: TestCase = TestCase(
    """SELECT test_table_2.testbyte FROM test_table_2 INTERSECT ALL
      | SELECT test_table.testint FROM test_table
      | ORDER BY 1""".stripMargin,
    Seq(Row(null), Row(null), Row(42)),
    s"""SELECT ( "SUBQUERY_2"."SUBQUERY_1_COL_2" ) AS "SUBQUERY_3_COL_0",
       | ( CASE WHEN ( "SUBQUERY_2"."SUBQUERY_1_COL_0" > "SUBQUERY_2"."SUBQUERY_1_COL_1" )
       | THEN "SUBQUERY_2"."SUBQUERY_1_COL_1" ELSE "SUBQUERY_2"."SUBQUERY_1_COL_0" END )
       | AS "SUBQUERY_3_COL_1" FROM ( SELECT * FROM ( SELECT ( COUNT (
       | "SUBQUERY_0"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_1_COL_0",
       | ( COUNT ( "SUBQUERY_0"."SUBQUERY_1_COL_1" ) ) AS "SUBQUERY_1_COL_1",
       | ( "SUBQUERY_0"."SUBQUERY_1_COL_2" ) AS "SUBQUERY_1_COL_2"
       | FROM ( ( SELECT ( true ) AS "SUBQUERY_1_COL_0", ( NULL ) AS "SUBQUERY_1_COL_1",
       | ( CAST ( "SUBQUERY_0"."TESTBYTE" AS INTEGER ) ) AS "SUBQUERY_1_COL_2"
       | FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | UNION ALL ( SELECT ( NULL ) AS "SUBQUERY_1_COL_0", ( true ) AS "SUBQUERY_1_COL_1",
       | ( "SUBQUERY_0"."TESTINT" ) AS "SUBQUERY_1_COL_2" FROM ( SELECT * FROM $test_table
       | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) ) AS "SUBQUERY_0"
       | GROUP BY "SUBQUERY_0"."SUBQUERY_1_COL_2" ) AS "SUBQUERY_1"
       | WHERE ( ( "SUBQUERY_1"."SUBQUERY_1_COL_0" >= 1 ) AND
       | ( "SUBQUERY_1"."SUBQUERY_1_COL_1 " >= 1 ) ) ) AS "SUBQUERY_2" """.stripMargin
  )

  val testIntersect11: TestCase = TestCase(
    """SELECT test_table_2.testfloat FROM test_table_2 INTERSECT
      | SELECT test_table.testdouble FROM test_table
      | ORDER BY 1""".stripMargin,
    Seq(Row(null), Row(0.0.toFloat)),
    s"""SELECT ( "SUBQUERY_0"."TESTDOUBLE" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | """.stripMargin
  )

  val testIntersect12: TestCase = TestCase(
    """SELECT test_table_2.testfloat FROM test_table_2 INTERSECT DISTINCT
      | SELECT test_table.testdouble FROM test_table
      | ORDER BY 1""".stripMargin,
    Seq(Row(null), Row(0.0.toFloat)),
    s"""SELECT ( "SUBQUERY_0"."TESTDOUBLE" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | """.stripMargin
  )

  val testIntersect13: TestCase = TestCase(
    """SELECT test_table_2.testfloat FROM test_table_2 INTERSECT ALL
      | SELECT test_table.testdouble FROM test_table
      | ORDER BY 1""".stripMargin,
    Seq(Row(null), Row(0.0.toFloat)),
    s"""SELECT ( "SUBQUERY_2"."SUBQUERY_1_COL_2" ) AS "SUBQUERY_3_COL_0",
       | ( CASE WHEN ( "SUBQUERY_2"."SUBQUERY_1_COL_0" > "SUBQUERY_2"."SUBQUERY_1_COL_1" )
       | THEN "SUBQUERY_2"."SUBQUERY_1_COL_1" ELSE "SUBQUERY_2"."SUBQUERY_1_COL_0" END )
       | AS "SUBQUERY_3_COL_1" FROM ( SELECT * FROM ( SELECT ( COUNT (
       | "SUBQUERY_0"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_1_COL_0",
       | ( COUNT ( "SUBQUERY_0"."SUBQUERY_1_COL_1" ) ) AS "SUBQUERY_1_COL_1",
       | ( "SUBQUERY_0"."SUBQUERY_1_COL_2" ) AS "SUBQUERY_1_COL_2"
       | FROM ( ( SELECT ( true ) AS "SUBQUERY_1_COL_0", ( NULL ) AS "SUBQUERY_1_COL_1",
       | ( CAST ( "SUBQUERY_0"."TESTFLOAT" AS FLOAT8 ) ) AS "SUBQUERY_1_COL_2"
       | FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | UNION ALL ( SELECT ( NULL ) AS "SUBQUERY_1_COL_0", ( true ) AS "SUBQUERY_1_COL_1",
       | ( "SUBQUERY_0"."TESTDOUBLE" ) AS "SUBQUERY_1_COL_2" FROM ( SELECT * FROM $test_table
       | AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) ) AS "SUBQUERY_0"
       | GROUP BY "SUBQUERY_0"."SUBQUERY_1_COL_2" ) AS "SUBQUERY_1"
       | WHERE ( ( "SUBQUERY_1"."SUBQUERY_1_COL_0" >= 1 ) AND
       | ( "SUBQUERY_1"."SUBQUERY_1_COL_1 " >= 1 ) ) ) AS "SUBQUERY_2" """.stripMargin
  )

  test("Test INTERSECT logical plan operator with different column type", P1Test) {
    doTest(sqlContext, testIntersect01)
    doTest(sqlContext, testIntersect02)
    doTest(sqlContext, testIntersect03)
    doTest(sqlContext, testIntersect11)
    doTest(sqlContext, testIntersect12)
    doTest(sqlContext, testIntersect13)
  }
}

class TextPushdownLogicalPlanOperatorSuite extends PushdownLogicalPlanOperatorSuite {
  override protected val s3format: String = "TEXT"
}

class ParquetPushdownLogicalPlanOperatorSuite extends PushdownLogicalPlanOperatorSuite {
  override protected val s3format: String = "PARQUET"
}

class TextNoPushdownLogicalPlanOperatorSuite extends PushdownLogicalPlanOperatorSuite {
  override protected val auto_pushdown: String = "false"
  override protected val s3format: String = "TEXT"
}

class ParquetNoPushdownLogicalPlanOperatorSuite extends PushdownLogicalPlanOperatorSuite {
  override protected val auto_pushdown: String = "false"
  override protected val s3format: String = "PARQUET"
}

class TextNoCachePushdownLogicalPlanOperatorSuite
  extends TextPushdownLogicalPlanOperatorSuite {
  override protected val s3_result_cache = "false"
}

class ParquetNoCachePushdownLogicalPlanOperatorSuite
  extends ParquetPushdownLogicalPlanOperatorSuite {
  override protected val s3_result_cache = "false"
}
