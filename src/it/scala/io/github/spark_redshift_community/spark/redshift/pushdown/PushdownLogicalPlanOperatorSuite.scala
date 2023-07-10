package io.github.spark_redshift_community.spark.redshift.pushdown

import org.apache.spark.sql.Row

abstract class PushdownLogicalPlanOperatorSuite extends IntegrationPushdownSuiteBase {
  private val test_table_2: String = s""""PUBLIC"."pushdown_suite_test_table2_$randomSuffix""""
  override def beforeAll(): Unit = {
    super.beforeAll()
    if (!preloaded_data.toBoolean) {
      conn.prepareStatement(s"drop table if exists $test_table_2").executeUpdate()
      createMoreDataInRedshift(test_table_2)
    }
  }

  override def afterAll(): Unit = {
    try {
      if (!preloaded_data.toBoolean) {
        conn.prepareStatement(s"drop table if exists $test_table_2").executeUpdate()
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
    conn.createStatement().executeUpdate(
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
    conn.createStatement().executeUpdate(
      s"""
         |insert into $tableName values
         |(null, null, null, null, null, null, null, null, null, null),
         |(0, null, '2015-07-02', 0.0, -1.0, 216, 54321, null, 'f', '2015-07-03 00:00:00.000'),
         |(1, false, null, -1234152.12312498, 100.0, null, 1239012341823719, 24, '___|_123', null),
         |(2, false, '2015-07-03', 1.1, 0.0, 42, 1239012341823719, -13, 'asdf', '2015-07-02 00:00:00.000'),
         |(3, true, '2015-07-04', 1234152.12312498, 2.0, 42, 1239012341823719, 23, 'Unicode''s樂趣', '2015-07-01 00:00:00.001')
         """.stripMargin
    )
    // scalastyle:on
  }

  val testJoin1: TestCase = TestCase (
    """SELECT test_table_2.testbyte FROM test_table JOIN test_table_2
      | ON test_table.testbyte = test_table_2.testbyte order by 1""".stripMargin,
    Seq(Row(0), Row(0), Row(1), Row(1)),
    s"""SELECT * FROM ( SELECT ( "SUBQUERY_6"."SUBQUERY_6_COL_1" ) AS "SUBQUERY_7_COL_0"
       | FROM ( SELECT ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) AS "SUBQUERY_6_COL_0",
       | ( "SUBQUERY_5"."SUBQUERY_5_COL_0" ) AS "SUBQUERY_6_COL_1"
       | FROM (SELECT ( "SUBQUERY_1"."TESTBYTE" ) AS "SUBQUERY_2_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_0" WHERE ( "SUBQUERY_0"."TESTBYTE" IS NOT NULL ) ) AS"SUBQUERY_1" )
       | AS "SUBQUERY_2" INNER JOIN ( SELECT ( "SUBQUERY_4"."TESTBYTE" ) AS "SUBQUERY_5_COL_0"
       | FROM ( SELECT * FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_3" WHERE ( "SUBQUERY_3"."TESTBYTE" IS NOT NULL ) ) AS "SUBQUERY_4" )
       | AS "SUBQUERY_5" ON ( "SUBQUERY_2"."SUBQUERY_2_COL_0" = "SUBQUERY_5"."SUBQUERY_5_COL_0" ) )
       | AS "SUBQUERY_6" ) AS "SUBQUERY_7" ORDER BY ( "SUBQUERY_7"."SUBQUERY_7_COL_0" ) ASC
       | NULLS FIRST""".stripMargin
  )

  // DISTINCT on joined result cannot be pushed down.
  val testJoin2: TestCase = TestCase(
    """SELECT DISTINCT test_table_2.testbyte FROM test_table JOIN test_table_2
      | ON test_table.testbyte = test_table_2.testbyte order by 1""".stripMargin,
    Seq(Row(0), Row(1)),
    s"""SELECT * FROM ( SELECT ( "SUBQUERY_7"."SUBQUERY_7_COL_0" ) AS "SUBQUERY_8_COL_0"
       | FROM ( SELECT ( "SUBQUERY_6"."SUBQUERY_6_COL_1" ) AS "SUBQUERY_7_COL_0"
       | FROM ( SELECT ( "SUBQUERY_2"."SUBQUERY_2_COL_0" ) AS "SUBQUERY_6_COL_0",
       | ( "SUBQUERY_5"."SUBQUERY_5_COL_0" ) AS "SUBQUERY_6_COL_1" FROM (
       | SELECT ( "SUBQUERY_1"."TESTBYTE" ) AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM (
       | SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | WHERE ( "SUBQUERY_0"."TESTBYTE" IS NOT NULL ) ) AS "SUBQUERY_1" ) AS "SUBQUERY_2"
       | INNER JOIN ( SELECT ( "SUBQUERY_4"."TESTBYTE" ) AS "SUBQUERY_5_COL_0" FROM (
       | SELECT * FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" )
       | AS "SUBQUERY_3" WHERE ( "SUBQUERY_3"."TESTBYTE" IS NOT NULL ) ) AS "SUBQUERY_4" )
       | AS "SUBQUERY_5" ON ( "SUBQUERY_2"."SUBQUERY_2_COL_0" = "SUBQUERY_5"."SUBQUERY_5_COL_0" ) )
       | AS "SUBQUERY_6" ) AS "SUBQUERY_7" GROUP BY "SUBQUERY_7"."SUBQUERY_7_COL_0" )
       | AS "SUBQUERY_8" ORDER BY ( "SUBQUERY_8"."SUBQUERY_8_COL_0" ) ASC
       | NULLS FIRST""".stripMargin
  )

  test("Test JOIN logical plan operator") {
    doTest(sqlContext, testJoin1)
    doTest(sqlContext, testJoin2)
  }

  val testUnion1: TestCase = TestCase(
    """SELECT test_table.testbyte FROM test_table UNION
      | SELECT test_table_2.testbyte FROM test_table_2
      | ORDER BY 1""".stripMargin,
    Seq(Row(null), Row(0), Row(1), Row(2), Row(3)),
    s"""SELECT * FROM ( SELECT ( "SUBQUERY_0"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_1_COL_0"
       | FROM ( ( SELECT ( "SUBQUERY_0"."TESTBYTE" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | UNION ALL ( SELECT ( "SUBQUERY_0"."TESTBYTE" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) )
       | AS "SUBQUERY_0" GROUP BY "SUBQUERY_0"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_1" ORDER BY
       | ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ASC NULLS FIRST""".stripMargin
  )

  val testUnion2: TestCase = TestCase(
    """SELECT test_table.testbyte FROM test_table UNION DISTINCT
      | SELECT test_table_2.testbyte FROM test_table_2
      | ORDER BY 1""".stripMargin,
    Seq(Row(null), Row(0), Row(1), Row(2), Row(3)),
    s"""SELECT * FROM ( SELECT ( "SUBQUERY_0"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_1_COL_0"
       | FROM ( ( SELECT ( "SUBQUERY_0"."TESTBYTE" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | UNION ALL ( SELECT ( "SUBQUERY_0"."TESTBYTE" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" ) )
       | AS "SUBQUERY_0" GROUP BY "SUBQUERY_0"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_1" ORDER BY
       | ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ASC NULLS FIRST""".stripMargin
  )

  val testUnion3: TestCase = TestCase(
    """SELECT test_table.testbyte FROM test_table UNION ALL
      | SELECT test_table_2.testbyte FROM test_table_2
      | ORDER BY 1""".stripMargin,
    Seq(Row(null), Row(null), Row(0), Row(0), Row(0), Row(1), Row(1), Row(1), Row(2), Row(3)),
    s"""SELECT * FROM ( ( SELECT ( "SUBQUERY_0"."TESTBYTE" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" )
       | UNION ALL ( SELECT ( "SUBQUERY_0"."TESTBYTE" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table_2 AS "RS_CONNECTOR_QUERY_ALIAS" ) AS"SUBQUERY_0" ) )
       | AS "SUBQUERY_0" ORDER BY ( "SUBQUERY_0"."SUBQUERY_1_COL_0" ) ASC NULLS FIRST""".stripMargin
  )

  test("Test UNION logical plan operator") {
    doTest(sqlContext, testUnion1)
    doTest(sqlContext, testUnion2)
    doTest(sqlContext, testUnion3)
  }

  // No push down for except
  val testExcept1: TestCase = TestCase(
    """SELECT test_table_2.testbyte FROM test_table_2 EXCEPT
      | SELECT test_table.testbyte FROM test_table
      | ORDER BY 1""".stripMargin,
    Seq(Row(2), Row(3)),
    s"""SELECT ( "SUBQUERY_0"."TESTBYTE" ) AS "SUBQUERY_1_COL_0"
       | FROM ( SELECT * FROM $test_table AS "RS_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
       | """.stripMargin
  )

  test("Test EXCEPT logical plan operator") {
    doTest(sqlContext, testExcept1)
  }
}


class DefaultPushdownLogicalPlanOperatorSuite extends PushdownLogicalPlanOperatorSuite {
  override protected val s3format: String = "DEFAULT"
}

class ParquetPushdownLogicalPlanOperatorSuite extends PushdownLogicalPlanOperatorSuite {
  override protected val s3format: String = "PARQUET"
}

class DefaultNoPushdownLogicalPlanOperatorSuite extends PushdownLogicalPlanOperatorSuite {
  override protected val auto_pushdown: String = "false"
  override protected val s3format: String = "DEFAULT"
}