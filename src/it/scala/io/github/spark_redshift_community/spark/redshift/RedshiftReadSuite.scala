/*
 * Copyright 2016 Databricks
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package io.github.spark_redshift_community.spark.redshift.test

import io.github.spark_redshift_community.spark.redshift.Parameters
import java.sql.Timestamp
import org.apache.spark.sql.types.{CalendarIntervalType, ByteType, DoubleType, FloatType, IntegerType, LongType, ShortType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, Row, execution}

/**
 * End-to-end tests of functionality which only impacts the read path (e.g. filter pushdown).
 */
class RedshiftReadSuite extends IntegrationSuiteBase with OverrideNullableSuite {

  private val test_table: String = s"read_suite_test_table_$randomSuffix"

  override def beforeAll(): Unit = {
    super.beforeAll()
    redshiftWrapper.executeUpdate(conn, s"drop table if exists $test_table")
    createTestDataInRedshift(test_table)
  }

  override def afterAll(): Unit = {
    try {
      redshiftWrapper.executeUpdate(conn, s"drop table if exists $test_table")
    } finally {
      super.afterAll()
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    read.option("dbtable", test_table).load().createOrReplaceTempView("test_table")
  }

  protected def createTestRealDataInRedshift(tableName: String): Unit = {
    redshiftWrapper.executeUpdate(conn,
      s"""
         | create table $tableName (
         | testreal real
         | )
         |""".stripMargin
    )

    redshiftWrapper.executeUpdate(conn,
      s"""insert into $tableName values
         | (1.0), (null), (-1.0)
         |""".stripMargin
    )
  }
  protected def createSmallintDataInRedshift(tableName: String): Unit = {
    redshiftWrapper.executeUpdate(conn,
      s"""
         | create table $tableName (
         | test_smallint smallint
         | )
         |""".stripMargin
    )

    redshiftWrapper.executeUpdate(conn,
      s"""insert into $tableName values
         | (1), (2), (32767), (-32768)
         |""".stripMargin
    )
  }


  test("DefaultSource can load Redshift UNLOAD output to a DataFrame") {
    checkAnswer(
      sqlContext.sql("select * from test_table"),
      TestUtils.expectedData)
  }

  test("count() on DataFrame created from a Redshift table") {
    checkAnswer(
      sqlContext.sql("select count(*) from test_table"),
      Seq(Row(TestUtils.expectedData.length))
    )
  }

  test("count() on DataFrame created from a Redshift query") {
    val loadedDf =
    // scalastyle:off
      read.option("query", s"select * from $test_table where teststring = 'Unicode''s樂趣'").load()
    // scalastyle:on
    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(1))
    )
  }

  test("backslashes in queries/subqueries are escaped (regression test for #215)") {
    val loadedDf =
      read.option("query", s"select replace(teststring, '\\\\', '') as col from $test_table").load()
    checkAnswer(
      loadedDf.filter("col = 'asdf'"),
      Seq(Row("asdf"))
    )
  }

  test("Can load output when 'dbtable' is a subquery wrapped in parentheses") {
    // scalastyle:off
    val query =
      s"""
         |(select testbyte, testbool
         |from $test_table
         |where testbool = true
         | and teststring = 'Unicode''s樂趣'
         | and testdouble = 1234152.12312498
         | and testfloat = 1.0
         | and testint = 42)
      """.stripMargin
    // scalastyle:on
    checkAnswer(read.option("dbtable", query).load(), Seq(Row(1, true)))
  }

  test("Can load output when 'query' is specified instead of 'dbtable'") {
    // scalastyle:off
    val query =
      s"""
         |select testbyte, testbool
         |from $test_table
         |where testbool = true
         | and teststring = 'Unicode''s樂趣'
         | and testdouble = 1234152.12312498
         | and testfloat = 1.0
         | and testint = 42
      """.stripMargin
    // scalastyle:on
    checkAnswer(read.option("query", query).load(), Seq(Row(1, true)))
  }

  test("Can load output of Redshift aggregation queries") {
    checkAnswer(
      read.option("query", s"select testbool, count(*) from $test_table group by testbool").load(),
      Seq(Row(true, 1), Row(false, 2), Row(null, 2)))
  }

  test("multiple scans on same table") {
    // .rdd() forces the first query to be unloaded from Redshift
    val rdd1 = sqlContext.sql("select testint from test_table").rdd
    // Similarly, this also forces an unload:
    sqlContext.sql("select testdouble from test_table").rdd
    // If the unloads were performed into the same directory then this call would fail: the
    // second unload from rdd2 would have overwritten the integers with doubles, so we'd get
    // a NumberFormatException.
    rdd1.count()
  }

  test("DefaultSource supports simple column filtering") {
    checkAnswer(
      sqlContext.sql("select testbyte, testbool from test_table"),
      Seq(
        Row(null, null),
        Row(0.toByte, null),
        Row(0.toByte, false),
        Row(1.toByte, false),
        Row(1.toByte, true)))
  }

  test("query with pruned and filtered scans") {
    // scalastyle:off
    checkAnswer(
      sqlContext.sql(
        """
          |select testbyte, testbool
          |from test_table
          |where testbool = true
          | and teststring = "Unicode's樂趣"
          | and testdouble = 1234152.12312498
          | and testfloat = 1.0
          | and testint = 42
        """.stripMargin),
      Seq(Row(1, true)))
    // scalastyle:on
  }

  test("RedshiftRelation implements Spark 1.6+'s unhandledFilters API") {
    assume(org.apache.spark.SPARK_VERSION.take(3) >= "1.6")
    val df = sqlContext.sql("select testbool from test_table where testbool = true")
    val physicalPlan = df.queryExecution.sparkPlan
    physicalPlan.collectFirst { case f: execution.FilterExec => f }.foreach { filter =>
      fail(s"Filter should have been eliminated:\n${df.queryExecution}")
    }
  }

  test("filtering based on date constants (regression test for #152)") {
    val date = TestUtils.toDate(year = 2015, zeroBasedMonth = 6, date = 3)
    val df = sqlContext.sql("select testdate from test_table")

    checkAnswer(df.filter(df("testdate") === date), Seq(Row(date)))
    // This query failed in Spark 1.6.0 but not in earlier versions. It looks like 1.6.0 performs
    // constant-folding, whereas earlier Spark versions would preserve the cast which prevented
    // filter pushdown.
    checkAnswer(df.filter("testdate = to_date('2015-07-03')"), Seq(Row(date)))
  }

  test("filtering based on timestamp constants (regression test for #152)") {
    val timestamp = TestUtils.toTimestamp(2015, zeroBasedMonth = 6, 1, 0, 0, 0, 1)
    val df = sqlContext.sql("select testtimestamp from test_table")

    checkAnswer(df.filter(df("testtimestamp") === timestamp), Seq(Row(timestamp)))
    // This query failed in Spark 1.6.0 but not in earlier versions. It looks like 1.6.0 performs
    // constant-folding, whereas earlier Spark versions would preserve the cast which prevented
    // filter pushdown.
    checkAnswer(df.filter("testtimestamp = '2015-07-01 00:00:00.001'"), Seq(Row(timestamp)))
  }

  test("read special float values (regression test for #261)") {
    val tableName = s"roundtrip_special_float_values_$randomSuffix"
    try {
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE $tableName (x real)")
      redshiftWrapper.executeUpdate(conn,
        s"INSERT INTO $tableName VALUES ('NaN'), ('Infinity'), ('-Infinity')")
      assert(redshiftWrapper.tableExists(conn, tableName))
      checkAnswer(
        read.option("dbtable", tableName).load(),
        Seq(Float.NaN, Float.PositiveInfinity, Float.NegativeInfinity).map(x => Row.apply(x)))
    } finally {
      redshiftWrapper.executeUpdate(conn, s"drop table if exists $tableName")
    }
  }

  test("test empty string and null") {
    withTempRedshiftTable("records_with_empty_and_null_characters") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE $tableName (x varchar(256))")
      redshiftWrapper.executeUpdate(conn,
        s"INSERT INTO $tableName VALUES ('null'), (''), (null)")
      assert(redshiftWrapper.tableExists(conn, tableName))
      checkAnswer(
        read.option("dbtable", tableName).load(),
        Seq("null", "", null).map(x => Row.apply(x)))
    }
  }

  test("test timestamptz parsing") {
    withTempRedshiftTable("luca_test_timestamptz_spark_redshift") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE $tableName (x timestamptz)"
      )
      redshiftWrapper.executeUpdate(conn,
        s"INSERT INTO $tableName VALUES ('2015-07-03 00:00:00.000 -0300')"
      )

      checkAnswer(
        read.option("dbtable", tableName).load(),
        Seq(Row.apply(
          new Timestamp(TestUtils.toMillis(
      2015, 6, 3, 0, 0, 0, 0, "-03"))))
      )
    }
  }

  test("read special double values (regression test for #261)") {
    val tableName = s"roundtrip_special_double_values_$randomSuffix"
    try {
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE $tableName (x double precision)")
      redshiftWrapper.executeUpdate(conn,
        s"INSERT INTO $tableName VALUES ('NaN'), ('Infinity'), ('-Infinity')")
      assert(redshiftWrapper.tableExists(conn, tableName))
      checkAnswer(
        read.option("dbtable", tableName).load(),
        Seq(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity).map(x => Row.apply(x)))
    } finally {
      redshiftWrapper.executeUpdate(conn, s"drop table if exists $tableName")
    }
  }

  test("read records containing escaped characters") {
    withTempRedshiftTable("records_with_escaped_characters") { tableName =>
      redshiftWrapper.executeUpdate(conn,
        s"CREATE TABLE $tableName (x text)")
      redshiftWrapper.executeUpdate(conn,
        s"""INSERT INTO $tableName VALUES ('a\\nb'), ('\\\\'), ('"')""")
      assert(redshiftWrapper.tableExists(conn, tableName))
      checkAnswer(
        read.option("dbtable", tableName).load(),
        Seq("a\nb", "\\", "\"").map(x => Row.apply(x)))
    }
  }

  test("read result of approximate count(distinct) query (#300)") {
    val df = read
      .option("query", s"select approximate count(distinct testbool) as c from $test_table")
      .load()
    assert(df.schema.fields(0).dataType === LongType)
  }

  test("read reals as floats by default") {
    withTempRedshiftTable("readRealsDefault") { tableName =>
      createTestRealDataInRedshift(tableName)
      val df = read
        .option("dbtable", tableName).load()
      assert(df.schema match {
        case StructType(Array(StructField(_, FloatType, _, _))) => true
        case _ => false
      })
    }
  }

  test("read reals as floats when legacy_jdbc_real_type_mapping option is false") {
    withTempRedshiftTable("readRealsLegacyFalse") { tableName =>
      createTestRealDataInRedshift(tableName)
      val df = read
        .option("dbtable", tableName)
        .option(Parameters.PARAM_LEGACY_JDBC_REAL_TYPE_MAPPING, value = false)
        .load()
      assert(df.schema match {
        case StructType(Array(StructField(_, FloatType, _, _))) => true
        case _ => false
      })
    }
  }

  test("read reals as doubles when legacy_jdbc_real_type_mapping option is true") {
    withTempRedshiftTable("readRealsLegacyTrue") { tableName =>
      createTestRealDataInRedshift(tableName)
      val df = read
        .option("dbtable", tableName)
        .option(Parameters.PARAM_LEGACY_JDBC_REAL_TYPE_MAPPING, value = true)
        .load()
      assert(df.schema match {
        case StructType(Array(StructField(_, DoubleType, _, _))) => true
        case _ => false
      })
    }
  }

  test("read smallint as ShortType by default") {
    withTempRedshiftTable("readSmallintDefault") { tableName =>
      createSmallintDataInRedshift(tableName)
      val df = read
        .option("dbtable", tableName).load()
      assert(df.schema match {
        case StructType(Array(StructField(_, ShortType, _, _))) => true
        case _ => false
      })
    }
  }

  test("read smallint as ShortType when legacy_mapping_short_to_int option is false") {
    withTempRedshiftTable("readSmallintFalse") { tableName =>
      createSmallintDataInRedshift(tableName)
      val df = read
        .option("dbtable", tableName)
        .option(Parameters.PARAM_LEGACY_MAPPING_SHORT_TO_INT, value = false)
        .load()
      assert(df.schema match {
        case StructType(Array(StructField(_, ShortType, _, _))) => true
        case _ => false
      })
    }
  }

  test("read smallint as IntegerType when legacy_mapping_short_to_int option is true") {
    withTempRedshiftTable("readSmallintTrue") { tableName =>
      createSmallintDataInRedshift(tableName)
      val df = read
        .option("dbtable", tableName)
        .option(Parameters.PARAM_LEGACY_MAPPING_SHORT_TO_INT, value = true)
        .load()
      assert(df.schema match {
        case StructType(Array(StructField(_, IntegerType, _, _))) => true
        case _ => false
      })
    }
  }

  test("reads with unsupported type in schema throws") {
    // Tests with text unload format as parquet implements its own check for supported data types
    assertThrows[AnalysisException] {
      withTempRedshiftTable("testTable") { name =>
        redshiftWrapper.executeUpdate(conn, s"create table $name (id integer)")
        val invalidSchema = StructType(StructField("id", CalendarIntervalType) :: Nil)
        read
          .schema(invalidSchema)
          .option("unload_s3_format", "TEXT")
          .option("dbtable", name)
          .load
          .collect
      }
    }
  }
}