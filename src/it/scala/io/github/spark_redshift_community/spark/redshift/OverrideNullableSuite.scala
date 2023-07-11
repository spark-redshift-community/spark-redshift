package io.github.spark_redshift_community.spark.redshift

import org.apache.spark.sql._
import org.apache.spark.sql.types._

trait OverrideNullableSuite extends IntegrationSuiteBase {
  test("read empty strings as null when overridenullable is true") {
    withTempRedshiftTable("overrideNullable") { name =>
      conn.createStatement().
        executeUpdate(s"create table $name (name text not null, nullable_name text)")
      conn.createStatement().executeUpdate(s"insert into $name values ('', '')")
      val df = read
        .option(Parameters.PARAM_OVERRIDE_NULLABLE, true)
        .option("dbtable", name)
        .load
      checkAnswer(
        df,
        Seq(Row(null, null))
      )
      assert(df.schema match {
        case StructType(Array(StructField(_, StringType, true, _),
        StructField(_, StringType, true, _))) => true
        case _ => false
      })
    }
  }

  test("read empty strings as null when overridenullable is true and unload_s3_format is TEXT") {
    withTempRedshiftTable("overrideNullable") { name =>
      conn.createStatement().
        executeUpdate(s"create table $name (name text not null, nullable_name text)")
      conn.createStatement().executeUpdate(s"insert into $name values ('', '')")
      val df = read
        .option(Parameters.PARAM_OVERRIDE_NULLABLE, true)
        .option("dbtable", name)
        .option("unload_s3_format", "TEXT")
        .load
      checkAnswer(
        df,
        Seq(Row(null, null))
      )
      assert(df.schema match {
        case StructType(Array(StructField(_, StringType, true, _),
        StructField(_, StringType, true, _))) => true
        case _ => false
      })
    }
  }

  test("read empty strings as empty strings when overridenullable is false") {
    withTempRedshiftTable("overrideNullable") { name =>
      conn.createStatement().
        executeUpdate(s"create table $name (name text not null, nullable_name text)")
      conn.createStatement().executeUpdate(s"insert into $name values ('', '')")
      val df = read
        .option(Parameters.PARAM_OVERRIDE_NULLABLE, false)
        .option("dbtable", name)
        .load
      checkAnswer(
        df,
        Seq(Row("", ""))
      )
      assert(df.schema match {
        case StructType(Array(StructField(_, StringType, false, _),
        StructField(_, StringType, true, _))) => true
        case _ => false
      })
    }
  }

  test("read empty strings as empty strings when overridenullable" +
    " is false and unload_s3_format is TEXT") {
    withTempRedshiftTable("overrideNullable") { name =>
      conn.createStatement().
        executeUpdate(s"create table $name (name text not null, nullable_name text)")
      conn.createStatement().executeUpdate(s"insert into $name values ('', '')")
      val df = read
        .option(Parameters.PARAM_OVERRIDE_NULLABLE, false)
        .option("dbtable", name)
        .option("unload_s3_format", "TEXT")
        .load
      checkAnswer(
        df,
        Seq(Row("", ""))
      )
      assert(df.schema match {
        case StructType(Array(StructField(_, StringType, false, _),
        StructField(_, StringType, true, _))) => true
        case _ => false
      })
    }
  }
}
