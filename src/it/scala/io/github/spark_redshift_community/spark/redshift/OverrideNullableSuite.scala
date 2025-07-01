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
package io.github.spark_redshift_community.spark.redshift.test

import io.github.spark_redshift_community.spark.redshift.Parameters
import org.apache.spark.sql._
import org.apache.spark.sql.types._

trait OverrideNullableSuite extends IntegrationSuiteBase {
  test("read empty strings as null when overridenullable is true") {
    withTempRedshiftTable("overrideNullable") { name =>
      redshiftWrapper.executeUpdate(
        conn, s"create table $name (name text not null, nullable_name text)")
      redshiftWrapper.executeUpdate(conn, s"insert into $name values ('', '')")
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
      redshiftWrapper.executeUpdate(
        conn, s"create table $name (name text not null, nullable_name text)")
      redshiftWrapper.executeUpdate(conn, s"insert into $name values ('', '')")
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
      redshiftWrapper.executeUpdate(
        conn, s"create table $name (name text not null, nullable_name text)")
      redshiftWrapper.executeUpdate(conn, s"insert into $name values ('', '')")
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
      redshiftWrapper.executeUpdate(
        conn, s"create table $name (name text not null, nullable_name text)")
      redshiftWrapper.executeUpdate(conn, s"insert into $name values ('', '')")
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
