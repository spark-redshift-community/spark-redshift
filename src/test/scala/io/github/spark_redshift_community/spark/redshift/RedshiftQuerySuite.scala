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

import io.github.spark_redshift_community.spark.redshift.pushdown.RedshiftScanExec
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class RedshiftQuerySuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("test q1") {
    spark.sql("""
      create table student(id int)
       using io.github.spark_redshift_community.spark.redshift
       OPTIONS (
      dbtable 'public.parquet_struct_table_view',
      tempdir '/tmp/dir',
      url '<jdbc-url-in-iam scheme>',
      forward_spark_s3_credentials 'true'
    )
      """).show()

    val df = spark.sql(
      """
        |SELECT * FROM student
        |""".stripMargin)
    val plan = df.queryExecution.executedPlan

    assert(plan.isInstanceOf[RedshiftScanExec])
    val rsPlan = plan.asInstanceOf[RedshiftScanExec]
    assert(rsPlan.query.statementString ==
      """SELECT * FROM "public"."parquet_struct_table_view" AS "RCQ_ALIAS""""
        .stripMargin)
  }

}
