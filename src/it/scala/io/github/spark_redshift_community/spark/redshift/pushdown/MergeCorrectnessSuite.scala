package io.github.spark_redshift_community.spark.redshift.pushdown

import org.apache.spark.sql.Row

class MergeCorrectnessSuite extends IntegrationPushdownSuiteBase {
  // These tests cannot disable pushdown since delete happens in pushdown
  override protected val auto_pushdown: String = "true"
  // These tests cannot use cache since they check the result changing
  override val s3_result_cache: String = "false"
  override val s3format = "TEXT"

  def initialMergeTestData(sourceTable: String, targetTable: String): Unit = {
    redshiftWrapper.executeUpdate(conn,
      s"create table $targetTable (id smallint, status int, name varchar(50))"
    )
    redshiftWrapper.executeUpdate(conn,
      s"create table $sourceTable (new_id smallint, status int, name varchar(50))"
    )

    read.option("dbtable", targetTable).load.createOrReplaceTempView(targetTable)
    read.option("dbtable", sourceTable).load.createOrReplaceTempView(sourceTable)

    val initialTargetData = Seq(
      (1, 400, "john"),
      (2, 401, "sean"),
      (3, 402, "mike"),
      (4, 403, "nathan"),
      (5, 405, "victor")
    )
    val initialSourceData = Seq(
      (2, 501, "emily"),
      (4, 502, "emma"),
      (6, 503, "pam")
    )
    val targetSchema = List("id", "status", "name")
    val sourceSchema = List("new_id", "status", "name")
    val targetDf = sqlContext.createDataFrame(initialTargetData).toDF(targetSchema: _*)
    val sourceDf = sqlContext.createDataFrame(initialSourceData).toDF(sourceSchema: _*)

    write(targetDf).option("dbtable", targetTable).mode("append").save()
    write(sourceDf).option("dbtable", sourceTable).mode("append").save()
  }

 test("MERGE with DELETE and INSERT") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      sqlContext.sql(
        s"""
           |MERGE INTO $targetTable USING
           | $sourceTable ON
           | $targetTable.id = $sourceTable.new_id
           | WHEN MATCHED THEN DELETE
           | WHEN NOT MATCHED THEN INSERT (id, status, name) VALUES
           |    ($sourceTable.new_id, $sourceTable.status, $sourceTable.name)
           |""".stripMargin)

      checkAnswer(
       sqlContext.sql(s"select * from $targetTable"),
        Seq( Row(1, 400, "john"),
             Row(3, 402, "mike"),
             Row(5, 405, "victor"),
             Row(6, 503, "pam"))
      )
    }
  }

  test("MERGE with DELETE and INSERT from source and constant") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      sqlContext.sql(
        s"""
           |MERGE INTO $targetTable USING
           | $sourceTable ON
           | $targetTable.id = $sourceTable.new_id
           | WHEN MATCHED THEN DELETE
           | WHEN NOT MATCHED THEN INSERT (id,status,name) values ($sourceTable.new_id, -1, \'N/A\')
           |""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $targetTable"),
         Seq( Row(1, 400, "john"),
              Row(3, 402, "mike"),
              Row(5, 405, "victor"),
              Row(6, -1, "N/A"))
      )
    }
  }

  test("Merge with only DELETE") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      sqlContext.sql(
        s"""
           |MERGE INTO $targetTable USING
           | $sourceTable ON
           | $targetTable.id = $sourceTable.new_id
           | WHEN MATCHED THEN DELETE
           |""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $targetTable"),
        Seq( Row(1, 400, "john"),
          Row(3, 402, "mike"),
          Row(5, 405, "victor"))
      )
    }
  }

  ignore("Merge with only UPDATE") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      sqlContext.sql(
        s"""
           |MERGE INTO $targetTable USING
           | $sourceTable ON
           | $targetTable.id = $sourceTable.new_id
           | WHEN MATCHED THEN
           | UPDATE SET $targetTable.id = $sourceTable.new_id,
           |            $targetTable.status = $sourceTable.status,
           |            $targetTable.name = $sourceTable.name
           |""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $targetTable"),
        Seq( Row(1, 400, "john"),
          Row(2, 501, "emily"),
          Row(3, 402, "mike"),
          Row(4, 502, "emma"),
          Row(5, 405, "victor"))
      )
    }
  }

  test("Merge with only DELETE with source subquery") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      sqlContext.sql(
        s"""
           |MERGE INTO $targetTable USING
           | (SELECT new_id from $sourceTable where new_id > 3) ON
           | $targetTable.id = $sourceTable.new_id
           | WHEN MATCHED THEN DELETE
           |""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $targetTable"),
        Seq( Row(1, 400, "john"),
          Row(2, 401, "sean"),
          Row(3, 402, "mike"),
          Row(5, 405, "victor"))
      )
    }
  }
  
  ignore("Merge with source subquery") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      sqlContext.sql(
        s"""
           |MERGE INTO $targetTable USING
           | (SELECT new_id from $sourceTable GROUP BY new_id) as my_source
           | ON $targetTable.id = my_source.new_id
           | WHEN MATCHED THEN
           |    UPDATE SET id = my_source.new_id
           | WHEN NOT MATCHED THEN
           |    INSERT (id, status, name) VALUES
           |        (my_source.new_id, -1, \'N/A\');
           |""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $targetTable"),
        Seq( Row(1, 400, "john"),
          Row(2, 501, "sean"),
          Row(3, 402, "mike"),
          Row(4, 502, "nathan"),
          Row(5, 405, "victor"),
          Row(6, -1, "N/A"))
      )
    }
  }

  test("Basic_Merge") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      val query = s"""MERGE INTO $targetTable
                     |USING $sourceTable
                     |ON $targetTable.id = $sourceTable.new_id
                     |WHEN MATCHED THEN
                     |    UPDATE SET $targetTable.name = $sourceTable.name
                     |WHEN NOT MATCHED THEN
                     |    INSERT (id, status, name) VALUES
                     |    ($sourceTable.new_id, $sourceTable.status, $sourceTable.name)"""
        .stripMargin
      sqlContext.sql(query)

      checkAnswer(
        sqlContext.sql(s"select * from $targetTable"),
        Seq( Row(1, 400, "john"),
          Row(2, 401, "emily"),
          Row(3, 402, "mike"),
          Row(4, 403, "emma"),
          Row(5, 405, "victor"),
          Row(6, 503, "pam"))
      )
    }
  }

  test("Basic_Merge with alias") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      val query = s"""MERGE INTO $targetTable tt
                       |USING $sourceTable st
                       |ON tt.id = st.new_id
                       |WHEN MATCHED THEN
                       |    UPDATE SET tt.status = st.status, tt.name = st.name
                       |WHEN NOT MATCHED THEN
                       |    INSERT (id, status, name)
                       |    VALUES (st.new_id, st.status, st.name)""".stripMargin
      sqlContext.sql(query)

      checkAnswer(
        sqlContext.sql(s"select * from $targetTable"),
        Seq( Row(1, 400, "john"),
          Row(2, 501, "emily"),
          Row(3, 402, "mike"),
          Row(4, 502, "emma"),
          Row(5, 405, "victor"),
          Row(6, 503, "pam"))
      )
    }
  }

  test("Merge_NOT_MATCHED_BY_TARGET") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      val query =
        s"""MERGE INTO $targetTable
           |USING $sourceTable
           ON $targetTable.id = $sourceTable.new_id
           |WHEN MATCHED THEN
           |    UPDATE SET
           |     $targetTable.status = $sourceTable.status, $targetTable.name = $sourceTable.name
           |WHEN NOT MATCHED BY TARGET THEN
           |    INSERT (id, status, name) VALUES
           |     ($sourceTable.new_id, $sourceTable.status, $sourceTable.name)""".stripMargin
      sqlContext.sql(query)

      checkAnswer(
        sqlContext.sql(s"select * from $targetTable"),
        Seq( Row(1, 400, "john"),
          Row(2, 501, "emily"),
          Row(3, 402, "mike"),
          Row(4, 502, "emma"),
          Row(5, 405, "victor"),
          Row(6, 503, "pam"))
      )
    }
  }

  test("Negative test: conditional matched action not supported") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      val query =
        s"""MERGE INTO $targetTable
           |USING $sourceTable
           |ON $targetTable.id = $sourceTable.new_id
           |WHEN MATCHED AND $targetTable.id = 1 THEN
           |    UPDATE SET
           |     $targetTable.name = $sourceTable.name, $targetTable.status = $sourceTable.status
           |WHEN NOT MATCHED AND $sourceTable.new_id IN (4, 6) THEN
           |    INSERT (id, status, name) VALUES
           |     ($sourceTable.new_id, $sourceTable.status, $sourceTable.name)""".stripMargin

      try {
        sqlContext.sql(query)
      } catch {
        case e: Throwable =>
        assert(e.getMessage == "MERGE INTO TABLE is not supported temporarily.")
      }
    }
  }

  test("Negative test: NOT MATCH BY SOURCE action") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      val query = s"""MERGE INTO $targetTable
                     |USING $sourceTable
                     |ON $targetTable.id = $sourceTable.new_id
                     |WHEN NOT MATCHED BY SOURCE THEN DELETE;""".stripMargin

      try {
        sqlContext.sql(query)
      } catch {
        case e: Exception =>
          assert(e.getMessage.equals("MERGE INTO TABLE is not supported temporarily."))
      }
    }
  }
}
