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

  test("MERGE with MATCHED No-Op and UNMATCHED INSERT") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      sqlContext.sql(
        s"""
           |MERGE INTO $targetTable USING
           | $sourceTable ON
           | $targetTable.id = $sourceTable.new_id
           | WHEN NOT MATCHED THEN INSERT (id, status, name) VALUES
           |    ($sourceTable.new_id, $sourceTable.status, $sourceTable.name)
           |""".stripMargin)

      checkSqlStatement(
        s""" MERGE INTO "PUBLIC"."$targetTable"
           | USING "PUBLIC"."$sourceTable"
           | ON ( "PUBLIC"."$targetTable"."ID" = "PUBLIC"."$sourceTable"."NEW_ID" )
           | WHEN MATCHED THEN UPDATE SET "ID" = "PUBLIC"."$targetTable"."ID"
           | WHEN NOT MATCHED THEN INSERT ("ID", "STATUS", "NAME") VALUES
           | ("PUBLIC"."$sourceTable"."NEW_ID", "PUBLIC"."$sourceTable"."STATUS",
           |  "PUBLIC"."$sourceTable"."NAME" ) """.stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $targetTable"),
        Seq( Row(1, 400, "john"),
          Row(2, 401, "sean"),
          Row(3, 402, "mike"),
          Row(4, 403, "nathan"),
          Row(5, 405, "victor"),
          Row(6, 503, "pam"))
      )
    }
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

      checkSqlStatement(
        s""" MERGE INTO "PUBLIC"."$targetTable" USING "PUBLIC"."$sourceTable"
           | ON ( "PUBLIC"."$targetTable"."ID" = "PUBLIC"."$sourceTable"."NEW_ID" )
           | WHEN MATCHED THEN DELETE
           | WHEN NOT MATCHED THEN INSERT ("ID", "STATUS", "NAME")
           | VALUES ("PUBLIC"."$sourceTable"."NEW_ID",
           |         "PUBLIC"."$sourceTable"."STATUS",
           |         "PUBLIC"."$sourceTable"."NAME" ) """.stripMargin)

      checkAnswer(
       sqlContext.sql(s"select * from $targetTable"),
        Seq( Row(1, 400, "john"),
             Row(3, 402, "mike"),
             Row(5, 405, "victor"),
             Row(6, 503, "pam"))
      )
    }
  }

  test("MERGE with redundant unmatched condition") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      sqlContext.sql(
        s"""
           |MERGE INTO $targetTable USING
           | $sourceTable ON
           | $targetTable.id = $sourceTable.new_id
           | WHEN MATCHED AND $sourceTable.new_id <= 3 THEN DELETE
           | WHEN MATCHED THEN DELETE
           | WHEN NOT MATCHED AND $sourceTable.new_id > 3 THEN INSERT (id, status, name) VALUES
           |    ($sourceTable.new_id, $sourceTable.status, $sourceTable.name)
           | WHEN NOT MATCHED THEN INSERT (id, status, name) VALUES
           |    ($sourceTable.new_id, $sourceTable.status, $sourceTable.name)
           |""".stripMargin)

      checkSqlStatement(
        s"""
           |MERGE INTO "PUBLIC"."$targetTable" USING "PUBLIC"."$sourceTable"
           | ON ( "PUBLIC"."$targetTable"."ID" = "PUBLIC"."$sourceTable"."NEW_ID" )
           | WHEN MATCHED THEN DELETE
           | WHEN NOT MATCHED THEN INSERT ("ID", "STATUS", "NAME") VALUES
           |     ("PUBLIC"."$sourceTable"."NEW_ID", "PUBLIC"."$sourceTable"."STATUS",
           |     "PUBLIC"."$sourceTable"."NAME" )
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

  test("negative test: MERGE with all unmatched action conditioned") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      try {
        sqlContext.sql(
          s"""
             |MERGE INTO $targetTable USING
             | $sourceTable ON
             | $targetTable.id = $sourceTable.new_id
             | WHEN MATCHED THEN DELETE
             | WHEN NOT MATCHED AND $sourceTable.new_id > 3 THEN INSERT (id, status, name) VALUES
             |    ($sourceTable.new_id, $sourceTable.status, $sourceTable.name)
             | WHEN NOT MATCHED AND $sourceTable.new_id <= 3 THEN INSERT (id, status, name) VALUES
             |    ($sourceTable.new_id, $sourceTable.status, $sourceTable.name)
             |""".stripMargin)
      } catch {
        case e: Throwable =>
          assert(e.getMessage.equals("MERGE INTO TABLE is not supported temporarily."))
      }
    }
  }

  test("MERGE with different ordering of unmatched action") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      sqlContext.sql(
        s"""
           |MERGE INTO $targetTable USING
           | $sourceTable ON
           | $targetTable.id = $sourceTable.new_id
           | WHEN MATCHED AND $sourceTable.new_id <= 3 THEN DELETE
           | WHEN MATCHED THEN DELETE
           | WHEN NOT MATCHED AND $sourceTable.new_id > 3 THEN INSERT (id, status, name) VALUES
           |    ($sourceTable.new_id, $sourceTable.status, $sourceTable.name)
           | WHEN NOT MATCHED THEN INSERT (status, name, id) VALUES
           |    ($sourceTable.status, $sourceTable.name, $sourceTable.new_id)
           |""".stripMargin)

      checkSqlStatement(
        s"""
           |MERGE INTO "PUBLIC"."$targetTable" USING "PUBLIC"."$sourceTable"
           | ON ( "PUBLIC"."$targetTable"."ID" = "PUBLIC"."$sourceTable"."NEW_ID" )
           | WHEN MATCHED THEN DELETE
           | WHEN NOT MATCHED THEN INSERT ("STATUS", "NAME", "ID") VALUES
           |     ("PUBLIC"."$sourceTable"."STATUS", "PUBLIC"."$sourceTable"."NAME",
           |     "PUBLIC"."$sourceTable"."NEW_ID" )
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

  test("MERGE with complex redundant INSERT") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      sqlContext.sql(
        s"""
           |MERGE INTO $targetTable USING
           | $sourceTable ON
           | $targetTable.id = $sourceTable.new_id
           | WHEN MATCHED AND $sourceTable.new_id <= 3 THEN DELETE
           | WHEN MATCHED THEN DELETE
           | WHEN NOT MATCHED AND $sourceTable.new_id > 3 THEN INSERT (id, status, name) VALUES
           |    ( $sourceTable.new_id + 10,
           |      CASE WHEN $sourceTable.status % 2 = 1 THEN 999 ELSE 888 END ,
           |      CONCAT($sourceTable.name,' JR'))
           | WHEN NOT MATCHED THEN INSERT (status, name, id) VALUES
           |    ( CASE WHEN $sourceTable.status % 2 = 1 THEN 999 ELSE 888 END ,
           |      CONCAT($sourceTable.name,' JR') ,
           |      $sourceTable.new_id + 10)
           |""".stripMargin)

      checkSqlStatement(
        s"""
           |MERGE INTO "PUBLIC"."$targetTable" USING "PUBLIC"."$sourceTable"
           | ON ( "PUBLIC"."$targetTable"."ID" = "PUBLIC"."$sourceTable"."NEW_ID" )
           | WHEN MATCHED THEN DELETE
           | WHEN NOT MATCHED THEN INSERT ("STATUS", "NAME", "ID") VALUES
           |  ( CASE WHEN ( ( "PUBLIC"."$sourceTable"."STATUS" % 2 ) = 1 ) THEN 999 ELSE 888 END,
           |    CONCAT ( "PUBLIC"."$sourceTable"."NAME" , ' JR' ),
           |    CAST ( ( CAST ( "PUBLIC"."$sourceTable"."NEW_ID" AS INTEGER ) + 10 ) AS SMALLINT))
           |""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $targetTable"),
        Seq( Row(1, 400, "john"),
          Row(3, 402, "mike"),
          Row(5, 405, "victor"),
          Row(16, 999, "pam JR"))
      )
    }
  }

  test("negative test: MERGE with complex irredundant INSERT") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      try {
        sqlContext.sql(
          s"""
             |MERGE INTO $targetTable USING
             | $sourceTable ON
             | $targetTable.id = $sourceTable.new_id
             | WHEN MATCHED AND $sourceTable.new_id <= 3 THEN DELETE
             | WHEN MATCHED THEN DELETE
             | WHEN NOT MATCHED AND $sourceTable.new_id > 3 THEN INSERT (id, status, name) VALUES
             |    ( $sourceTable.new_id + 10,
             |      CASE WHEN $sourceTable.status % 2 = 1 THEN 999 ELSE 888 END ,
             |      CONCAT($sourceTable.name,' JR.'))
             | WHEN NOT MATCHED THEN INSERT (status, name, id) VALUES
             |    ( CASE WHEN $sourceTable.status % 2 = 1 THEN 999 ELSE 888 END ,
             |      CONCAT($sourceTable.name,' JR') ,
             |      $sourceTable.new_id + 10)
             |""".stripMargin)
      } catch {
        case e: Throwable =>
          assert(e.getMessage.equals("MERGE INTO TABLE is not supported temporarily."))
      }
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

      checkSqlStatement(
        s""" MERGE INTO "PUBLIC"."$targetTable" USING "PUBLIC"."$sourceTable"
           | ON ( "PUBLIC"."$targetTable"."ID" = "PUBLIC"."$sourceTable"."NEW_ID" )
           | WHEN MATCHED THEN DELETE
           | WHEN NOT MATCHED THEN INSERT ("ID", "STATUS", "NAME")
           | VALUES ("PUBLIC"."$sourceTable"."NEW_ID", -1, 'N/A' ) """.stripMargin)

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

      checkSqlStatement(
        s""" DELETE FROM "PUBLIC"."$targetTable"
           | USING "PUBLIC"."$sourceTable"
           | WHERE ( "PUBLIC"."$targetTable"."ID" = "PUBLIC"."$sourceTable"."NEW_ID" )
           | """.stripMargin)

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

      checkSqlStatement(
        s""" DELETE FROM "PUBLIC"."$targetTable"
           | USING ( SELECT ( "SUBQUERY_2"."NEW_ID" ) AS "SUBQUERY_3_COL_0" FROM
           |  ( SELECT * FROM ( SELECT * FROM "PUBLIC"."$sourceTable" AS "RS_CONNECTOR_QUERY_ALIAS"
           |   ) AS "SUBQUERY_1" WHERE ( ( "SUBQUERY_1"."NEW_ID" IS NOT NULL ) AND
           |   ( "SUBQUERY_1"."NEW_ID" > 3 ) ) ) AS "SUBQUERY_2" ) AS "SUBQUERY_3"
           |  WHERE ( "PUBLIC"."$targetTable"."ID" = "SUBQUERY_3"."SUBQUERY_3_COL_0" )
           | """.stripMargin)

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

      checkSqlStatement(
        s""" MERGE INTO "PUBLIC"."$targetTable" USING "PUBLIC"."$sourceTable"
           | ON ( "PUBLIC"."$targetTable"."ID" = "PUBLIC"."$sourceTable"."NEW_ID" )
           | WHEN MATCHED THEN UPDATE SET "NAME" = "PUBLIC"."$sourceTable"."NAME"
           | WHEN NOT MATCHED THEN INSERT ("ID", "STATUS", "NAME")
           | VALUES ("PUBLIC"."$sourceTable"."NEW_ID",
           |         "PUBLIC"."$sourceTable"."STATUS",
           |         "PUBLIC"."$sourceTable"."NAME" ) """.stripMargin)

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

  test("Basic Merge with redundant UPDATE") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      val query = s"""MERGE INTO $targetTable
                     |USING $sourceTable
                     |ON $targetTable.id = $sourceTable.new_id
                     |WHEN MATCHED AND $sourceTable.new_id < 3 THEN
                     |    UPDATE SET $targetTable.name = $sourceTable.name
                     |WHEN MATCHED THEN
                     |    UPDATE SET $targetTable.name = $sourceTable.name
                     |WHEN NOT MATCHED THEN
                     |    INSERT (id, status, name) VALUES
                     |    ($sourceTable.new_id, $sourceTable.status, $sourceTable.name)"""
        .stripMargin
      sqlContext.sql(query)

      checkSqlStatement(
        s"""
           |MERGE INTO "PUBLIC"."$targetTable" USING "PUBLIC"."$sourceTable"
           | ON ( "PUBLIC"."$targetTable"."ID" = "PUBLIC"."$sourceTable"."NEW_ID" )
           | WHEN MATCHED THEN
           |     UPDATE SET "NAME" = "PUBLIC"."$sourceTable"."NAME"
           | WHEN NOT MATCHED THEN INSERT ("ID", "STATUS", "NAME") VALUES
           |     ("PUBLIC"."$sourceTable"."NEW_ID", "PUBLIC"."$sourceTable"."STATUS",
           |     "PUBLIC"."$sourceTable"."NAME" )
           |""".stripMargin)

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

  test("MERGE with complex redundant UPDATE") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      sqlContext.sql(
        s"""
           |MERGE INTO $targetTable USING
           | $sourceTable ON
           | $targetTable.id = $sourceTable.new_id
           | WHEN MATCHED AND $sourceTable.new_id < 3 THEN
           | UPDATE SET
           |    $targetTable.id = $sourceTable.new_id + 10,
           |    $targetTable.name = CONCAT($sourceTable.name,' II'),
           |    $targetTable.status = CASE WHEN $sourceTable.status % 2 = 1 THEN 999 ELSE 888 END
           | WHEN MATCHED THEN
           | UPDATE SET
           |    $targetTable.name = CONCAT($sourceTable.name,' II'),
           |    $targetTable.status = CASE WHEN $sourceTable.status % 2 = 1 THEN 999 ELSE 888 END,
           |    $targetTable.id = $sourceTable.new_id + 10
           | WHEN NOT MATCHED THEN INSERT (ID, STATUS, NAME) VALUES
           |     ($sourceTable.new_id, $sourceTable.status, $sourceTable.name )
           """.stripMargin)

      checkSqlStatement(
        s"""
           |MERGE INTO "PUBLIC"."$targetTable" USING "PUBLIC"."$sourceTable"
           | ON ( "PUBLIC"."$targetTable"."ID" = "PUBLIC"."$sourceTable"."NEW_ID" )
           | WHEN MATCHED THEN UPDATE SET
           |  "NAME" = CONCAT ( "PUBLIC"."$sourceTable"."NAME" , ' II' ),
           |  "STATUS" = CASE WHEN ( ( "PUBLIC"."$sourceTable"."STATUS" % 2 ) = 1 )
           |      THEN 999 ELSE 888 END,
           |  "ID" = CAST ( ( CAST ( "PUBLIC"."$sourceTable"."NEW_ID" AS INTEGER ) + 10 )
           |      AS SMALLINT )
           | WHEN NOT MATCHED THEN INSERT ("ID", "STATUS", "NAME")VALUES
           |    ("PUBLIC"."$sourceTable"."NEW_ID",
           |     "PUBLIC"."$sourceTable"."STATUS",
           |     "PUBLIC"."$sourceTable"."NAME" )
           |""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $targetTable"),
        Seq( Row(1, 400, "john"),
          Row(12, 999, "emily II"),
          Row(3, 402, "mike"),
          Row(14, 888, "emma II"),
          Row(5, 405, "victor"),
          Row(6, 503, "pam"))
      )
    }
  }

  test("negative test: MERGE with complex irredundant UPDATE") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      try {
        sqlContext.sql(
          s"""
             |MERGE INTO $targetTable USING
             | $sourceTable ON
             | $targetTable.id = $sourceTable.new_id
             | WHEN MATCHED AND $sourceTable.new_id < 3 THEN
             | UPDATE SET
             |    $targetTable.id = $sourceTable.new_id + 10,
             |    $targetTable.name = CONCAT($sourceTable.name,' II'),
             |    $targetTable.status = CASE WHEN $sourceTable.status % 2 = 1 THEN 999 ELSE 888 END
             | WHEN MATCHED THEN
             | UPDATE SET
             |    $targetTable.name = CONCAT($sourceTable.name,' II'),
             |    $targetTable.status = CASE WHEN $sourceTable.status % 2 = 0 THEN 999 ELSE 888 END,
             |    $targetTable.id = $sourceTable.new_id + 10
             | WHEN NOT MATCHED THEN INSERT (ID, STATUS, NAME) VALUES
             |     ($sourceTable.new_id, $sourceTable.status, $sourceTable.name )
           """.stripMargin)
      } catch {
        case e: Throwable =>
          assert(e.getMessage.equals("MERGE INTO TABLE is not supported temporarily."))
      }
    }
  }

  test("MERGE with alias complex redundant UPDATE") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      sqlContext.sql(
        s"""
           |MERGE INTO $targetTable USING
           | $sourceTable st ON
           | $targetTable.id = st.new_id
           | WHEN MATCHED AND st.new_id < 3 THEN
           | UPDATE SET
           |    $targetTable.id = st.new_id + 10,
           |    $targetTable.name = CONCAT(st.name,' II'),
           |    $targetTable.status = CASE WHEN st.status % 2 = 1 THEN 999 ELSE 888 END
           | WHEN MATCHED THEN
           | UPDATE SET
           |    $targetTable.name = CONCAT(st.name,' II'),
           |    $targetTable.status = CASE WHEN st.status % 2 = 1 THEN 999 ELSE 888 END,
           |    $targetTable.id = st.new_id + 10
           | WHEN NOT MATCHED THEN INSERT (ID, STATUS, NAME) VALUES
           |     (st.new_id, st.status, st.name )
           """.stripMargin)

      checkSqlStatement(
        s"""
           |MERGE INTO "PUBLIC"."$targetTable" USING "PUBLIC"."$sourceTable"
           | ON ( "PUBLIC"."$targetTable"."ID" = "PUBLIC"."$sourceTable"."NEW_ID" )
           | WHEN MATCHED THEN UPDATE SET
           |  "NAME" = CONCAT ( "PUBLIC"."$sourceTable"."NAME" , ' II' ),
           |  "STATUS" = CASE WHEN ( ( "PUBLIC"."$sourceTable"."STATUS" % 2 ) = 1 )
           |      THEN 999 ELSE 888 END,
           |  "ID" = CAST ( ( CAST ( "PUBLIC"."$sourceTable"."NEW_ID" AS INTEGER ) + 10 )
           |      AS SMALLINT )
           | WHEN NOT MATCHED THEN INSERT ("ID", "STATUS", "NAME")VALUES
           |    ("PUBLIC"."$sourceTable"."NEW_ID",
           |     "PUBLIC"."$sourceTable"."STATUS",
           |     "PUBLIC"."$sourceTable"."NAME" )
           |""".stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $targetTable"),
        Seq( Row(1, 400, "john"),
          Row(12, 999, "emily II"),
          Row(3, 402, "mike"),
          Row(14, 888, "emma II"),
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

      checkSqlStatement(
        s""" MERGE INTO "PUBLIC"."$targetTable" USING "PUBLIC"."$sourceTable"
           | ON ( "PUBLIC"."$targetTable"."ID" = "PUBLIC"."$sourceTable"."NEW_ID" )
           | WHEN MATCHED THEN UPDATE
           |    SET "STATUS" = "PUBLIC"."$sourceTable"."STATUS",
           |        "NAME" = "PUBLIC"."$sourceTable"."NAME"
           | WHEN NOT MATCHED THEN INSERT ("ID", "STATUS", "NAME")
           | VALUES ("PUBLIC"."$sourceTable"."NEW_ID",
           |         "PUBLIC"."$sourceTable"."STATUS",
           |         "PUBLIC"."$sourceTable"."NAME" ) """.stripMargin)

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

  test("Basic_Merge with mixed source and target alias in UPDATE") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      val query = s"""MERGE INTO $targetTable tt
                     |USING $sourceTable st
                     |ON tt.id = st.new_id
                     |WHEN MATCHED THEN
                     |    UPDATE SET tt.status = tt.status + st.status + 1000, tt.name = st.name
                     |WHEN NOT MATCHED THEN
                     |    INSERT (id, status, name)
                     |    VALUES (st.new_id, st.status, st.name)""".stripMargin
      sqlContext.sql(query)

      checkSqlStatement(
        s""" MERGE INTO "PUBLIC"."$targetTable"
           | USING "PUBLIC"."$sourceTable"
           | ON ( "PUBLIC"."$targetTable"."ID" =
           |            "PUBLIC"."$sourceTable"."NEW_ID" )
           | WHEN MATCHED THEN
           |    UPDATE SET "STATUS" = ( ( "PUBLIC"."$targetTable"."STATUS" +
           |                            "PUBLIC"."$sourceTable"."STATUS" ) + 1000 ),
           |               "NAME" = "PUBLIC"."$sourceTable"."NAME"
           | WHEN NOT MATCHED THEN INSERT ("ID", "STATUS", "NAME") VALUES
           |    ("PUBLIC"."$sourceTable"."NEW_ID",
           |     "PUBLIC"."$sourceTable"."STATUS",
           |     "PUBLIC"."$sourceTable"."NAME" ) """.stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $targetTable"),
        Seq( Row(1, 400, "john"),
          Row(2, 1902, "emily"),
          Row(3, 402, "mike"),
          Row(4, 1905, "emma"),
          Row(5, 405, "victor"),
          Row(6, 503, "pam"))
      )
    }
  }

  test("Basic_Merge with mixed source and target column in UPDATE") {
    withTwoTempRedshiftTables("sourceTable", "targetTable") { (sourceTable, targetTable) =>
      initialMergeTestData(sourceTable, targetTable)
      val query = s"""MERGE INTO $targetTable tt
                     |USING $sourceTable st
                     |ON tt.id = st.new_id
                     |WHEN MATCHED THEN
                     |    UPDATE SET tt.status = tt.status + st.status + Length(tt.name)
                     |          + st.new_id, tt.name = st.name
                     |WHEN NOT MATCHED THEN
                     |    INSERT (id, status, name)
                     |    VALUES (st.new_id, st.status, st.name)""".stripMargin
      sqlContext.sql(query)

      checkSqlStatement(
        s""" MERGE INTO "PUBLIC"."$targetTable"
           | USING "PUBLIC"."$sourceTable"
           | ON ( "PUBLIC"."$targetTable"."ID" =
           |            "PUBLIC"."$sourceTable"."NEW_ID" )
           | WHEN MATCHED THEN
           |    UPDATE SET "STATUS" = ( ( ( "PUBLIC"."$targetTable"."STATUS" +
           |                            "PUBLIC"."$sourceTable"."STATUS" ) +
           |                            LENGTH ( "PUBLIC"."$targetTable"."NAME" ) ) +
           |                            CAST ( "PUBLIC"."$sourceTable"."NEW_ID" AS INTEGER ) ),
           |               "NAME" = "PUBLIC"."$sourceTable"."NAME"
           | WHEN NOT MATCHED THEN INSERT ("ID", "STATUS", "NAME") VALUES
           |    ("PUBLIC"."$sourceTable"."NEW_ID",
           |     "PUBLIC"."$sourceTable"."STATUS",
           |     "PUBLIC"."$sourceTable"."NAME" ) """.stripMargin)

      checkAnswer(
        sqlContext.sql(s"select * from $targetTable"),
        Seq( Row(1, 400, "john"),
          Row(2, 908, "emily"),
          Row(3, 402, "mike"),
          Row(4, 915, "emma"),
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

      checkSqlStatement(
        s""" MERGE INTO "PUBLIC"."$targetTable" USING "PUBLIC"."$sourceTable"
           | ON ( "PUBLIC"."$targetTable"."ID" = "PUBLIC"."$sourceTable"."NEW_ID" )
           | WHEN MATCHED THEN UPDATE
           |    SET "STATUS" = "PUBLIC"."$sourceTable"."STATUS",
           |        "NAME" = "PUBLIC"."$sourceTable"."NAME"
           | WHEN NOT MATCHED THEN INSERT ("ID", "STATUS", "NAME")
           | VALUES ("PUBLIC"."$sourceTable"."NEW_ID",
           |         "PUBLIC"."$sourceTable"."STATUS",
           |         "PUBLIC"."$sourceTable"."NAME" ) """.stripMargin)

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
