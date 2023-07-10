package io.github.spark_redshift_community.spark.redshift.pushdown

import org.apache.spark.sql.Row

class StringSelectCorrectnessSuite extends StringIntegrationPushdownSuiteBase {

  test("Select StringType Column") {
    // (id, column, result)
    val paramTuples = List(
      (0, "testfixedstring", null),
      (0, "testvarstring", null),
      (1, "testfixedstring", "Hello World"),
      (1, "testvarstring", "Hello World"),
      (2, "testfixedstring", "Controls\t \b\n\r\f\\'\""),
      (2, "testvarstring", "Controls\t \b\n\r\f\\'\""),
      (3, "testfixedstring", "Specials/%"),
      (3, "testvarstring", "Specials/%"),
      (4, "testfixedstring", "Singl_Byte_Chars" ),
      (4, "testvarstring", "Multi樂Byte趣Chars"),
      (5, "testfixedstring", ""),
      (5, "testvarstring", ""),
      (6, "testfixedstring", "  Hello World"),
      (6, "testvarstring", "  Hello World  "),
      (7, "testfixedstring", "  \t\b\nFoo\r\f\\'\""),
      (7, "testvarstring", "  \t\b\nFoo\r\f\\'\"  "),
      (8, "testfixedstring", "  /%Foo%/"),
      (8, "testvarstring", "  /%Foo%/  "),
      (9, "testfixedstring", "  _Single_"),
      (9, "testvarstring", "  樂Multi趣  ")
    )

    paramTuples.foreach(paramTuple => {
      val id = paramTuple._1
      val column = paramTuple._2
      val result = paramTuple._3

      checkAnswer(
        sqlContext.sql(
          s"""SELECT $column FROM test_table WHERE testid=$id""".stripMargin),
        Seq(Row(result)))
    })
  }
}

class DefaultStringSelectCorrectnessSuite extends PushdownStringConcatSuite {
  override protected val s3format: String = "DEFAULT"
  override protected val auto_pushdown: String = "true"
}

class ParquetStringSelectCorrectnessSuite extends PushdownStringConcatSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "true"
}

class DefaultNoPushdownStringSelectCorrectnessSuite extends PushdownStringConcatSuite {
  override protected val s3format: String = "DEFAULT"
  override protected val auto_pushdown: String = "false"
}

class ParquetNoPushdownStringSelectCorrectnessSuite extends PushdownStringConcatSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}