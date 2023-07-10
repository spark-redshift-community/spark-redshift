package io.github.spark_redshift_community.spark.redshift.pushdown

class StringIntegrationPushdownSuiteBase extends IntegrationPushdownSuiteBase {
  override def createTestDataInRedshift(tableName: String): Unit = {
    conn.createStatement().executeUpdate(
      s"""
         |create table $tableName (
         |testid int,
         |testbyte int2,
         |testbool boolean,
         |testdate date,
         |testdouble float8,
         |testfloat float4,
         |testint int4,
         |testlong int8,
         |testshort int2,
         |teststring varchar(256),
         |testfixedstring char(256),
         |testvarstring varchar(256),
         |testtimestamp timestamp
         |)
    """.stripMargin
    )
    // scalastyle:off
    conn.createStatement().executeUpdate(
      s"""
         |insert into $tableName values
         |(0, null, null, null, null, null, null, null, null, null, null, null, null),
         |(1, 0, null, '2015-07-03', 0.0, -1.0, 4141214, 1239012341823719, null, 'f', 'Hello World', 'Hello World', '2015-07-03 00:00:00.000'),
         |(2, 0, false, null, -1234152.12312498, 100000.0, null, 1239012341823719, 24, '___|_123', 'Controls\t \b\n\r\f\\\\''\"', 'Controls\t \b\n\r\f\\\\''\"', null),
         |(3, 1, false, '2015-07-02', 0.0, 0.0, 42, 1239012341823719, -13, 'asdf', 'Specials/%', 'Specials/%', '2015-07-02 00:00:00.000'),
         |(4, 1, true, '2015-07-01', 1234152.12312498, 1.0, 42, 1239012341823719, 23, 'Unicode''s樂趣', 'Singl_Byte_Chars', 'Multi樂Byte趣Chars', '2015-07-01 00:00:00.001'),
         |(5, null, null, null, null, null, null, null, null, null, '', '', null),
         |(6, null, null, null, null, null, null, null, null, null, '  Hello World  ', '  Hello World  ', null),
         |(7, null, null, null, null, null, null, null, null, null, '  \t\b\nFoo\r\f\\\\''\"  ', '  \t\b\nFoo\r\f\\\\''\"  ', null),
         |(8, null, null, null, null, null, null, null, null, null, '  /%Foo%/  ', '  /%Foo%/  ', null),
         |(9, null, null, null, null, null, null, null, null, null, '  _Single_  ', '  樂Multi趣  ', null)
       """.stripMargin
    )
    // scalastyle:on
  }
}
