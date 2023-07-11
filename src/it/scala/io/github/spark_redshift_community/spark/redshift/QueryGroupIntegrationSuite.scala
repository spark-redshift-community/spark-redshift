package io.github.spark_redshift_community.spark.redshift

import org.mockito.Mockito.verify
import org.scalatestplus.mockito.MockitoSugar.mock
import org.slf4j.Logger

class QueryGroupIntegrationSuite extends IntegrationSuiteBase {
  test("getConnectorWithQueryGroup returns a working connection when setting query group fails") {
    val invalidQueryGroup = "'"
    val conn = TestJdbcWrapper.getConnectorWithQueryGroup(None, jdbcUrl, None, invalidQueryGroup)
    verify(TestJdbcWrapper.getLogger).debug("Unable to set query group: " +
        "com.amazon.redshift.util.RedshiftException: Unterminated string literal " +
        "started at position 21 in SQL set query_group to '''. Expected  char")
    try {
      val results = TestJdbcWrapper.executeQueryInterruptibly(conn.prepareStatement("select 1"))
      assert(results.next())
      assert(results.getInt(1) == 1)
      assert(!results.next())
    } finally {
      conn.close()
    }
  }
}

private object TestJdbcWrapper extends JDBCWrapper {
  override protected val log = mock[Logger]
  def getLogger: Logger = log
}
