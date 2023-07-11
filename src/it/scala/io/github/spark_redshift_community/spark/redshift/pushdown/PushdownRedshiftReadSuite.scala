package io.github.spark_redshift_community.spark.redshift.pushdown

import io.github.spark_redshift_community.spark.redshift.{IntegrationSuiteBase, OverrideNullableSuite}

class PushdownRedshiftReadSuite extends IntegrationSuiteBase with OverrideNullableSuite {
  override val auto_pushdown: String = "true"
}
