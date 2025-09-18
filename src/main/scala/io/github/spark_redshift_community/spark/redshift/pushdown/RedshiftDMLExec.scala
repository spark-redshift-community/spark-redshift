package io.github.spark_redshift_community.spark.redshift.pushdown

import io.github.spark_redshift_community.spark.redshift.RedshiftRelation
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.LongType


case class RedshiftDMLExec (query: RedshiftSQLStatement, relation: RedshiftRelation)
  extends LeafRunnableCommand {

  override def output: Seq[Attribute] = Seq(AttributeReference("num_affected_rows", LongType)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    relation.runDMLFromSQL(query)
  }
}
