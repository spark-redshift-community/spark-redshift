package io.github.spark_redshift_community.spark.redshift;

import java.sql.SQLException
import java.util

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.JavaConverters.mapAsJavaMap

import io.github.spark_redshift_community.spark.redshift.v2.{RedshiftDataSourceV2}
import org.apache.spark.sql.connector.catalog.{Identifier, TableChange, Table => SparkTable}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap


class RedshiftCatalog extends JDBCTableCatalog {
  
  private var options    : JDBCOptions = _
  private var dialect    : JdbcDialect = _
  
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    super.initialize(name, options)
    
    val map = options.asCaseSensitiveMap().asScala.toMap
    // The `JDBCOptions` checks the existence of the table option. This is required by JDBC v1, but
    // JDBC V2 only knows the table option when loading a table. Here we put a table option with a
    // fake value, so that it can pass the check of `JDBCOptions`.
    this.options = new JDBCOptions(map + (JDBCOptions.JDBC_TABLE_NAME -> "__invalid_dbtable"))
    dialect = JdbcDialects.get(this.options.url)
  }
  override def loadTable(ident: Identifier): SparkTable = {
        val optionsWithTableName = new JDBCOptions(
          options.parameters + (JDBCOptions.JDBC_TABLE_NAME -> getTableName(ident)))
          logWarning(optionsWithTableName.asProperties.toString)
        try {
          val map = mapAsJavaMap(optionsWithTableName.parameters.toMap)
          new RedshiftDataSourceV2()
            .getTable(new  CaseInsensitiveStringMap(map))
        } catch {
          case _: SQLException => throw QueryCompilationErrors.noSuchTableError(ident)
        }
      }
  
  private def getTableName(ident: Identifier): String = {
    (ident.namespace() :+ ident.name()).map(dialect.quoteIdentifier).mkString(".")
  }
  override def invalidateTable(ident: Identifier): Unit = {
    // TODO  When refresh table, then drop the s3 folder
  }
  override def dropTable(ident: Identifier): Boolean =
    throw new UnsupportedOperationException("Drop table is not supported.")
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new UnsupportedOperationException("Rename table is not supported.")
  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform],
    properties: util .Map[String, String]): SparkTable =
    throw new UnsupportedOperationException("Create table is not supported.")

  override def alterTable(ident: Identifier, changes: TableChange*): SparkTable =
    throw new UnsupportedOperationException("Purge table is not supported.")
  override def dropNamespace(namespace: Array[String]): Boolean =
    throw new UnsupportedOperationException("Drop namespace is not supported.")
}