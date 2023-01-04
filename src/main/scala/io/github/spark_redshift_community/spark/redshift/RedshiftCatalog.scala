package io.github.spark_redshift_community.spark.redshift;

import java.sql.SQLException

import scala.collection.JavaConverters.{mapAsJavaMap, mapAsScalaMapConverter}

import io.github.spark_redshift_community.spark.redshift.Parameters.MergedParameters
import io.github.spark_redshift_community.spark.redshift.v2.RedshiftDataSourceV2
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, Table => SparkTable}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.util.CaseInsensitiveStringMap


class RedshiftCatalog extends JDBCTableCatalog {
  
  private var options    : JDBCOptions = _
  private var dialect    : JdbcDialect = _
  val spark: SparkSession = SparkSession.active
  
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    super.initialize(name, options)
    
    val map = options.asCaseSensitiveMap().asScala.toMap
    // The `JDBCOptions` checks the existence of the table option. This is required by JDBC v1, but
    // JDBC V2 only knows the table option when loading a table. Here we put a table option with a
    // fake value, so that it can pass the check of `JDBCOptions`.
    this.options = new JDBCOptions(map + (JDBCOptions.JDBC_TABLE_NAME -> "__invalid_dbtable"))
    dialect = JdbcDialects.get(this.options.url)
  }
  private def getTableName(ident: Identifier): String = {
    (ident.namespace() :+ ident.name()).map(dialect.quoteIdentifier).mkString(".")
  }
  override def loadTable(ident: Identifier): SparkTable = {
        val optionsWithTableName = new JDBCOptions(
          options.parameters + (JDBCOptions.JDBC_TABLE_NAME -> getTableName(ident)))
        try {
          val map = mapAsJavaMap(optionsWithTableName.parameters.toMap)
          new RedshiftDataSourceV2()
            .getTable(new CaseInsensitiveStringMap(map))
        } catch {
          case _: SQLException => throw QueryCompilationErrors.noSuchTableError(ident)
        }
      }
  
  override def invalidateTable(ident: Identifier): Unit = {
    import org.apache.hadoop.fs.{FileSystem, Path}
    import java.net.URI
    val parameters = Parameters.mergeParameters(options.parameters.toMap)
    val rootDir = parameters.rootTempDir
    val tableName = (ident.namespace() :+ ident.name()).map(dialect.quoteIdentifier).mkString(".")
    val tableHash = tableName.hashCode.toString
    val tablePath = Utils.fixS3Url(Utils.joinUrls(rootDir, tableHash))
    val fs = FileSystem.get(URI.create(tablePath), spark.sparkContext.hadoopConfiguration)
    logWarning(s"Invalidating cache for $tableName by deleting $tablePath recursively." +
               s" Caution: this might break concurrent queries using cache on that table")
    fs.delete(new Path(tablePath), true)
  }
}