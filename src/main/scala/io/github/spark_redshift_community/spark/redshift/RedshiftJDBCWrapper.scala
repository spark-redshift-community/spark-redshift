/*
 * Copyright 2015-2018 Snowflake Computing
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.spark_redshift_community.spark.redshift

import io.github.spark_redshift_community.spark.redshift.Parameters.MergedParameters
import io.github.spark_redshift_community.spark.redshift.pushdown.{ConstantString, EmptyRedshiftSQLStatement, Identifier, RedshiftSQLStatement}
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.sql.{Connection, Driver, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData, SQLException}
import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.Try
import scala.util.control.NonFatal



/**
 * Shim which exposes some JDBC helper functions. Most of this code is copied from Spark SQL, with
 * minor modifications for Redshift-specific features and limitations.
 */
private[redshift] class JDBCWrapper extends Serializable {

  protected val log = LoggerFactory.getLogger(getClass)

  // Note: Marking fields `implicit transient lazy` allows spark to recreate them upon
  // de-serialization

  /**
   * This iterator automatically increments every time it is used.
   */
  @transient implicit private lazy val JDBCCallNumberGenerator = Iterator.from(1)

  @transient implicit private lazy val ec: ExecutionContext = {
    val threadFactory = new ThreadFactory {
      private[this] val count = new AtomicInteger()
      override def newThread(r: Runnable) = {
        val thread = new Thread(r)
        thread.setName(s"spark-redshift-JDBCWrapper-${count.incrementAndGet}")
        thread.setDaemon(true)
        thread
      }
    }
    ExecutionContext.fromExecutorService(Executors.newCachedThreadPool(threadFactory))
  }

  /**
   * Given a JDBC subprotocol, returns the name of the appropriate driver class to use.
   *
   * If the user has explicitly specified a driver class in their configuration then that class will
   * be used. Otherwise, we will attempt to load the correct driver class based on
   * the JDBC subprotocol.
   *
   * @param jdbcSubprotocol 'redshift' or 'postgresql'
   * @param userProvidedDriverClass an optional user-provided explicit driver class name
   * @return the driver class
   */
  private def getDriverClass(
      jdbcSubprotocol: String,
      userProvidedDriverClass: Option[String]): String = {
    userProvidedDriverClass.getOrElse {
      jdbcSubprotocol match {
        case "redshift" =>
          try {
            Utils.classForName("com.amazon.redshift.jdbc42.Driver").getName
          } catch {
            case _: ClassNotFoundException =>
              try {
                Utils.classForName("com.amazon.redshift.jdbc41.Driver").getName
              } catch {
                case _: ClassNotFoundException =>
                  try {
                    Utils.classForName("com.amazon.redshift.jdbc4.Driver").getName
                  } catch {
                    case e: ClassNotFoundException =>
                      throw new ClassNotFoundException(
                        "Could not load an Amazon Redshift JDBC driver; see the README for " +
                          "instructions on downloading and configuring the official Amazon driver.",
                        e
                      )
                  }
              }
          }
        case "postgresql" =>
          "org.postgresql.Driver"
        case "jdbc-secretsmanager" =>
          "com.amazonaws.secretsmanager.sql.AWSSecretsManagerRedshiftDriver"

        case other => throw new IllegalArgumentException(s"Unsupported JDBC protocol: '$other'")
      }
    }
  }

  @transient private lazy val cancellationMap =
    new ConcurrentHashMap[PreparedStatement, Int]()

  // Adding ShutdownHook
  sys.addShutdownHook {
    cancellationMap.forEach { (statement, jdbcSmtNum) =>
      try {
        log.info(s"Cancelling pending JDBC call {}", jdbcSmtNum)
        statement.cancel()
      } catch {
        case e: Throwable =>
          log.error("Exception occurred while cancelling statement", e)
      }
    }
  }

  /**
   * Execute the given SQL statement while supporting interruption.
   * If InterruptedException is caught, then the statement will be cancelled if it is running.
   *
   * @return <code>true</code> if the first result is a <code>ResultSet</code>
   *         object; <code>false</code> if the first result is an update
   *         count or there is no result
   */
  def executeInterruptibly(statement: PreparedStatement): Boolean = {
    executeInterruptibly(statement, _.execute())
  }

  /**
   * Execute the given SQL statement while supporting interruption.
   * If InterruptedException is caught, then the statement will be cancelled if it is running.
   *
   * @return a <code>ResultSet</code> object that contains the data produced by the
   *         query; never <code>null</code>
   */
  def executeQueryInterruptibly(statement: PreparedStatement): ResultSet = {
    executeInterruptibly(statement, _.executeQuery())
  }

  private def executeInterruptibly[T](
      statement: PreparedStatement,
      op: PreparedStatement => T): T = {
    val JDBCCallNumber = JDBCCallNumberGenerator.next
    try {
      log.info("Begin JDBC call {}", JDBCCallNumber)
      cancellationMap.put(statement, JDBCCallNumber)
      val future = Future[T](op(statement))(ec)
      try {
        Await.result(future, Duration.Inf)
      } catch {
        case e: SQLException =>
          // Wrap and re-throw so that this thread's stacktrace appears to the user.
          throw new SQLException("Exception thrown in awaitResult: ", e)
        case NonFatal(t) =>
          // Wrap and re-throw so that this thread's stacktrace appears to the user.
          throw new Exception("Exception thrown in awaitResult: ", t)
      }
    } catch {
      case e: InterruptedException =>
        try {
          statement.cancel()
          throw e
        } catch {
          case s: SQLException =>
            log.error("Exception occurred while cancelling query", s)
            throw e
        }
    }
    finally {
      cancellationMap.remove(statement)
      log.info("End JDBC call {}", JDBCCallNumber)
    }
  }

  /**
   * Takes a (schema, table) specification and returns the table's Catalyst
   * schema.
   *
   * @param conn A JDBC connection to the database.
   * @param table The table name of the desired table.  This may also be a
   *   SQL query wrapped in parentheses.
   *
   * @return A StructType giving the table's Catalyst schema.
   * @throws SQLException if the table specification is garbage.
   * @throws SQLException if the table contains an unsupported type.
   */
  def resolveTable(
                    conn: Connection,
                    table: String,
                    params: Option[MergedParameters] = None): StructType = {
    // It's important to leave the `LIMIT 1` clause in order to limit the work of the query in case
    // the underlying JDBC driver implementation implements PreparedStatement.getMetaData() by
    // executing the query. It looks like the standard Redshift and Postgres JDBC drivers don't do
    // this but we leave the LIMIT condition here as a safety-net to guard against perf regressions.
    val ps = conn.prepareStatement(s"SELECT * FROM $table LIMIT 1")
    try {
      log.info("Getting schema from Redshift for table: {}", table)
      val rsmd = executeInterruptibly(ps, _.getMetaData)
      val ncols = rsmd.getColumnCount
      val fields = {
        new Array[StructField](ncols)
      }
      var i = 0
      while (i < ncols) {
        val columnName = rsmd.getColumnLabel(i + 1)
        val rsType = rsmd.getColumnTypeName(i + 1)
        val dataType = rsmd.getColumnType(i + 1)
        val fieldSize = rsmd.getPrecision(i + 1)
        val fieldScale = rsmd.getScale(i + 1)
        val isSigned = rsmd.isSigned(i + 1)
        val nullable = if (params.exists(_.overrideNullable)) {
          true
        } else rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
        val columnType = getCatalystType(dataType, fieldSize, fieldScale, isSigned, params)
        val meta = new MetadataBuilder().putString("redshift_type", rsType).build()

        fields(i) = StructField(columnName, columnType, nullable, meta)
        i = i + 1
      }
      new StructType(fields)
    } finally {
      ps.close()
    }
  }

  def resolveTableFromMeta(conn: Connection,
                           rsmd: ResultSetMetaData,
                           params: MergedParameters): StructType = {
    val ncols = rsmd.getColumnCount
    val fields = new Array[StructField](ncols)
    var i = 0
    while (i < ncols) {
      val columnName = rsmd.getColumnLabel(i + 1)
      val rsType = rsmd.getColumnTypeName(i + 1)
      val dataType = rsmd.getColumnType(i + 1)
      val fieldSize = rsmd.getPrecision(i + 1)
      val fieldScale = rsmd.getScale(i + 1)
      val isSigned = rsmd.isSigned(i + 1)
      val nullable = rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
      val columnType =
        getCatalystType(dataType, fieldSize, fieldScale, isSigned, Some(params))
      val meta = new MetadataBuilder().putString("redshift_type", rsType).build()
      fields(i) = StructField(
        // Add quotes around column names if Redshift would usually require them.
        if (columnName.matches("[_A-Z]([_0-9A-Z])*")) columnName
        else s""""$columnName"""",
        columnType,
        nullable, meta)
      i = i + 1
    }
    new StructType(fields)
  }

  /**
   * Returns the default application name.
   */
  def defaultAppName: String = Utils.connectorServiceName.
    map(name => s"${Utils.DEFAULT_APP_NAME}_$name").getOrElse(Utils.DEFAULT_APP_NAME)

  /**
   * The same as get connector but after connection attempt to set the query group.
   * If setting the query group fails for any reason return a new connection with
   * the query group unset.
   * @param userProvidedDriverClass the class name of the JDBC driver for the given url
   * @param url the JDBC url to connect to
   * @param params parameters set by the user
   * @param queryGroup query group to set for the connection
   * @return a connection
   */
  def getConnectorWithQueryGroup(userProvidedDriverClass: Option[String],
                                 url: String,
                                 params: Option[MergedParameters],
                                 queryGroup: String): Connection = {
    val conn = getConnector(userProvidedDriverClass, url, params)
    try {
      executeInterruptibly(conn.prepareStatement(s"""set query_group to '${queryGroup}'"""))
      conn
    } catch {
      case e: Throwable => {
        log.debug(s"Unable to set query group: $e")
        conn.close()
        getConnector(userProvidedDriverClass, url, params)
      }
    }
  }

  /**
   * Given a driver string and a JDBC url, load the specified driver and return a DB connection.
   *
   * @param userProvidedDriverClass the class name of the JDBC driver for the given url. If this
   *                                is None then `spark-redshift` will attempt to automatically
   *                                discover the appropriate driver class.
   * @param url the JDBC url to connect to.
   */
  def getConnector(userProvidedDriverClass: Option[String],
                   url: String,
                   params: Option[MergedParameters]): Connection = {
    // Update the url if we are using secrets manager.
    val updatedURL = if (params.isDefined && params.get.secretId.isDefined) {
      url.replaceFirst("jdbc", "jdbc-secretsmanager")
    } else {
      url
    }

    // Map any embedded driver properties for both JDBC and Secrets Manager. For some properties
    // we designate a particular prefix to allow passing these properties through to their related
    // driver. Note that AWS Secrets Manager uses System properties for "drivers.region" property
    // and that this property must be set before registering the secret manager driver class.
    // Otherwise, EC2 metadata server requests will occur for determining the secret's region which
    // can fail when running outside of AWS environments.
    val driverProperties = new Properties()
    params.foreach { case MergedParameters(sourceParams) =>
      Utils.copyProperty("user", sourceParams, driverProperties)
      Utils.copyProperty("password", sourceParams, driverProperties)
      Utils.copyProperty("secret.id", "user", sourceParams, driverProperties)
      Utils.copyProperties("^jdbc\\..+", "^jdbc\\.", "", sourceParams, driverProperties)
      Utils.copyProperties("^secret\\..+", "^secret\\.", "drivers\\.",
        sourceParams - "secret.id", System.getProperties)
    }

    // Set the application name property if not already specified by the client.
    if (!updatedURL.toLowerCase().contains("applicationname=") &&
        !driverProperties.containsKey("applicationname")) {
      driverProperties.setProperty("applicationname", defaultAppName)
    }

    val subprotocol = updatedURL.stripPrefix("jdbc:").split(":")(0)
    val driverClass: String = getDriverClass(subprotocol, userProvidedDriverClass)
    DriverRegistry.register(driverClass)
    val driverWrapperClass: Class[_] = if (SPARK_VERSION.startsWith("1.4")) {
      Utils.classForName("org.apache.spark.sql.jdbc.package$DriverWrapper")
    } else { // Spark 1.5.0+
      Utils.classForName("org.apache.spark.sql.execution.datasources.jdbc.DriverWrapper")
    }

    def getWrapped(d: Driver): Driver = {
      require(driverWrapperClass.isAssignableFrom(d.getClass))
      driverWrapperClass.getDeclaredMethod("wrapped").invoke(d).asInstanceOf[Driver]
    }

    // Note that we purposely don't call DriverManager.getConnection() here: we want to ensure
    // that an explicitly-specified user-provided driver class can take precedence over the default
    // class, but DriverManager.getConnection() might return a according to a different precedence.
    // At the same time, we don't want to create a driver-per-connection, so we use the
    // DriverManager's driver instances to handle that singleton logic for us.
    val driver: Driver = DriverManager.getDrivers.asScala.collectFirst {
      case d if driverWrapperClass.isAssignableFrom(d.getClass)
        && getWrapped(d).getClass.getCanonicalName == driverClass => d
      case d if d.getClass.getCanonicalName == driverClass => d
    }.getOrElse {
      throw new IllegalArgumentException(s"Did not find registered driver with class $driverClass")
    }

    // Make the connection.
    driver.connect(updatedURL, driverProperties)
  }

  /**
   * Compute the SQL schema string for the given Spark SQL Schema.
   */
  def schemaString(schema: StructType): String = {
    val sb = new StringBuilder()
    schema.fields.foreach { field => {
      val name = field.name
      val typ: String = if (field.metadata.contains("redshift_type")) {
        field.metadata.getString("redshift_type")
      } else {
        field.dataType match {
          case IntegerType => "INTEGER"
          case LongType => "BIGINT"
          case DoubleType => "DOUBLE PRECISION"
          case FloatType => "REAL"
          case ShortType => "SMALLINT"
          case ByteType => "SMALLINT" // Redshift does not support the BYTE type.
          case BooleanType => "BOOLEAN"
          case StringType =>
            if (field.metadata.contains("maxlength")) {
              s"VARCHAR(${field.metadata.getLong("maxlength")})"
            } else {
              s"VARCHAR(MAX)"
            }
          case TimestampType => "TIMESTAMP"
          case DateType => "DATE"
          case t: DecimalType => s"DECIMAL(${t.precision},${t.scale})"
          case _ : ArrayType | _ : MapType | _ : StructType => "SUPER"
          case _ => throw new IllegalArgumentException(s"Don't know how to save $field to JDBC")
        }
      }

      val nullable = if (field.nullable) "" else "NOT NULL"
      val encoding = if (field.metadata.contains("encoding")) {
        s"ENCODE ${field.metadata.getString("encoding")}"
      } else {
        ""
      }
      sb.append(s""", "${name.replace("\"", "\\\"")}" $typ $nullable $encoding""".trim)
    }}
    if (sb.length < 2) "" else sb.substring(2)
  }

  /**
   * Returns true if the table already exists in the JDBC database.
   */
  def tableExists(conn: Connection, table: String): Boolean = {
    // Somewhat hacky, but there isn't a good way to identify whether a table exists for all
    // SQL database systems, considering "table" could also include the database name.
    Try {
      val stmt = conn.prepareStatement(s"SELECT 1 FROM $table LIMIT 1")
      log.info("Checking if table exists: {}", table)
      executeInterruptibly(stmt, _.getMetaData).getColumnCount
    }.isSuccess
  }

  /**
   * Maps a JDBC type to a Catalyst type.
   *
   * @param sqlType - A field of java.sql.Types
   * @return The Catalyst type corresponding to sqlType.
   */
  private def getCatalystType(
      sqlType: Int,
      precision: Int,
      scale: Int,
      signed: Boolean,
      params: Option[MergedParameters] = None): DataType = {

    val answer = sqlType match {
      // scalastyle:off
      // Null Type
      case java.sql.Types.NULL          => null

      // Character Types
      case java.sql.Types.CHAR          => StringType
      case java.sql.Types.NCHAR         => StringType
      case java.sql.Types.NVARCHAR      => StringType
      case java.sql.Types.VARCHAR       => StringType
      case java.sql.Types.LONGVARCHAR   => StringType

      // Datetime Types
      case java.sql.Types.DATE          => DateType
      case java.sql.Types.TIME          => TimestampType
      case java.sql.Types.TIMESTAMP     => TimestampType

      // Boolean Type
      case java.sql.Types.BIT           => BooleanType // @see JdbcDialect for quirks
      case java.sql.Types.BOOLEAN       => BooleanType

      // Numeric Types
      case java.sql.Types.BIGINT        => if (signed) { LongType } else { DecimalType(20,0) }
      case java.sql.Types.DECIMAL
        if precision != 0 || scale != 0 => DecimalType(precision, scale)
      case java.sql.Types.DECIMAL       => DecimalType(38, 18) // Spark 1.5.0 default
      case java.sql.Types.DOUBLE        => DoubleType
      case java.sql.Types.FLOAT         => FloatType
      case java.sql.Types.INTEGER       => if (signed) { IntegerType } else { LongType }
      case java.sql.Types.NUMERIC
        if precision != 0 || scale != 0 => DecimalType(precision, scale)
      case java.sql.Types.NUMERIC       => DecimalType(38, 18) // Spark 1.5.0 default
      // Redshift Real is represented in 4 bytes IEEE Float. https://docs.aws.amazon.com/redshift/latest/dg/r_Numeric_types201.html
      case java.sql.Types.REAL          => if (params.exists(_.legacyJdbcRealTypeMapping)) { DoubleType } else { FloatType }
      case java.sql.Types.SMALLINT      => IntegerType
      case java.sql.Types.TINYINT       => IntegerType
      case _                            => null
      // scalastyle:on
    }

    if (answer == null) throw new SQLException("Unsupported type " + sqlType)
    answer
  }
}

private[redshift] object DefaultJDBCWrapper
  extends JDBCWrapper
  with Serializable {

  private val LOGGER = LoggerFactory.getLogger(getClass.getName)

  implicit class DataBaseOperations(connection: Connection) {

    /**
     * @return telemetry connector
     */
//    def getTelemetry: Telemetry = TelemetryClient.createTelemetry(connection)

    /**
     * Create a table
     *
     * @param name      table name
     * @param schema    table schema
     * @param overwrite use "create or replace" if true,
     *                  otherwise, use "create if not exists"
     */
    def createTable(name: String,
                    schema: StructType,
                    params: MergedParameters,
                    overwrite: Boolean,
                    temporary: Boolean,
                    bindVariableEnabled: Boolean = true): Unit =
      (ConstantString("create") +
        (if (overwrite) "or replace" else "") +
        (if (temporary) "temporary" else "") + "table" +
        (if (!overwrite) "if not exists" else "") + Identifier(name) +
        s"(${schemaString(schema)})")
        .execute(bindVariableEnabled)(connection)

    def createTableLike(newTable: String,
                        originalTable: String,
                        bindVariableEnabled: Boolean = true): Unit = {
      (ConstantString("create or replace table") + Identifier(newTable) +
        "like" + Identifier(originalTable))
        .execute(bindVariableEnabled)(connection)
    }

    def truncateTable(table: String,
                      bindVariableEnabled: Boolean = true): Unit =
      (ConstantString("truncate") + table)
        .execute(bindVariableEnabled)(connection)

    def swapTable(newTable: String,
                  originalTable: String,
                  bindVariableEnabled: Boolean = true): Unit =
      (ConstantString("alter table") + Identifier(newTable) + "swap with" +
        Identifier(originalTable)).execute(bindVariableEnabled)(connection)

    def renameTable(newName: String,
                    oldName: String,
                    bindVariableEnabled: Boolean = true): Unit =
      (ConstantString("alter table") + Identifier(oldName) + "rename to" +
        Identifier(newName)).execute(bindVariableEnabled)(connection)

    /**
     * @param name table name
     * @return true if table exists, otherwise false
     */
    def tableExists(name: String,
                    bindVariableEnabled: Boolean = true): Boolean =
      Try {
        (EmptyRedshiftSQLStatement() + "desc table" + Identifier(name))
          .execute(bindVariableEnabled)(connection)
      }.isSuccess

    /**
     * Drop a table
     *
     * @param name table name
     * @return true if table dropped, false if the given table not exists
     */
    def dropTable(name: String, bindVariableEnabled: Boolean = true): Boolean =
      Try {
        (EmptyRedshiftSQLStatement() + "drop table" + Identifier(name))
          .execute(bindVariableEnabled)(connection)
      }.isSuccess

    def tableMetaData(name: String): ResultSetMetaData =
      tableMetaDataFromStatement(ConstantString(name) !)

    def tableMetaDataFromStatement(
                                    statement: RedshiftSQLStatement,
                                    bindVariableEnabled: Boolean = true
                                  ): ResultSetMetaData =
      (ConstantString("select * from") + statement + "where 1 = 0")
        .prepareStatement(bindVariableEnabled)(connection)
        .getMetaData

    def tableSchema(name: String, params: Option[MergedParameters]): StructType =
      resolveTable(connection, name, params)

    def tableSchema(statement: RedshiftSQLStatement,
                    params: MergedParameters): StructType =
      resolveTableFromMeta(
        connection,
        tableMetaDataFromStatement(statement),
        params
      )

    def execute(statement: RedshiftSQLStatement,
                bindVariableEnabled: Boolean = true): Unit =
      statement.execute(bindVariableEnabled)(connection)
  }

}
