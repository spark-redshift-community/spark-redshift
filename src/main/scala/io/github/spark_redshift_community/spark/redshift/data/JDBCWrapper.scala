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
package io.github.spark_redshift_community.spark.redshift.data

import io.github.spark_redshift_community.spark.redshift.Parameters.MergedParameters
import io.github.spark_redshift_community.spark.redshift.Utils
import io.github.spark_redshift_community.spark.redshift.data.JDBCWrapper.{POSTGRESQL_DRIVER, REDSHIFT_JDBC_4_1_DRIVER, REDSHIFT_JDBC_4_2_DRIVER, REDSHIFT_JDBC_4_DRIVER, SECRET_MANAGER_REDSHIFT_DRIVER}
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.sql.{Driver, DriverManager, PreparedStatement, SQLException}
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, Executors, ThreadFactory}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal
import io.github.spark_redshift_community.spark.redshift.pushdown.{BooleanVariable, ByteVariable, ConstantString, DoubleVariable, FloatVariable, Identifier, IntVariable, LongVariable, RedshiftSQLStatement, ShortVariable, StatementElement, StringVariable}
import org.apache.spark.sql.types._

import java.sql.ResultSetMetaData
import scala.collection.JavaConverters._
import scala.language.postfixOps

private[redshift] class JDBCWrapper extends RedshiftWrapper with Serializable {

  protected val log = LoggerFactory.getLogger(getClass)

  // Note: Marking fields `implicit transient lazy` allows spark to recreate them upon
  // de-serialization

  /**
   * This iterator automatically increments every time it is used.
   */
  @transient implicit private lazy val callNumberGenerator = Iterator.from(1)

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

  @transient private lazy val cancellationMap =
    new ConcurrentHashMap[PreparedStatement, Int]()

  // Adding ShutdownHook
  sys.addShutdownHook {
    cancellationMap.forEach { (statement, callNumber) =>
      try {
        log.info(s"Cancelling pending JDBC call {}", callNumber)
        statement.cancel()
      } catch {
        case e: Throwable =>
          log.error("Exception occurred while cancelling statement: {}", e.getMessage)
      }
    }
  }

  private def executeInterruptibly[T](statement: PreparedStatement,
                                      op: PreparedStatement => T): T = {
    val callNumber = callNumberGenerator.next
    try {
      log.info("Begin JDBC call {}", callNumber)
      cancellationMap.put(statement, callNumber)
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
            log.error("Exception occurred while cancelling query: {}", s.getMessage)
            throw e
        }
    }
    finally {
      cancellationMap.remove(statement)
      log.info("End JDBC call {}", callNumber)
    }
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
            Utils.classForName(REDSHIFT_JDBC_4_2_DRIVER).getName
          } catch {
            case _: ClassNotFoundException =>
              try {
                Utils.classForName(REDSHIFT_JDBC_4_1_DRIVER).getName
              } catch {
                case _: ClassNotFoundException =>
                  try {
                    Utils.classForName(REDSHIFT_JDBC_4_DRIVER).getName
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
        case "postgresql" => POSTGRESQL_DRIVER
        case "jdbc-secretsmanager" => SECRET_MANAGER_REDSHIFT_DRIVER
        case other => throw new IllegalArgumentException(s"Unsupported JDBC protocol: '$other'")
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
  override def getConnector(params: MergedParameters): RedshiftConnection = {
    // Update the url if we are using secrets manager.
    val updatedURL = if (params.secretId.isDefined) {
      params.jdbcUrl.get.replaceFirst("jdbc", "jdbc-secretsmanager")
    } else {
      params.jdbcUrl.get
    }

    // Map any embedded driver properties for both JDBC and Secrets Manager. For some properties
    // we designate a particular prefix to allow passing these properties through to their related
    // driver. Note that AWS Secrets Manager uses System properties for "drivers.region" property
    // and that this property must be set before registering the secret manager driver class.
    // Otherwise, EC2 metadata server requests will occur for determining the secret's region which
    // can fail when running outside of AWS environments.
    val driverProperties = new Properties()
    Utils.copyProperty("user", params.parameters, driverProperties)
    Utils.copyProperty("password", params.parameters, driverProperties)
    Utils.copyProperty("secret.id", "user", params.parameters, driverProperties)
    Utils.copyProperties("^jdbc\\..+", "^jdbc\\.", "", params.parameters, driverProperties)
    Utils.copyProperties("^secret\\..+", "^secret\\.", "drivers\\.",
      params.parameters - "secret.id", System.getProperties)

    // Set the application name property if not already specified by the client.
    if (!updatedURL.toLowerCase().contains("applicationname=") &&
      !driverProperties.containsKey("applicationname")) {
      driverProperties.setProperty("applicationname", Utils.getApplicationName(params))
    }

    val subprotocol = updatedURL.stripPrefix("jdbc:").split(":")(0)
    val driverClass: String = getDriverClass(subprotocol, params.jdbcDriver)
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

    Utils.checkJDBCSecurity(driverClass, updatedURL, driverProperties)

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
    val conn = driver.connect(updatedURL, driverProperties)

    // Return a wrapper around the connection
    JDBCConnection(conn)
  }

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
  override def getConnectorWithQueryGroup(params: MergedParameters,
                                          queryGroup: String): RedshiftConnection = {
    val conn = getConnector(params)
    try {
      executeInterruptibly(conn, s"""set query_group to '${queryGroup}'""")
      conn
    } catch {
      case e: Throwable =>
        log.debug(s"Unable to set query group: ${e.getMessage}")
        conn.close()
        getConnector(params)
    }
  }

  override def setAutoCommit(conn: RedshiftConnection, autoCommit: Boolean): Unit = {
    conn.setAutoCommit(autoCommit)
  }

  override def commit(conn: RedshiftConnection): Unit = {
    val jdbcConnection = conn.asInstanceOf[JDBCConnection]
    jdbcConnection.conn.commit()
  }

  override def rollback(conn: RedshiftConnection): Unit = {
    val jdbcConnection = conn.asInstanceOf[JDBCConnection]
    jdbcConnection.conn.rollback()
  }

  override def executeInterruptibly(conn: RedshiftConnection, sql: String): Boolean = {
    val jdbcConnection = conn.asInstanceOf[JDBCConnection]
    val statement = jdbcConnection.conn.prepareStatement(sql)
    executeInterruptibly(statement, _.execute())
  }

  /**
   * Execute the given SQL statement while supporting interruption.
   * If InterruptedException is caught, then the statement will be cancelled if it is running.
   *
   * @return a <code>ResultSet</code> object that contains the data produced by the
   *         query; never <code>null</code>
   */
  override def executeQueryInterruptibly(conn: RedshiftConnection, sql: String)
  : RedshiftResults = {
    val jdbcConnection = conn.asInstanceOf[JDBCConnection]
    val statement = jdbcConnection.conn.prepareStatement(sql)
    val resultSet = executeInterruptibly(statement, _.executeQuery())
    JDBCResults(resultSet)
  }

  /**
   * Execute the given SQL statement while supporting interruption.
   * If InterruptedException is caught, then the statement will be cancelled if it is running.
   * @param statement
   * @return number of affected rows
   */
  override def executeUpdateInterruptibly(conn: RedshiftConnection, sql: String): Long = {
    val jdbcConnection = conn.asInstanceOf[JDBCConnection]
    val statement = jdbcConnection.conn.prepareStatement(sql)
    executeInterruptibly(statement, _.executeLargeUpdate())
  }

  override def executeUpdate(conn: RedshiftConnection, sql: String): Long = {
    val jdbcConnection = conn.asInstanceOf[JDBCConnection]
    val statement = jdbcConnection.conn.prepareStatement(sql)
    executeInterruptibly(statement, _.executeUpdate())
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
  override def resolveTable(
                    conn: RedshiftConnection,
                    table: String,
                    params: Option[MergedParameters] = None): StructType = {
    val jdbcConnection = conn.asInstanceOf[JDBCConnection]
    // It's important to leave the `LIMIT 1` clause in order to limit the work of the query in case
    // the underlying JDBC driver implementation implements PreparedStatement.getMetaData() by
    // executing the query. It looks like the standard Redshift and Postgres JDBC drivers don't do
    // this but we leave the LIMIT condition here as a safety-net to guard against perf regressions.
    val ps = jdbcConnection.conn.prepareStatement(s"SELECT * FROM $table LIMIT 1")
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

  private def resolveTableFromMeta(rsmd: ResultSetMetaData,
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

      // Binary types
      case java.sql.Types.LONGVARBINARY => BinaryType

      // Datetime Types
      case java.sql.Types.DATE          => DateType
      case java.sql.Types.TIME          => TimestampType
      case java.sql.Types.TIMESTAMP     => TimestampType
      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE
                                        => TimestampType
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
      case java.sql.Types.SMALLINT      => if (params.exists(_.legacyMappingShortToInt)) { IntegerType } else { ShortType }
      case java.sql.Types.TINYINT       => ByteType
      case _                            => null
      // scalastyle:on
    }

    if (answer == null) throw new SQLException("Unsupported type " + sqlType)
    answer
  }

  /**
   * Returns true if the table already exists in the JDBC database.
   */
  override def tableExists(conn: RedshiftConnection, table: String): Boolean = {
    val jdbcConnection = conn.asInstanceOf[JDBCConnection]
    // Somewhat hacky, but there isn't a good way to identify whether a table exists for all
    // SQL database systems, considering "table" could also include the database name.
    Try {
      val stmt = jdbcConnection.conn.prepareStatement(s"SELECT 1 FROM $table LIMIT 1")
      log.info("Checking if table exists: {}", table)
      executeInterruptibly(stmt, _.getMetaData).getColumnCount
    }.isSuccess
  }

  override def tableSchema(conn: RedshiftConnection,
                           statement: RedshiftSQLStatement,
                           params: MergedParameters): StructType = {
    resolveTableFromMeta(
      tableMetaDataFromStatement(conn, statement),
      params
    )
  }

  private def tableMetaDataFromStatement(conn: RedshiftConnection,
                                         statement: RedshiftSQLStatement,
                                         bindVariableEnabled: Boolean = true
                                        ): ResultSetMetaData = {
    val sqlStatement = (ConstantString("select * from") + statement + "where 1 = 0")
    prepareStatement(conn, sqlStatement, bindVariableEnabled).getMetaData
  }

  private def prepareStatement(conn: RedshiftConnection,
                               statement: RedshiftSQLStatement,
                               bindVariableEnabled: Boolean): PreparedStatement = {
    if (bindVariableEnabled) prepareWithBindVariable(conn, statement)
    else prepareWithoutBindVariable(conn, statement)
  }

  private def prepareWithoutBindVariable(conn: RedshiftConnection,
                                         sqlStatement: RedshiftSQLStatement): PreparedStatement = {
    val sql = sqlStatement.list.reverse
    val query = sql
      .foldLeft(new StringBuilder) {
        case (buffer, statement) =>
          buffer.append(
            if (statement.isInstanceOf[ConstantString]) statement
            else statement.sql
          )
          buffer.append(" ")
      }
      .toString()

    val logPrefix = s"""${MASTER_LOG_PREFIX}:
                       | execute query without bind variable:
                       |""".stripMargin.filter(_ >= ' ')

    conn.asInstanceOf[JDBCConnection].conn.prepareStatement(query)
  }

  private def prepareWithBindVariable(conn: RedshiftConnection,
                                      sqlStatement: RedshiftSQLStatement): PreparedStatement = {
    val sql = sqlStatement.list.reverse
    val varArray: Array[StatementElement] =
      new Array[StatementElement](sqlStatement.numOfVar)
    var indexOfVar: Int = 0
    val buffer = new StringBuilder

    sql.foreach(element => {
      buffer.append(element)
      if (!element.isInstanceOf[ConstantString]) {
        varArray(indexOfVar) = element
        indexOfVar += 1
      }
      buffer.append(" ")
    })

    val query: String = buffer.toString

    val logPrefix = s"""${MASTER_LOG_PREFIX}:
                       | execute query with bind variable:
                       |""".stripMargin.filter(_ >= ' ')

    val statement = conn.asInstanceOf[JDBCConnection].conn.prepareStatement(query)
    varArray.zipWithIndex.foreach {
      case (element, index) =>
        element match {
          case ele: StringVariable =>
            if (ele.variable.isDefined) {
              statement.setString(index + 1, ele.variable.get)
            } else {
              statement.setNull(index + 1, java.sql.Types.VARCHAR)
            }
          case ele: Identifier =>
            statement.setString(index + 1, ele.variable.get)
          case ele: IntVariable =>
            if (ele.variable.isDefined) {
              statement.setInt(index + 1, ele.variable.get)
            } else {
              statement.setNull(index + 1, java.sql.Types.INTEGER)
            }
          case ele: LongVariable =>
            if (ele.variable.isDefined) {
              statement.setLong(index + 1, ele.variable.get)
            } else {
              statement.setNull(index + 1, java.sql.Types.BIGINT)
            }
          case ele: ShortVariable =>
            if (ele.variable.isDefined) {
              statement.setShort(index + 1, ele.variable.get)
            } else {
              statement.setNull(index + 1, java.sql.Types.SMALLINT)
            }
          case ele: FloatVariable =>
            if (ele.variable.isDefined) {
              statement.setFloat(index + 1, ele.variable.get)
            } else {
              statement.setNull(index + 1, java.sql.Types.FLOAT)
            }
          case ele: DoubleVariable =>
            if (ele.variable.isDefined) {
              statement.setDouble(index + 1, ele.variable.get)
            } else {
              statement.setNull(index + 1, java.sql.Types.DOUBLE)
            }
          case ele: BooleanVariable =>
            if (ele.variable.isDefined) {
              statement.setBoolean(index + 1, ele.variable.get)
            } else {
              statement.setNull(index + 1, java.sql.Types.BOOLEAN)
            }
          case ele: ByteVariable =>
            if (ele.variable.isDefined) {
              statement.setByte(index + 1, ele.variable.get)
            } else {
              statement.setNull(index + 1, java.sql.Types.TINYINT)
            }
          case _ =>
            throw new IllegalArgumentException(
              "Unexpected Element Type: " + element.getClass.getName
            )
        }
    }

    statement
  }
}


private[redshift] object JDBCWrapper {
  val REDSHIFT_JDBC_4_2_DRIVER = "com.amazon.redshift.jdbc42.Driver"
  val REDSHIFT_JDBC_4_1_DRIVER = "com.amazon.redshift.jdbc41.Driver"
  val REDSHIFT_JDBC_4_DRIVER = "com.amazon.redshift.jdbc4.Driver"
  val POSTGRESQL_DRIVER = "org.postgresql.Driver"
  val SECRET_MANAGER_REDSHIFT_DRIVER =
    "com.amazonaws.secretsmanager.sql.AWSSecretsManagerRedshiftDriver"

  // PostgreSQL drivers are not tested and not supported by the Amazon Redshift team -
  // https://docs.aws.amazon.com/redshift/latest/mgmt/configuring-connections.html#connecting-drivers
  val REDSHIFT_FIRST_PARTY_DRIVERS = Seq(REDSHIFT_JDBC_4_2_DRIVER, REDSHIFT_JDBC_4_1_DRIVER,
    REDSHIFT_JDBC_4_DRIVER, SECRET_MANAGER_REDSHIFT_DRIVER)
}
