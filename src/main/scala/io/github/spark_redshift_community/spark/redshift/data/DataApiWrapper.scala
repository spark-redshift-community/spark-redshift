/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.spark_redshift_community.spark.redshift.data

import io.github.spark_redshift_community.spark.redshift

import java.sql.{ResultSetMetaData, SQLException}
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal
import io.github.spark_redshift_community.spark.redshift.Parameters.MergedParameters
import io.github.spark_redshift_community.spark.redshift.TimestampNTZTypeExtractor
import io.github.spark_redshift_community.spark.redshift.pushdown.{BooleanVariable, ByteVariable, ConstantString, DoubleVariable, FloatVariable, IntVariable, LongVariable, RedshiftSQLStatement, ShortVariable, StatementElement, StringVariable}
import org.slf4j.LoggerFactory

import java.util.concurrent.{ConcurrentHashMap, Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext
import scala.concurrent.{Await, Future}
import scala.util.Try
import scala.collection.JavaConverters._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

private[redshift] class DataApiWrapper extends RedshiftWrapper with Serializable {

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
        thread.setName(s"spark-redshift-DataApiWrapper-${count.incrementAndGet}")
        thread.setDaemon(true)
        thread
      }
    }
    ExecutionContext.fromExecutorService(Executors.newCachedThreadPool(threadFactory))
  }

  @transient private lazy val cancellationMap =
    new ConcurrentHashMap[DataApiCommand, Int]()

  // Adding ShutdownHook
  sys.addShutdownHook {
    cancellationMap.forEach { (statement, callNumber) =>
      try {
        log.info(s"Cancelling pending Data API call {}", callNumber)
        statement.cancel()
      } catch {
        case e: Throwable =>
          log.error("Exception occurred while cancelling Data API request: {}", e.getMessage)
      }
    }
  }

  /**
   *
   * @param cmd
   * @param op
   * @tparam T
   * @return
   */
  private def executeInterruptibly[T](cmd: DataApiCommand,
                                      op: DataApiCommand => T): T = {
    val callNumber = callNumberGenerator.next
    try {
      log.info("Begin Redshift Data API call {}", callNumber)
      cancellationMap.put(cmd, callNumber)
      val future = Future[T](op(cmd))(ec)
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
          cmd.cancel()
          throw e
        } catch {
          case s: SQLException =>
            log.error("Exception occurred while cancelling query: {}", s.getMessage)
            throw e
        }
    }
    finally {
      cancellationMap.remove(cmd)
      log.info("End Redshift Data API call {}", callNumber)
    }
  }

  override def setAutoCommit(conn: RedshiftConnection, autoCommit: Boolean): Unit = {
    // Ignore if the value is not changing.
    if (conn.getAutoCommit() != autoCommit) {
      conn.setAutoCommit(autoCommit)

      // Automatically commit any pending commands when value is changing.
      // (e.g. re-enabling auto-commit)
      executeBufferedCommands(conn)
    }
  }

  override def commit(conn: RedshiftConnection): Unit = {
    // Make sure we are not already auto-committing.
    if (conn.getAutoCommit) {
      throw new IllegalStateException("Cannot commit when autoCommit is enabled.")
    }

    // Execute the buffered commands (if any).
    executeBufferedCommands(conn)
  }

  override def rollback(conn: RedshiftConnection): Unit = {
    // Make sure we are not already auto-committing.
    if (conn.getAutoCommit) {
      throw new IllegalStateException("Cannot rollback when autoCommit is enabled.")
    }

    // Clear out any buffered commands.
    val dataApiConnection = conn.asInstanceOf[DataAPIConnection]
    dataApiConnection.bufferedCommands.clear()
  }

  private def executeBufferedCommands(conn: RedshiftConnection): Unit = {
    val dataApiConnection = conn.asInstanceOf[DataAPIConnection]

    // If there are buffered commands, execute them.
    if (dataApiConnection.bufferedCommands.length > 0) {

      // Execute the buffered commands (if any).
      executeBatch(conn, dataApiConnection.bufferedCommands)

      // Reset for next time.
      dataApiConnection.bufferedCommands.clear()
    }
  }

  /**
   * Executes a sequence of SQL statements.
   *
   * @param params
   * @param params
   * @param sqlStatements
   * @return
   */
  private def executeBatch(conn: RedshiftConnection,
                            sqls: Seq[String]): Boolean = {
    val dataApiConnection = conn.asInstanceOf[DataAPIConnection]
    executeInterruptibly(new DataApiCommand(dataApiConnection), _.executeBatch(sqls))
  }

  /**
   * * Execute the given SQL statement while supporting interruption.
   * If InterruptedException is caught, then the statement will be cancelled if it is running.
   *
   * @param conn
   * @param sql
   * @return <code>true</code> if the first result is a <code>ResultSet</code>
   * object; <code>false</code> if the first result is an update
   * count or there is no result
   */
  override def executeInterruptibly(conn: RedshiftConnection, sql: String): Boolean = {
    val dataApiConnection = conn.asInstanceOf[DataAPIConnection]

    // If we are not in auto-commit mode, buffer the command for later batch execution.
    if (!conn.getAutoCommit()) {
      log.info("Buffering statement for batch execution later since auto-commit is disabled.")
      dataApiConnection.bufferedCommands.append(sql)
      false
    } else {
      executeInterruptibly(new DataApiCommand(dataApiConnection), _.execute(sql))
    }
  }

  override def executeQueryInterruptibly(conn: RedshiftConnection, sql: String)
  : RedshiftResults = {
    // Make sure we are in auto-commit mode. This command is not supported otherwise.
    if (!conn.getAutoCommit()) {
      throw new IllegalStateException("Cannot execute queries while not in auto-commit mode!")
    }

    val dataApiConnection = conn.asInstanceOf[DataAPIConnection]
    executeInterruptibly(
      new DataApiCommand(dataApiConnection), _.executeQueryInterruptibly(sql))
  }

  override def executeUpdateInterruptibly(conn: RedshiftConnection, sql: String)
  : Long = {
    val dataApiConnection = conn.asInstanceOf[DataAPIConnection]

    // If we are not in auto-commit mode, buffer the command for later batch execution.
    if (!conn.getAutoCommit()) {
      log.info("Buffering statement for batch execution later since auto-commit is disabled.")
      dataApiConnection.bufferedCommands.append(sql)
      0
    } else {
      executeInterruptibly(new DataApiCommand(dataApiConnection), _.executeUpdate(sql))
    }
  }

  override def executeUpdate(conn: RedshiftConnection, sql: String): Long = {
    val dataApiConnection = conn.asInstanceOf[DataAPIConnection]

    // If we are not in auto-commit mode, buffer the command for later batch execution.
    if (!conn.getAutoCommit()) {
      log.info("Buffering statement for batch execution later since auto-commit is disabled.")
      dataApiConnection.bufferedCommands.append(sql)
      0
    } else {
      executeInterruptibly(new DataApiCommand(dataApiConnection), _.executeUpdate(sql))
    }
  }

  override def getConnector(params: MergedParameters): RedshiftConnection = {
    DataAPIConnection(params)
  }

  override def getConnectorWithQueryGroup(params: MergedParameters,
                                          queryGroup: String): RedshiftConnection = {
    DataAPIConnection(params, Option(queryGroup))
  }

  /**
   * Returns true if the table already exists in the Redshift database.
   */
  override def tableExists(conn: RedshiftConnection, table: String): Boolean = {
    // Somewhat hacky, but there isn't a good way to identify whether a table exists for all
    // SQL database systems, considering "table" could also include the database name.
    Try {
      val sql = s"SELECT 1 FROM $table LIMIT 1"
      val results = executeInterruptibly(
        new DataApiCommand(conn.asInstanceOf[DataAPIConnection]),
        _.executeQueryInterruptibly(sql)).asInstanceOf[DataApiResults].results
      results.getColumnMetadata.asScala.length
    }.isSuccess
  }

  override def resolveTable(conn: RedshiftConnection,
                            table: String,
                            params: Option[MergedParameters] = None): StructType = {
    // It's important to leave the `LIMIT 1` clause in order to limit the work of the query in case
    // the underlying JDBC driver implementation implements PreparedStatement.getMetaData() by
    // executing the query. It looks like the standard Redshift and Postgres JDBC drivers don't do
    // this but we leave the LIMIT condition here as a safety-net to guard against perf regressions.
    log.info("Getting schema from Redshift for table: {}", table)
    val res = executeQueryInterruptibly(conn, s"SELECT * FROM $table LIMIT 1")
      .asInstanceOf[DataApiResults]
    val rsmd = res.results.getColumnMetadata.asScala
    val ncols = rsmd.length
    val fields = {
      new Array[StructField](ncols)
    }
    var i = 0
    while (i < ncols) {
      val colmd = rsmd.apply(i)

      val columnName = colmd.getLabel
      val rsType = colmd.getTypeName
      val fieldSize = colmd.getPrecision
      val fieldScale = colmd.getScale
      val isSigned = colmd.isSigned
      val nullable = if (params.exists(_.overrideNullable)) {
        true
      } else colmd.getNullable != ResultSetMetaData.columnNoNulls
      val columnType = getCatalystType(rsType, fieldSize, fieldScale, isSigned, params)
      val meta = new MetadataBuilder().putString("redshift_type", rsType).build()

      fields(i) = StructField(columnName, columnType, nullable, meta)
      i = i + 1
    }
    new StructType(fields)
  }

  private def resolveTableFromMeta(results: RedshiftResults,
                                    params: MergedParameters): StructType = {
    val rsmd = results.asInstanceOf[DataApiResults].results.getColumnMetadata.asScala
    val ncols = rsmd.length
    val fields = new Array[StructField](ncols)
    var i = 0
    while (i < ncols) {
      val colmd = rsmd.apply(i)

      val columnName = colmd.getLabel
      val rsType = colmd.getTypeName
      val fieldSize = colmd.getPrecision
      val fieldScale = colmd.getScale
      val isSigned = colmd.isSigned
      val nullable = colmd.getNullable != ResultSetMetaData.columnNoNulls
      val columnType = getCatalystType(rsType, fieldSize, fieldScale, isSigned, Some(params))
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
                               redshiftType: String,
                               precision: Int,
                               scale: Int,
                               signed: Boolean,
                               params: Option[MergedParameters] = None): DataType = {
    val answer = redshiftType match {
      // scalastyle:off
      case "oid" => if (signed) { LongType } else { DecimalType(20, 0) }
      case "money" => DoubleType
      case "double precision" => DoubleType
      case "bpchar" => StringType
      case "text" => StringType
      case "name" => StringType
      case "character varying" => StringType
      case "bit" => BooleanType
      case "time without time zone" => TimestampType
      case "timestamp without time zone" => if (redshift.legacyTimestampHandling) TimestampType else TimestampNTZTypeExtractor.defaultType
      case "timestamp with time zone" => TimestampType
      case "xid" => if (signed) { LongType } else { DecimalType(20, 0) }
      case "tid" => StringType
      case "abstime" => TimestampType
      case "int2" => if (params.exists(_.legacyMappingShortToInt)) { IntegerType } else { ShortType }
      case "int4" => if (signed) { IntegerType } else { LongType }
      case "int8" => if (signed) { LongType } else { DecimalType(20, 0) }
      case "numeric" if precision != 0 || scale != 0 => DecimalType(precision, scale)
      case "float4" =>
        if (params.exists(_.legacyJdbcRealTypeMapping)) { DoubleType } else { FloatType }
      case "float8" => DoubleType
      case "char" => StringType
      case "varchar" => StringType
      case "bool" => BooleanType
      case "date" => DateType
      case "time" => TimestampType
      case "time with time zone" => TimestampType
      case "timetz" => TimestampType
      case "timestamp" => if (redshift.legacyTimestampHandling) TimestampType else TimestampNTZTypeExtractor.defaultType
      case "timestamptz" => TimestampType
      case "oidvector" => StringType
      case "super" => StringType
      case _ => null
      // scalastyle:on
    }

    if (answer == null) throw new SQLException("Unsupported type " + redshiftType)
    answer
  }

  override def tableSchema(conn: RedshiftConnection,
                  statement: RedshiftSQLStatement,
                  params: MergedParameters): StructType =
    resolveTableFromMeta(
      tableMetaDataFromStatement(conn, statement),
      params
    )

  private def tableMetaDataFromStatement(conn: RedshiftConnection,
                                         statement: RedshiftSQLStatement,
                                         bindVariableEnabled: Boolean = true
                                        ): RedshiftResults = {
    val sqlStatement = (ConstantString("select * from") + statement + "where 1 = 0")
    val (query, params) = prepareStatement(sqlStatement, bindVariableEnabled)
    val dataApiConnection = conn.asInstanceOf[DataAPIConnection]
    executeInterruptibly(
      new DataApiCommand(dataApiConnection, params), _.executeQueryInterruptibly(query))
  }

  private def prepareStatement(sqlStatement: RedshiftSQLStatement,
                               bindVariableEnabled: Boolean):
  (String, Option[Seq[QueryParameter[_]]]) = {
    if (bindVariableEnabled) prepareWithBindVariable(sqlStatement)
    else (prepareWithoutBindVariable(sqlStatement), None)
  }

  private def prepareWithoutBindVariable(sqlStatement: RedshiftSQLStatement): String = {
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

    val logPrefix =
      s"""${MASTER_LOG_PREFIX}:
         | execute query without bind variable:
         |""".stripMargin.filter(_ >= ' ')

    query
  }

  private def prepareWithBindVariable(sqlStatement: RedshiftSQLStatement):
  (String, Option[Seq[QueryParameter[_]]]) = {
    val sql = sqlStatement.list.reverse
    val varArray: Array[StatementElement] =
      new Array[StatementElement](sqlStatement.numOfVar)
    var indexOfVar: Int = 0
    val buffer = new StringBuilder

    sql.foreach(element => {
      if (element.isInstanceOf[ConstantString]) {
        buffer.append(element)
      }
      else {
        buffer.append(s":param$indexOfVar")
        varArray(indexOfVar) = element
        indexOfVar += 1
      }
      buffer.append(" ")
    })

    val query: String = buffer.toString
    if (varArray.length == 0) {
      return (query, None)
    }

    val logPrefix =
      s"""${MASTER_LOG_PREFIX}:
         | execute query with bind variable:
         |""".stripMargin.filter(_ >= ' ')

    val parameters = new ArrayBuffer[QueryParameter[_]]
    varArray.zipWithIndex.foreach {
      case (element, index) =>
        val name = s"param$index"
        parameters += {
          element match {
            case ele: StringVariable => QueryParameter(name, ele.variable, java.sql.Types.VARCHAR)
            case ele: IntVariable => QueryParameter(name, ele.variable, java.sql.Types.INTEGER)
            case ele: LongVariable => QueryParameter(name, ele.variable, java.sql.Types.BIGINT)
            case ele: ShortVariable => QueryParameter(name, ele.variable, java.sql.Types.SMALLINT)
            case ele: FloatVariable => QueryParameter(name, ele.variable, java.sql.Types.FLOAT)
            case ele: DoubleVariable => QueryParameter(name, ele.variable, java.sql.Types.DOUBLE)
            case ele: BooleanVariable => QueryParameter(name, ele.variable, java.sql.Types.BOOLEAN)
            case ele: ByteVariable => QueryParameter(name, ele.variable, java.sql.Types.TINYINT)
            case _ =>
              throw new IllegalArgumentException(
                "Unexpected Element Type: " + element.getClass.getName
              )
          }
        }
    }

    (query, Option(parameters))
  }
}
