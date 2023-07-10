/*
* Copyright 2015-2018 Snowflake Computing
* Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package io.github.spark_redshift_community.spark.redshift.pushdown

import io.github.spark_redshift_community.spark.redshift.DefaultJDBCWrapper
import org.slf4j.LoggerFactory

import java.sql.{Connection, PreparedStatement, ResultSet}

// scalastyle:off
/**
 * SQL string wrapper
 */
private[redshift] class RedshiftSQLStatement(
                                                val numOfVar: Int = 0,
                                                val list: List[StatementElement] = Nil
                                              ) {

  private val log = LoggerFactory.getLogger(getClass)

  private val MASTER_LOG_PREFIX = "Spark Connector Master"
  private val WORKER_LOG_PREFIX = "Spark Connector Worker"

  def +(element: StatementElement): RedshiftSQLStatement =
    new RedshiftSQLStatement(numOfVar + element.isVariable, element :: list)

  def +(statement: RedshiftSQLStatement): RedshiftSQLStatement =
    new RedshiftSQLStatement(
      numOfVar + statement.numOfVar,
      statement.list ::: list
    )

  def +(str: String): RedshiftSQLStatement = this + ConstantString(str)

  def isEmpty: Boolean = list.isEmpty

  def execute(
               bindVariableEnabled: Boolean
             )(implicit conn: Connection): ResultSet = {
    val statement = prepareStatement(bindVariableEnabled)
    try {
      val rs = DefaultJDBCWrapper.executeQueryInterruptibly(statement)
      rs
    } catch {
      case th: Throwable => {
        throw th
      }
    }
  }

  private[redshift] def prepareStatement(
                                           bindVariableEnabled: Boolean
                                         )(implicit conn: Connection): PreparedStatement =
    if (bindVariableEnabled) prepareWithBindVariable(conn)
    else parepareWithoutBindVariable(conn)

  private def parepareWithoutBindVariable(conn: Connection): PreparedStatement = {
    val sql = list.reverse
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

    conn.prepareStatement(query)
  }

  private def prepareWithBindVariable(conn: Connection): PreparedStatement = {
    val sql = list.reverse
    val varArray: Array[StatementElement] =
      new Array[StatementElement](numOfVar)
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

    val statement = conn.prepareStatement(query)
    varArray.zipWithIndex.foreach {
      case (element, index) =>
        element match {
          case ele: StringVariable =>
            if (ele.variable.isDefined)
              statement.setString(index + 1, ele.variable.get)
            else
              statement.setNull(index + 1, java.sql.Types.VARCHAR)
          case ele: Identifier =>
            statement.setString(index + 1, ele.variable.get)
          case ele: IntVariable =>
            if (ele.variable.isDefined)
              statement.setInt(index + 1, ele.variable.get)
            else
              statement.setNull(index + 1, java.sql.Types.INTEGER)
          case ele: LongVariable =>
            if (ele.variable.isDefined)
              statement.setLong(index + 1, ele.variable.get)
            else
              statement.setNull(index + 1, java.sql.Types.BIGINT)
          case ele: ShortVariable =>
            if (ele.variable.isDefined)
              statement.setShort(index + 1, ele.variable.get)
            else
              statement.setNull(index + 1, java.sql.Types.SMALLINT)
          case ele: FloatVariable =>
            if (ele.variable.isDefined)
              statement.setFloat(index + 1, ele.variable.get)
            else
              statement.setNull(index + 1, java.sql.Types.FLOAT)
          case ele: DoubleVariable =>
            if (ele.variable.isDefined)
              statement.setDouble(index + 1, ele.variable.get)
            else
              statement.setNull(index + 1, java.sql.Types.DOUBLE)
          case ele: BooleanVariable =>
            if (ele.variable.isDefined)
              statement.setBoolean(index + 1, ele.variable.get)
            else
              statement.setNull(index + 1, java.sql.Types.BOOLEAN)
          case ele: ByteVariable =>
            if (ele.variable.isDefined)
              statement.setByte(index + 1, ele.variable.get)
            else
              statement.setNull(index + 1, java.sql.Types.TINYINT)
          case _ =>
            throw new IllegalArgumentException(
              "Unexpected Element Type: " + element.getClass.getName
            )
        }
    }

    statement
  }

  override def equals(obj: scala.Any): Boolean =
    obj match {
      case other: RedshiftSQLStatement =>
        if (this.statementString == other.statementString) true else false
      case _ => false
    }

  override def toString: String = statementString

  def statementString: String = {
    val buffer = new StringBuilder
    val sql = list.reverse

    sql.foreach {
      case x: ConstantString =>
        if (buffer.nonEmpty && buffer.last != ' ') {
          buffer.append(" ")
        }
        buffer.append(x)
      case x: VariableElement[_] =>
        if (buffer.nonEmpty && buffer.last != ' ') {
          buffer.append(" ")
        }
        buffer.append(x.sql)
    }

    buffer.toString()
  }

}
// scalastyle:on

private[redshift] object EmptyRedshiftSQLStatement {
  def apply(): RedshiftSQLStatement = new RedshiftSQLStatement()
}

private[redshift] object ConstantStringVal {
  def apply(l: Any): StatementElement = {
    if (l == null || l.toString.toLowerCase == "null") {
      ConstantString("NULL")
    } else {
      ConstantString(l.toString)
    }
  }
}

private[redshift] sealed trait StatementElement {

  val value: String

  val isVariable: Int = 0

  def +(element: StatementElement): RedshiftSQLStatement =
    new RedshiftSQLStatement(
      isVariable + element.isVariable,
      element :: List[StatementElement](this)
    )

  def +(statement: RedshiftSQLStatement): RedshiftSQLStatement =
    new RedshiftSQLStatement(
      isVariable + statement.numOfVar,
      statement.list ::: List[StatementElement](this)
    )

  def +(str: String): RedshiftSQLStatement = this + ConstantString(str)

  override def toString: String = value

  def ! : RedshiftSQLStatement = toStatement

  def toStatement: RedshiftSQLStatement =
    new RedshiftSQLStatement(isVariable, List[StatementElement](this))

  def sql: String = value
}

private[redshift] case class ConstantString(override val value: String)
  extends StatementElement

private[redshift] sealed trait VariableElement[T] extends StatementElement {
  override val value = "?"

  override val isVariable: Int = 1

  val variable: Option[T]

  override def sql: String = if (variable.isDefined) variable.get.toString else "NULL"

}

private[redshift] case class Identifier(name: String) extends VariableElement[String] {
  override val variable = Some(name)
  override val value: String = "identifier(?)"
}

private[redshift] case class StringVariable(override val variable: Option[String])
  extends VariableElement[String] {
  override def sql: String = if (variable.isDefined) s"""'${variable.get}'""" else "NULL"
}

private[redshift] case class IntVariable(override val variable: Option[Int])
  extends VariableElement[Int]

private[redshift] case class LongVariable(override val variable: Option[Long])
  extends VariableElement[Long]

private[redshift] case class ShortVariable(override val variable: Option[Short])
  extends VariableElement[Short]

private[redshift] case class FloatVariable(override val variable: Option[Float])
  extends VariableElement[Float]

private[redshift] case class DoubleVariable(override val variable: Option[Double])
  extends VariableElement[Double]

private[redshift] case class BooleanVariable(override val variable: Option[Boolean])
  extends VariableElement[Boolean]

private[redshift] case class ByteVariable(override val variable: Option[Byte])
  extends VariableElement[Byte]
