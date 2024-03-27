/*
 * Copyright 2015-2018 Snowflake Computing
 * Copyright 2015 Databricks
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

package io.github.spark_redshift_community.spark.redshift

import io.github.spark_redshift_community.spark.redshift.pushdown.{ConstantString, Identifier}

import scala.collection.mutable.ArrayBuffer

/**
 * Wrapper class for representing the name of a Redshift table.
 */
private[redshift] case class TableName(
    unescapedSchemaName: String,
    unescapedTableName: String,
    addAutomount: Boolean = false) {
  private def quote(str: String) = '"' + str.replace("\"", "\"\"") + '"'

  private def automountPrefix = if (addAutomount) s"""${quote("awsdatacatalog")}.""" else ""
  def escapedSchemaName: String = quote(unescapedSchemaName)
  def escapedTableName: String = quote(unescapedTableName)
  override def toString: String = s"$automountPrefix$escapedSchemaName.$escapedTableName"
  def toStatement: Identifier =
    Identifier(s"$automountPrefix$escapedSchemaName.$escapedTableName")
  def toConstantString: ConstantString =
    ConstantString(s"$automountPrefix$escapedSchemaName.$escapedTableName")
}

private[redshift] object TableName {
  /**
   * Parses a table name which is assumed to have been escaped according to Redshift's rules for
   * delimited identifiers.
   */
  def parseFromEscaped(str: String, addAutomount: Boolean = false): TableName = {
    def dropOuterQuotes(s: String) =
      if (s.startsWith("\"") && s.endsWith("\"")) s.drop(1).dropRight(1) else s
    def unescapeQuotes(s: String) = s.replace("\"\"", "\"")
    def unescape(s: String) = unescapeQuotes(dropOuterQuotes(s))
    splitByDots(str) match {
      case Seq(tableName) =>
        TableName("PUBLIC", unescape(tableName), addAutomount)
      case Seq(schemaName, tableName) =>
        TableName(unescape(schemaName), unescape(tableName), addAutomount)
      case Seq("awsdatacatalog", schemaName, tableName) =>
        TableName(unescape(schemaName), unescape(tableName), addAutomount = true)
      case other => throw new IllegalArgumentException(s"Could not parse table name from '$str'")
    }
  }

  /**
   * Split by dots (.) while obeying our identifier quoting rules in order to allow dots to appear
   * inside of quoted identifiers.
   */
  private def splitByDots(str: String): Seq[String] = {
    val parts: ArrayBuffer[String] = ArrayBuffer.empty
    val sb = new StringBuilder
    var inQuotes: Boolean = false
    for (c <- str) c match {
      case '"' =>
        // Note that double quotes are escaped by pairs of double quotes (""), so we don't need
        // any extra code to handle them; we'll be back in inQuotes=true after seeing the pair.
        sb.append('"')
        inQuotes = !inQuotes
      case '.' =>
        if (!inQuotes) {
          parts.append(sb.toString())
          sb.clear()
        } else {
          sb.append('.')
        }
      case other =>
        sb.append(other)
    }
    if (sb.nonEmpty) {
      parts.append(sb.toString())
    }
    parts.toSeq
  }
}
