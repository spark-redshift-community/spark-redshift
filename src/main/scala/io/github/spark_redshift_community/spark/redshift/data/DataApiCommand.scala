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

import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient
import software.amazon.awssdk.services.redshiftdata.model._
import io.github.spark_redshift_community.spark.redshift.Utils
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.mutable.ArrayBuffer
import collection.JavaConverters._
import scala.collection.Seq

class DataApiCommand(connection: DataAPIConnection,
                     params: Option[Seq[QueryParameter[_]]] = None) {

  private val log = LoggerFactory.getLogger(getClass)
  private var client: RedshiftDataClient = null
  private var requestId: String = ""

  def execute(sql: String): Boolean = {
    // Validate, execute and wait for the command to complete.
    checkExecuteAndWait(Seq(sql))

    // Return whether there are results.
    hasResults()
  }

  def executeUpdate(sql: String): Long = {
    // Validate, execute and wait for the command to complete.
    checkExecuteAndWait(Seq(sql))

    // Get the number of result rows
    getResultRows()
  }

  def executeQueryInterruptibly(sql: String): RedshiftResults = {
    // Validate, execute and wait for the command to complete.
    checkExecuteAndWait(Seq(sql))

    // Get the results
    getResults()
  }

  def executeBatch(sqls: Seq[String]): Boolean = {
    // Validate, execute and wait for the command to complete.
    checkExecuteAndWait(sqls)

    // Return whether there are results.
    hasResults()
  }

  def cancel(): Boolean = {
    // Request cancellation
    cancelRequest()
  }

  /**
   * Returns the application name. Conforms to naming conventions of Data API for
   * setting the application name. The name after "Client:" must only contain alpha characters
   * with no spaces or special characters. Otherwise, they will be ignored.
   */
  private def applicationName: String = s"Client:${Utils.getApplicationName(connection.params)}"

  /**
   * Primary entry point for executing commands using the Data API. All public methods should
   * use this method to federate their calls to Data API.
   * @param sqls The set of commands to execute.
   */
  private def checkExecuteAndWait(sqls: Seq[String]): Unit = {
    // Make sure the input is not empty.
    if (sqls == null || sqls.isEmpty || sqls.exists(_.isEmpty)) {
      throw new IllegalArgumentException("Data API cannot execute missing or empty commands!")
    }

    // Prepend setting the query group as a separate command if it's requested.
    val updatedSqls = if (connection.queryGroup.isDefined) {
      val strQG = s"""set query_group to '${connection.queryGroup.get}'"""
      ArrayBuffer(strQG) ++= sqls
    } else {
      sqls
    }

    // Make sure there are no parameters being used with multiple commands or the query group
    // since Data API does not support this.
    if (params.isDefined && (updatedSqls.length > 1)) {
      throw new IllegalArgumentException(
        "Data API parameters require a single command with no query group specified!")
    }

    // Check whether we should do a single or batch execution.
    if (updatedSqls.length == 1) {
      singleExecuteAndWait(updatedSqls.head)
    } else {
      batchExecuteAndWait(updatedSqls)
    }
  }

  /**
   * Executes a single statement. Note that multi-part statements are not permitted by DataAPI.
   * @param sql The command to execute.
   */
  private def singleExecuteAndWait(sql: String): Unit = {
    // Make sure the input is not empty.
    if (sql == null || sql.isEmpty) {
      throw new IllegalArgumentException("Cannot execute null or empty command!")
    }

    // Create the DataAPI client.
    initializeDataApiClient()

    // Initialize the statement request
    val statementRequestBuilder = ExecuteStatementRequest.builder()
      .database(connection.params.dataApiDatabase.getOrElse(
        throw new IllegalArgumentException("Data API database is required!")
      ))
      .sql(sql)
      .statementName(applicationName)

    if (connection.params.dataApiCluster.isDefined) {
      statementRequestBuilder.clusterIdentifier(connection.params.dataApiCluster.get)
    }
    if (connection.params.dataApiWorkgroup.isDefined) {
      statementRequestBuilder.workgroupName(connection.params.dataApiWorkgroup.get)
    }
    if (connection.params.dataApiUser.isDefined) {
      statementRequestBuilder.dbUser(connection.params.dataApiUser.get)
    }
    if (connection.params.secretId.isDefined) {
      statementRequestBuilder.secretArn(connection.params.secretId.get)
    }

    // Initialize any parameters
    if (params.isDefined) {
      val sqlParams = params.get.map { qp =>
        if (qp.value.isEmpty || qp.value.get.toString.isEmpty) {
          throw new IllegalArgumentException("Query parameters must not be null or non-empty!")
        }
        SqlParameter.builder()
          .name(qp.name)
          .value(qp.value.get.toString)
          .build()
      }
      if (sqlParams.nonEmpty) {
        statementRequestBuilder.parameters(sqlParams.asJava)
      }
    }

    // Execute the statement request and remember the handle for potential cancellation later.
    val result = client.executeStatement(statementRequestBuilder.build())
    requestId = result.id()
    log.info("Issued Redshift Data API execute statement with request id: {}", requestId)

    // Wait for the request to finish.
    awaitCompletion()
  }

  private def batchExecuteAndWait(sqls: Seq[String]): Unit = {
    // Make sure the input is not empty.
    if (sqls == null || sqls.isEmpty || sqls.exists(_.isEmpty)) {
      throw new IllegalArgumentException("Batch execution requires at least one command!")
    }

    // Create the DataAPI client.
    initializeDataApiClient()

    // Initialize the statement request
    val statementRequestBuilder = BatchExecuteStatementRequest.builder()
      .database(connection.params.dataApiDatabase.getOrElse(
        throw new IllegalArgumentException("Data API database is required!")
      ))
      .sqls(sqls.asJava)
      .statementName(applicationName)

    if (connection.params.dataApiCluster.isDefined) {
      statementRequestBuilder.clusterIdentifier(connection.params.dataApiCluster.get)
    }
    if (connection.params.dataApiWorkgroup.isDefined) {
      statementRequestBuilder.workgroupName(connection.params.dataApiWorkgroup.get)
    }
    if (connection.params.dataApiUser.isDefined) {
      statementRequestBuilder.dbUser(connection.params.dataApiUser.get)
    }
    if (connection.params.secretId.isDefined) {
      statementRequestBuilder.secretArn(connection.params.secretId.get)
    }

    // Make sure there are no parameters since batch statements don't support them.
    if (params.isDefined) {
      throw new IllegalArgumentException(
        "Parameters are not permitted with Data API batch execution!")
    }

    // Execute the statement request and remember the handle for potential cancellation later.
    val result = client.batchExecuteStatement(statementRequestBuilder.build())
    requestId = result.id()
    log.info("Issued Redshift Data API batch execute statement with request id: {}", requestId)

    // Wait for the request to finish.
    awaitCompletion()
  }

  private def initializeDataApiClient(): Unit = {
    // Create the DataAPI client.
    client = Utils.createDataApiClient(connection.params.dataApiRegion)
  }

  val DATA_API_RETRY_DELAY_MIN_KEY = "spark.datasource.redshift.community.data_api_retry_delay_min"
  val DATA_API_RETRY_DELAY_MAX_KEY = "spark.datasource.redshift.community.data_api_retry_delay_max"
  val DATA_API_RETRY_DELAY_MULT_KEY = "spark.datasource.redshift.community.data_api_retry_delay_mult"
  val DATA_API_RETRY_DELAY_MIN_DEFAULT = "100.0" // milliseconds
  val DATA_API_RETRY_DELAY_MAX_DEFAULT = "250.0" // milliseconds
  val DATA_API_RETRY_DELAY_MULT_DEFAULT = "1.25" // Multiplier
  private def getDataApiDelayParams(): (Double, Double, Double) = {
    // Get the delay parameters
    val retryDelayMin = Utils.getSparkConfigValue(
      DATA_API_RETRY_DELAY_MIN_KEY, DATA_API_RETRY_DELAY_MIN_DEFAULT).toDouble
    val retryDelayMax = Utils.getSparkConfigValue(
      DATA_API_RETRY_DELAY_MAX_KEY, DATA_API_RETRY_DELAY_MAX_DEFAULT).toDouble
    val retryDelayMult = Utils.getSparkConfigValue(
      DATA_API_RETRY_DELAY_MULT_KEY, DATA_API_RETRY_DELAY_MULT_DEFAULT).toDouble

    (retryDelayMin, retryDelayMax, retryDelayMult)
  }

  private def awaitCompletion(): Unit = {
    // Check the status of the result.
    val describeStatementRequest = DescribeStatementRequest.builder()
      .id(requestId)
      .build()
    var describeResult: DescribeStatementResponse = null

    // Get the retry delays.
    val (retryDelayMin, retryDelayMax, retryDelayMult) = getDataApiDelayParams()

    // Poll until the result is ready.
    var period = retryDelayMin
    do {
      // Use an exponential-backoff and wait policy
      Thread.sleep(period.toLong)
      period *= retryDelayMult
      period = Math.min(period, retryDelayMax)

      // Check if the command is complete.
      describeResult = client.describeStatement(describeStatementRequest)
    } while (describeResult.status() != StatusString.FINISHED &&
      describeResult.status() != StatusString.ABORTED &&
      describeResult.status() != StatusString.FAILED)

    // Ensure the query completed successfully.
    if (describeResult.status() == StatusString.ABORTED) {
      throw new DataApiRuntimeException("DataAPI query was aborted")
    }
    if (describeResult.status() == StatusString.FAILED) {
      val error = Option(describeResult.error()).getOrElse("unknown error")
      throw new DataApiRuntimeException("DataAPI query execution failed: " + error)
    }

    log.info("The following Redshift Data API request id completed successfully: {}", requestId)
  }

  private def hasResults(): Boolean = {
    val describeStatementRequest = DescribeStatementRequest.builder()
      .id(requestId)
      .build()
    val describeResults = client.describeStatement(describeStatementRequest)
    describeResults.hasResultSet()
  }

  private def getResults(): DataApiResults = {
    val describeStatementRequest = DescribeStatementRequest.builder()
      .id(requestId)
      .build()
    val describeResults = client.describeStatement(describeStatementRequest)

    // Check if we have sub-statement results. If so, use the last one since we
    // can prefix sqls with the setting of the query group.
    val resultId = Option(describeResults.subStatements())
      .map(_.asScala)
      .filter(_.nonEmpty)
      .map(_.last.id())
      .getOrElse(requestId)

    val statementResultRequest = GetStatementResultRequest.builder()
      .id(resultId)
      .build()
    val results = client.getStatementResult(statementResultRequest)
    DataApiResults(results)
  }

  private def getResultRows(): Long = {
    val describeStatementRequest = DescribeStatementRequest.builder()
      .id(requestId)
      .build()
    val describeResults = client.describeStatement(describeStatementRequest)

    // Check if we have sub-statement results. If so, use the last one since we
    // can prefix sqls with the setting of the query group.
    if ((describeResults.subStatements() == null) ||
      describeResults.subStatements().asScala.isEmpty) {
      describeResults.resultRows()
    } else {
      describeResults.subStatements().asScala.last.resultRows()
    }
  }

  private def cancelRequest(): Boolean = {
    // Request cancellation
    val cancelRequest = CancelStatementRequest.builder()
      .id(requestId)
      .build()
    val cancelResult = client.cancelStatement(cancelRequest)
    cancelResult.status()
  }
}
