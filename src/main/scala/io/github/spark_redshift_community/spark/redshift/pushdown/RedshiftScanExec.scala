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

package io.github.spark_redshift_community.spark.redshift.pushdown

import io.github.spark_redshift_community.spark.redshift.RedshiftRelation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.execution.LeafExecNode

import java.util.concurrent.{Callable, ExecutorService, Executors, Future}

/**
 * Redshift Scan Plan for pushing query fragment to redshift endpoint and
 * reading data from UNLOAD location
 *
 * @param output   projected columns
 * @param query    SQL query that is pushed to redshift for evaluation
 * @param relation Redshift node aiding in redshift cluster connection
 */
case class RedshiftScanExec(output: Seq[Attribute],
                            query: RedshiftSQLStatement,
                            relation: RedshiftRelation)
  extends LeafExecNode {

  @transient implicit private var data: Future[RedshiftPushdownResult] = _
  @transient implicit private val service: ExecutorService = Executors.newCachedThreadPool()

  // this is the thread which constructed this not necessarily the executing thread
  private val threadName = Thread.currentThread.getName

  override protected def doPrepare(): Unit = {
    logInfo("Preparing query to push down to redshift")

    val work = new Callable[RedshiftPushdownResult]() {
      override def call(): RedshiftPushdownResult = {
        val result = {
          try {
            val data = relation.buildScanFromSQL[InternalRow](query, Some(schema), threadName)
            RedshiftPushdownResult(data = Some(data))
          } catch {
            case e: Exception =>
              logError("Failure in redshift query execution", e)
              RedshiftPushdownResult(failure = Some(e))
          }
        }
        result
      }
    }
    data = service.submit(work)
    logInfo("submitted query to redshift asynchronously")
  }

  override protected def doExecute(): RDD[InternalRow] = {
    if (data.get().failure.nonEmpty) {
      // raise original exception
      throw data.get().failure.get
    }

    data.get().data.get
  }

  override def cleanupResources(): Unit = {
    logDebug(s"shutting down service to clean up resources")
    if (service != null) {
      service.shutdown()
    }
  }
}

/**
 * Result holder
 *
 * @param data    RDD that holds the data from UNLOAD location
 * @param failure failure information if we unable to push down to
 *                redshift or read unload data
 */
private case class RedshiftPushdownResult(data: Option[RDD[InternalRow]] = None,
                                          failure: Option[Exception] = None)
  extends Serializable
