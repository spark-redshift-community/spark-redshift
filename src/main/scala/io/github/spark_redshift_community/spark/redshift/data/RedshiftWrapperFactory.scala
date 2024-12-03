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

import io.github.spark_redshift_community.spark.redshift.Parameters.MergedParameters
import io.github.spark_redshift_community.spark.redshift.data.RedshiftWrapperType.{DataAPI, JDBC, RedshiftInterfaceType}
import org.slf4j.LoggerFactory

private[redshift] object RedshiftWrapperType extends Enumeration {
  type RedshiftInterfaceType = Value
  val DataAPI, JDBC = Value
}

private[redshift] object RedshiftWrapperFactory {
  private val log = LoggerFactory.getLogger(getClass)
  private val jdbcSingleton = RedshiftWrapperFactory(JDBC)
  private val dataAPISingleton = RedshiftWrapperFactory(DataAPI)

  def apply(params: MergedParameters): RedshiftWrapper = {
    if (params.dataAPICreds) {
      log.debug("Using Data API to communicate with Redshift")
      dataAPISingleton
    } else {
      log.debug("Using JDBC to communicate with Redshift")
      jdbcSingleton
    }
  }

  private def apply(dataInterfaceType: RedshiftInterfaceType): RedshiftWrapper = {
    dataInterfaceType match {
      case DataAPI => new DataApiWrapper()
      case _ => new JDBCWrapper()
    }
  }
}
