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
package io.github.spark_redshift_community.spark.redshift.pushdown.querygeneration

import org.apache.spark.sql.catalyst.expressions.{Expression, GetMapValue}

private[querygeneration] object GetMapValueExtractor {
  def unapply(expr: Expression): Option[(Expression, Expression, Boolean)] = expr match {
    // set third tuple value representing failOnError to false
    // this is how GetMapValue in spark 3.4 works. Since the
    // parameter has been removed
    case GetMapValue(child, key) => Some(child, key, false)
    case _ => None
  }
}
