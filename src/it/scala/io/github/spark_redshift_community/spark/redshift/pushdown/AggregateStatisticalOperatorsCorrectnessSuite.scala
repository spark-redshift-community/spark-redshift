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

import org.apache.spark.sql.Row

abstract class AggregateStatisticalOperatorsCorrectnessSuite
  extends AggregateStddevSampCorrectnessSuite
  with AggregateStddevPopCorrectnessSuite
  with AggregateVarSampCorrectnessSuite
  with AggregateVarPopCorrectnessSuite {

  override protected val preloaded_data: String = "true"
  override def setTestTableName(): String = """"PUBLIC"."all_shapes_dist_all_sort_compound_12col""""

}

class TextAggregateStatisticalOperatorsCorrectnessSuite
  extends AggregateStatisticalOperatorsCorrectnessSuite {
  override protected val s3format: String = "TEXT"
}

class ParquetAggregateStatisticalOperatorsCorrectnessSuite
  extends AggregateStatisticalOperatorsCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
}

class TextPushdownNoCacheAggregateStatisticalOperatorsCorrectnessSuite
  extends AggregateStatisticalOperatorsCorrectnessSuite {
  override protected val s3_result_cache = "false"
}

class ParquetPushdownNoCacheAggregateStatisticalOperatorsCorrectnessSuite
  extends AggregateStatisticalOperatorsCorrectnessSuite {
  override protected val s3_result_cache = "false"
}

class ParquetNoPushdownAggregateStatisticalOperatorsCorrectnessSuite
  extends AggregateStatisticalOperatorsCorrectnessSuite {
  override protected val s3format: String = "PARQUET"
  override protected val auto_pushdown: String = "false"
}

class TextNoPushdownAggregateStatisticalOperatorsCorrectnessSuite
  extends AggregateStatisticalOperatorsCorrectnessSuite {
  override protected val s3format: String = "TEXT"
  override protected val auto_pushdown: String = "false"
}
