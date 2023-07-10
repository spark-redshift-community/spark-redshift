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

package io.github.spark_redshift_community.spark.redshift.pushdown

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{StructField, StructType}

/** RedshiftPlan, with RDD defined by custom query. */
case class RedshiftPlan(output: Seq[Attribute], rdd: RDD[InternalRow])
  extends SparkPlan {

  override def children: Seq[SparkPlan] = Nil
  protected override def doExecute(): RDD[InternalRow] = {

    val schema = StructType(
      output.map(attr => StructField(attr.name, attr.dataType, attr.nullable))
    )

    rdd.mapPartitions { iter =>
      val project = UnsafeProjection.create(schema)
      iter.map(project)
    }
  }

  override def simpleString(maxFields: Int): String = {
      super.simpleString(maxFields) + " " + output.mkString("[", ",", "]")
  }

  override def simpleStringWithNodeId(): String = {
    super.simpleStringWithNodeId() + " " + output.mkString("[", ",", "]")
  }

  // withNewChildrenInternal() is a new interface function from spark 3.2 in
  // org.apache.spark.sql.catalyst.trees.TreeNode. For details refer to
  // https://github.com/apache/spark/pull/32030
  // As for spark connector the RedshiftPlan is a leaf Node, we don't expect
  // caller to set any new children for it.
  // RedshiftPlan is only used for spark connector PushDown. Even if the Exception is
  // raised, the PushDown will not be used and it still works.
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = {
    if (newChildren.nonEmpty) {
      throw new Exception("Spark connector internal error: " +
        "RedshiftPlan.withNewChildrenInternal() is called to set some children nodes.")
    }
    this
  }
}