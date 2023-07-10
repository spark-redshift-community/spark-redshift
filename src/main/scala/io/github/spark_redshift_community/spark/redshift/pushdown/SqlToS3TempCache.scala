/*
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

import java.util.concurrent.ConcurrentHashMap

object SqlToS3TempCache {
  private val sqlToS3Cache = new ConcurrentHashMap[String, String]()

  def getS3Path(sql : String): Option[String] = {
    Option(sqlToS3Cache.get(sql))
  }

  def setS3Path(sql : String, s3Path : String): Option[String] = {
    Option(sqlToS3Cache.put(sql, s3Path))
  }

  def clearCache(): Unit = {
    sqlToS3Cache.clear()
  }

}
