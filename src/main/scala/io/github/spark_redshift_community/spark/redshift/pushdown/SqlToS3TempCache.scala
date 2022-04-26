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
