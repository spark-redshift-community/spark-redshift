package io.github.spark_redshift_community.spark.redshift

import org.apache.spark.sql.execution.datasources.PartitionedFile

import java.net.URI

// cannot be a companion object since it must be in a different file
private[redshift] object RedshiftFileFormatUtils {
  def uriFromPartitionedFile(file: PartitionedFile): URI = file.pathUri
}
