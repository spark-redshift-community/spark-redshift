package io.github.spark_redshift_community.spark.redshift.v2;

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import io.github.spark_redshift_community.spark.redshift.Parameters.MergedParameters
import io.github.spark_redshift_community.spark.redshift.{JDBCWrapper, RedshiftWriter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.StructType

case class RedshiftWriteBuilder(schema: StructType, options: JdbcOptionsInWrite)
  extends WriteBuilder with SupportsTruncate with Logging{

  private var isTruncate = false

  override def truncate(): WriteBuilder = {
    isTruncate = true
    this
  }

  override def build(): V1Write = new V1Write {
    override def toInsertableRelation: InsertableRelation = (data: DataFrame, _: Boolean) => {
      var saveMode = SaveMode.Append
      if (isTruncate) {
        saveMode = SaveMode.Overwrite
      }
      logWarning("using insert table relation")
      val s3ClientFactory: AWSCredentialsProvider => AmazonS3Client =
        awsCredentials => new AmazonS3Client(awsCredentials)
      new RedshiftWriter(new JDBCWrapper(), s3ClientFactory)
        .saveToRedshift(data.sqlContext, data, saveMode, MergedParameters(options.parameters.toMap))
    }
  }
}
