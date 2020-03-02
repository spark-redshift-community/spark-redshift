package io.github.spark_redshift_community.spark.redshift

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3ClientBuilder}

class AWSS3ClientFactory {
  def apply(creds: AWSCredentialsProvider): AmazonS3Client = {
    AmazonS3ClientBuilder.standard()
      .withCredentials(creds)
      .build().asInstanceOf[AmazonS3Client]
  }
}
