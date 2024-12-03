/*
 * Copyright 2015 Databricks
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package io.github.spark_redshift_community.spark.redshift

import java.net.URI
import com.amazonaws.auth._
import io.github.spark_redshift_community.spark.redshift.Parameters.MergedParameters
import org.apache.hadoop.conf.Configuration

private[redshift] object AWSCredentialsUtils {

  /**
    * Generates a credentials string for use in Redshift COPY and UNLOAD statements.
    * Favors a configured `aws_iam_role` if available in the parameters.
    */
  def getRedshiftCredentialsString(
      params: MergedParameters,
      sparkAwsCredentials: AWSCredentials): String = {

    def awsCredsToString(credentials: AWSCredentials): String = {
      credentials match {
        case creds: AWSSessionCredentials =>
          s"aws_access_key_id=${creds.getAWSAccessKeyId};" +
            s"aws_secret_access_key=${creds.getAWSSecretKey};token=${creds.getSessionToken}"
        case creds =>
          s"aws_access_key_id=${creds.getAWSAccessKeyId};" +
            s"aws_secret_access_key=${creds.getAWSSecretKey}"
      }
    }

    if (params.iamRole.isDefined) {
      s"aws_iam_role=${params.iamRole.get}"
    } else if (params.temporaryAWSCredentials.isDefined) {
      awsCredsToString(params.temporaryAWSCredentials.get.getCredentials)
    } else if (params.forwardSparkS3Credentials) {
      awsCredsToString(sparkAwsCredentials)
    } else {
      throw new IllegalStateException("No Redshift S3 authentication mechanism was specified")
    }
  }
  def staticCredentialsProvider(credentials: AWSCredentials): AWSCredentialsProvider = {
    new AWSCredentialsProvider {
      override def getCredentials: AWSCredentials = credentials
      override def refresh(): Unit = {}
    }
  }

  def load(params: MergedParameters, hadoopConfiguration: Configuration): AWSCredentialsProvider = {
    // Load the credentials.
    params.temporaryAWSCredentials.getOrElse(loadFromURI(params.rootTempDir, hadoopConfiguration))
  }

  private def loadFromURI(
      tempPath: String,
      hadoopConfiguration: Configuration): AWSCredentialsProvider = {
    // scalastyle:off
    // A good reference on Hadoop's configuration loading / precedence is
    // https://github.com/apache/hadoop/blob/trunk/hadoop-tools/hadoop-aws/src/site/markdown/tools/hadoop-aws/index.md
    // scalastyle:on
    val uri = new URI(tempPath)
    val uriScheme = uri.getScheme

    uriScheme match {
      case "s3" | "s3n" | "s3a" =>
         new DefaultAWSCredentialsProviderChain()
      case other =>
        throw new IllegalArgumentException(s"Unrecognized scheme $other; expected s3, s3n, or s3a")
    }
  }
}
