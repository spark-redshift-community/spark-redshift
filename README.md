
[//]: # (                                                                                  )
[//]: # ( Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. )
[//]: # (                                                                                  )
[//]: # ( Licensed under the Apache License, Version 2.0 \(the "License"\);                )
[//]: # ( you may not use this file except in compliance with the License.                 )
[//]: # ( You may obtain a copy of the License at                                          )
[//]: # (                                                                                  )
[//]: # (    http://www.apache.org/licenses/LICENSE-2.0                                    )
[//]: # (                                                                                  )
[//]: # ( Unless required by applicable law or agreed to in writing, software              )
[//]: # ( distributed under the License is distributed on an "AS IS" BASIS,                )
[//]: # ( WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.         )
[//]: # ( See the License for the specific language governing permissions and              )
[//]: # ( limitations under the License.                                                   )
[//]: # (                                                                                  )

# Performant Redshift Data Source for Apache Spark - Community Edition

[![Build Status](https://travis-ci.org/spark-redshift-community/spark-redshift.svg?branch=master)](https://travis-ci.com/spark-redshift-community/spark-redshift)
[![codecov.io](http://codecov.io/github/spark-redshift-community/spark-redshift/coverage.svg?branch=master)](http://codecov.io/github/spark-redshift-community/spark-redshift?branch=master)

Welcome to the community edition of spark-redshift! 
 The community's feedback and contributions are vitally important. 
 Pull requests are very welcome.

This repository is a fork of [databricks/spark-redshift](https://github.com/databricks/spark-redshift) which aims to provide a performant, secure, and user-friendly Apache Spark connector for Amazon Redshift. Our project maintainers closely monitors Apache Spark releases to ensure compatibility with the latest versions of Spark.


## Disclaimer

Our intent is to do the best job possible supporting the minimal set of features
 that the community needs. 

This is tested using open source Spark. Some tests have been temporarily disabled, and some features removed.

## How to help

Community's contributions are very welcome! Feel free to:

* Open an issue on github.
* Open a PR on github. To ensure a smooth code review process, please follow these steps:
  * Install the project pre-commit hooks: `pre-commit install`
  * If you don't already have .env configure at top level directory, copy the example using `cp .env.example .env`
  * Run unit tests: `sbt test` 
  * [Install integration test data](#install-integration-test-data)
  * Run integration tests:
    * Update your local .env file with real values to use your own cloud resources. 
    * (note about .env, we already git ignore, do not include it in your pull request)
    * run `sbt it:test`
   * Ensure commit messages are concise and descriptive.
   * Get a team member to review your code on github (if possible). This speeds up the PR approval for the admins.

## About

A library to load data into Spark SQL DataFrames from Amazon Redshift, and write them back to
Redshift tables. Amazon S3 is used to efficiently transfer data in and out of Redshift, and
JDBC is used to automatically trigger the appropriate `COPY` and `UNLOAD` commands on Redshift.

This library is more suited to ETL than interactive queries, since large amounts of data could be extracted to S3 for each query execution. If you plan to perform many queries against the same Redshift tables then we recommend saving the extracted data in a format such as Parquet.

- [Installation](#installation)
  - [Release builds](#release-builds)
  - [Snapshot builds](#snapshot-builds)
- [Usage](#usage)
  - [Data Sources API](#data-sources-api)
    - [Scala](#scala)
    - [Python](#python)
    - [SQL](#sql)
    - [R](#r)
  - [Hadoop InputFormat](#hadoop-inputformat)
- [Configuration](#configuration)
  - [Authenticating to S3 and Redshift](#authenticating-to-s3-and-redshift)
  - [Encryption](#encryption)
  - [Parameters](#parameters)
- [Additional configuration options](#additional-configuration-options)
    - [Configuring the maximum size of string columns](#configuring-the-maximum-size-of-string-columns)
    - [Setting a custom column type](#setting-a-custom-column-type)
    - [Configuring column encoding](#configuring-column-encoding)
    - [Setting descriptions on columns](#setting-descriptions-on-columns)
- [Auto Pushdown](#autopushdown)
- [Transactional Guarantees](#transactional-guarantees)
- [Common problems and solutions](#common-problems-and-solutions)
    - [S3 bucket and Redshift cluster are in different AWS regions](#s3-bucket-and-redshift-cluster-are-in-different-aws-regions)
    - [Warnings Associated with EC2MetadataUtils](#warnings-associated-with-ec2metadatautils)
- [Migration Guide](#migration-guide)

## Installation

This library requires Apache Spark 2.0+ and Amazon Redshift 1.0.963+.

For library versions that work with Spark 1.x, please check the [1.x branch](https://github.com/databricks/spark-redshift/tree/branch-1.x).

Currently, only master-SNAPSHOT is supported.

NOTE: In the examples below, `2.12` is the Scala version. If you are using a different version, be sure to update these values accordingly.

### Release builds
You may use this library in your applications with the following dependency information:

- **Without build tool**:
    ```bash
    spark-submit \
      --deploy-mode cluster \
      --master yarn \
      --packages com.amazon.redshift:redshift-jdbc42:2.1.0.29,org.apache.spark:spark-avro_2.12:3.5.1,io.github.spark-redshift-community:spark-redshift_2.12:6.3.0-spark_3.5 \
      my_script.py
    ```


- **In Maven**:

    ```XML
    <dependency>
        <groupId>io.github.spark-redshift-community</groupId>
        <artifactId>spark-redshift_2.12</artifactId>
        <version>6.3.0-spark_3.5</version>
    </dependency>
    ```

- **In SBT**:

    ```SBT
    libraryDependencies += "io.github.spark-redshift-community" %% "spark-redshift_2.12" % "6.3.0-spark_3.5"
    ```


You will also need to provide a JDBC driver that is compatible with Redshift. Amazon recommends that you use [the latest official Amazon Redshift JDBC driver](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42/), which is available on Maven Central. Additionally, is hosted in S3 and can be found in the [official AWS documentation for the Redshift JDBC Driver](https://docs.aws.amazon.com/redshift/latest/mgmt/jdbc20-install.html).

**Note on Hadoop versions**: This library depends on [`spark-avro`](https://github.com/databricks/spark-avro), which should automatically be downloaded because it is declared as a dependency. However, you may need to provide the corresponding `avro-mapred` dependency which matches your Hadoop distribution. In most deployments, however, this dependency will be automatically provided by your cluster's Spark assemblies and no additional action will be required.

**Note on dependencies**: This library declares a `provided` dependency on multiple libraries, such as the AWS SDK. This means they must be provided in your development environment. In many cases, these libraries will be provided by your deployment environment. However, if you encounter a `ClassNotFoundException` then you will need to add explicit dependencies for these libraries. For a complete list of provided dependencies please see the project's `build.sbt` file.

### Install Integration Test Data
The integration tests tagged with `PreloadTest` require prerequisite data loaded into Redshift prior to running. The following procedure can be used to prepopulate this test data.
1. Locate the folder `src/it/resources/PreloadTestData` for the below operations.
2. Copy all the files with the prefix `all_shapes_data...csv` to a temporary S3 bucket.
3. Within the file `setup_all_shapes_default_null.sql`, modify the four `copy` instructions on lines 160, 322, 484, and 646 to point to the temporary S3 bucket folder, S3 bucket region, and IAM role associated with the Redshift cluster that has the necessary permissions to read from the S3 bucket. 
4. Execute the file `setup_all_shapes_default_null.sql` using a SQL command tool (e.g., Redshift query editor or PSQL).
5. Remove the temporary S3 bucket.


## Usage

### Data Sources API

Once you have [configured your AWS credentials](#authenticating-to-s3-and-redshift), you can use this library via the Data Sources API in Scala, Python or SQL, as follows:

#### Scala

```scala
import org.apache.spark.sql._

val sc = // existing SparkContext
val sqlContext = new SQLContext(sc)

// Get some data from a Redshift table
val df: DataFrame = sqlContext.read
    .format("io.github.spark_redshift_community.spark.redshift")
    .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass")
    .option("dbtable", "my_table")
    .option("tempdir", "s3n://path/for/temp/data")
    .load()

// Can also load data from a Redshift query
val df: DataFrame = sqlContext.read
    .format("io.github.spark_redshift_community.spark.redshift")
    .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass")
    .option("query", "select x, count(*) my_table group by x")
    .option("tempdir", "s3n://path/for/temp/data")
    .load()

// Apply some transformations to the data as per normal, then you can use the
// Data Source API to write the data back to another table

df.write
  .format("io.github.spark_redshift_community.spark.redshift")
  .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass")
  .option("dbtable", "my_table_copy")
  .option("tempdir", "s3n://path/for/temp/data")
  .mode("error")
  .save()

// Using IAM Role based authentication
df.write
  .format("io.github.spark_redshift_community.spark.redshift")
  .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass")
  .option("dbtable", "my_table_copy")
  .option("aws_iam_role", "arn:aws:iam::123456789000:role/redshift_iam_role")
  .option("tempdir", "s3n://path/for/temp/data")
  .mode("error")
  .save()
```

#### Python

```python
from pyspark.sql import SQLContext

sc = # existing SparkContext
sql_context = SQLContext(sc)

# Read data from a table
df = sql_context.read \
    .format("io.github.spark_redshift_community.spark.redshift") \
    .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass") \
    .option("dbtable", "my_table") \
    .option("tempdir", "s3n://path/for/temp/data") \
    .load()

# Read data from a query
df = sql_context.read \
    .format("io.github.spark_redshift_community.spark.redshift") \
    .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass") \
    .option("query", "select x, count(*) my_table group by x") \
    .option("tempdir", "s3n://path/for/temp/data") \
    .load()

# Write back to a table
df.write \
  .format("io.github.spark_redshift_community.spark.redshift") \
  .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass") \
  .option("dbtable", "my_table_copy") \
  .option("tempdir", "s3n://path/for/temp/data") \
  .mode("error") \
  .save()

# Using IAM Role based authentication
df.write \
  .format("io.github.spark_redshift_community.spark.redshift") \
  .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass") \
  .option("dbtable", "my_table_copy") \
  .option("tempdir", "s3n://path/for/temp/data") \
  .option("aws_iam_role", "arn:aws:iam::123456789000:role/redshift_iam_role") \
  .mode("error") \
  .save()
```

#### SQL

Reading data using SQL:

```sql
CREATE TABLE my_table
USING io.github.spark_redshift_community.spark.redshift
OPTIONS (
  dbtable 'my_table',
  tempdir 's3n://path/for/temp/data',
  url 'jdbc:redshift://redshifthost:5439/database?user=username&password=pass'
);
```

Writing data using SQL:

```sql
-- Create a new table, throwing an error if a table with the same name already exists:
CREATE TABLE my_table
USING io.github.spark_redshift_community.spark.redshift
OPTIONS (
  dbtable 'my_table',
  tempdir 's3n://path/for/temp/data'
  url 'jdbc:redshift://redshifthost:5439/database?user=username&password=pass'
)
AS SELECT * FROM tabletosave;
```

Note that the SQL API only supports the creation of new tables and not overwriting or appending; this corresponds to the default save mode of the other language APIs.

#### R

Reading data using R:

```R
df <- read.df(
   NULL,
   "io.github.spark_redshift_community.spark.redshift",
   tempdir = "s3n://path/for/temp/data",
   dbtable = "my_table",
   url = "jdbc:redshift://redshifthost:5439/database?user=username&password=pass")
```

### Hadoop InputFormat

The library contains a Hadoop input format for Redshift tables unloaded with the ESCAPE option,
which you may make direct use of as follows:

```scala
import io.github.spark_redshift_community.spark.redshift.RedshiftInputFormat

val records = sc.newAPIHadoopFile(
  path,
  classOf[RedshiftInputFormat],
  classOf[java.lang.Long],
  classOf[Array[String]])
```

## Configuration

### Authenticating to S3 and Redshift

The use of this library involves several connections which must be authenticated / secured, all of
which are illustrated in the following diagram:

```
                            ┌───────┐
                            │Amazon │
       ┌───────────────────▶│  S3   │◀─────────────────┐
       │  IAM role or keys  └───────┘ IAM role or keys │
       │                        ▲                      │
       │                        │ IAM role or keys     │
       ▼                        ▼               ┌──────▼────┐
┌────────────┐            ┌───────────┐         │┌──────────┴┐
│   Amazon   │            │   Spark   │         ││   Spark   │
│  Redshift  │◀──────────▶│   Driver  │◀────────▶┤ Executors │
└────────────┘            └───────────┘          └───────────┘
               JDBC with                  Configured
              Database or                     in
              IAM Credentials                Spark
          (SSL enabled by default)
```

This library reads and writes data to S3 when transferring data to/from Redshift. As a result, it
requires AWS IAM role or keys granting read and write access to a S3 bucket (specified using the `tempdir`
configuration parameter). Please refer to the official AWS documentation for instructions on 
[Using IAM to retrieve credentials and connect to Amazon Redshift](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-redshift-auth.html).

> **:warning: Note**: This library does not clean up the temporary files that it creates in S3.
> As a result, we recommend that you use a dedicated temporary S3 bucket with an
> [object lifecycle configuration](http://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html)
> to ensure that temporary files are automatically deleted after a specified expiration period.
> See the [_Encryption_](#encryption) section of this document for a discussion of how these files
> may be encrypted.

The following describes how each connection can be authenticated:

- **Spark driver to Redshift**: The Spark driver connects to Redshift via the official Amazon Redshift JDBC driver
    using IAM, Identity Provider, AWS Secrets Manager or database username and password.
    Using IAM authentication or AWS Secrets Manager is recommended; for more details, see the official AWS documentation for 
    [Configuring JDBC authentication and SSL](https://docs.aws.amazon.com/redshift/latest/mgmt/jdbc20-configure-authentication-ssl.html). 
    Securing the connection by enabling SSL is recommended; for more details, see the Encryption section below.

- **Spark to S3**: S3 acts as a middleman to store bulk data when reading from or writing to Redshift.
    Spark connects to S3 using both the Hadoop FileSystem interfaces and directly using the Amazon
    Java SDK's S3 client.

    This connection can be authenticated using either AWS keys or IAM roles (DBFS mountpoints are
    not currently supported, so Databricks users who do not want to rely on AWS keys should use
    cluster IAM roles instead).

    There are multiple ways of providing these credentials:

    1. **Default Credential Provider Chain (best option for most users):**
        AWS credentials will automatically be retrieved through the [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#id6).

        If you use IAM instance roles to authenticate to S3 (e.g. on Databricks, EMR, or EC2), then
        you should probably use this method.

        If another method of providing credentials is used (methods 2 or 3), then that will take
        precedence over this default.

    2. **Set keys in Hadoop conf:** You can specify AWS keys via
        [Hadoop configuration properties](https://github.com/apache/hadoop/blob/trunk/hadoop-tools/hadoop-aws/src/site/markdown/tools/hadoop-aws/index.md).
        For example, if your `tempdir` configuration points to a `s3n://` filesystem then you can
        set the `fs.s3n.awsAccessKeyId` and `fs.s3n.awsSecretAccessKey` properties in a Hadoop XML
        configuration file or call `sc.hadoopConfiguration.set()` to mutate Spark's global Hadoop
        configuration.

        For example, if you are using the `s3n` filesystem then add

        ```scala
        sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "YOUR_KEY_ID")
        sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "YOUR_SECRET_ACCESS_KEY")
        ```

        and for the `s3a` filesystem add

        ```scala
        sc.hadoopConfiguration.set("fs.s3a.access.key", "YOUR_KEY_ID")
        sc.hadoopConfiguration.set("fs.s3a.secret.key", "YOUR_SECRET_ACCESS_KEY")
        ```

        Python users will have to use a slightly different method to modify the `hadoopConfiguration`,
        since this field is not exposed in all versions of PySpark. Although the following command
        relies on some Spark internals, it should work with all PySpark versions and is unlikely to
        break or change in the future:

        ```python
        sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "YOUR_KEY_ID")
        sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "YOUR_SECRET_ACCESS_KEY")
        ```

- **Redshift to S3**: Redshift also connects to S3 during `COPY` and `UNLOAD` queries. There are
    three methods of authenticating this connection:

    1. **Have Redshift assume an IAM role (most secure)**: You can grant Redshift permission to assume
        an IAM role during `COPY` or `UNLOAD` operations and then configure this library to instruct
        Redshift to use that role:

        1. Create an IAM role granting appropriate S3 permissions to your bucket.
        2. Follow the guide
        [_Authorizing Amazon Redshift to Access Other AWS Services On Your Behalf_](http://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html)
        to configure this role's trust policy in order to allow Redshift to assume this role.
        3. Follow the steps in the
        [_Authorizing COPY and UNLOAD Operations Using IAM Roles_](http://docs.aws.amazon.com/redshift/latest/mgmt/copy-unload-iam-role.html)
        guide to associate that IAM role with your Redshift cluster.
        4. Set this library's `aws_iam_role` option to the role's ARN.
    2. **Forward Spark's S3 credentials to Redshift**: if the `forward_spark_s3_credentials` option is
        set to `true` then this library will automatically discover the credentials that Spark is
        using to connect to S3 and will forward those credentials to Redshift over JDBC. If Spark
        is authenticating to S3 using an IAM instance role then a set of temporary STS credentials
        will be passed to Redshift; otherwise, AWS keys will be passed. These credentials are
        sent as part of the JDBC query, therefore it is **strongly recommended** to enable SSL
        encryption of the JDBC connection when using this authentication method.
    3. **Use Security Token Service (STS) credentials**: You may configure the
        `temporary_aws_access_key_id`, `temporary_aws_secret_access_key`, and
        `temporary_aws_session_token` configuration properties to point to temporary keys created
        via the AWS
        [Security Token Service](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp.html).
        These credentials are sent as part of the JDBC query, therefore it is
        **strongly recommended** to enable SSL encryption of the JDBC connection when using this
        authentication method.
        If you choose this option then please be aware of the risk that the credentials expire before
        the read / write operation succeeds.

    These three options are mutually-exclusive, and you must explicitly choose which one to use.


### Encryption

- **Securing JDBC**: The Redshift and Postgres JDBC drivers both support SSL. To enable SSL support,
    first configure Java to add the required certificates by following the
    [_Using SSL and Server Certificates in Java_](http://docs.aws.amazon.com/redshift/latest/mgmt/connecting-ssl-support.html#connecting-ssl-support-java)
    instructions in the Redshift documentation. Then, follow the instructions in
    [_JDBC Driver Configuration Options_](http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-options.html) to add the appropriate SSL options
    to the JDBC `url` used with this library.

- **Encrypting `UNLOAD` data stored in S3 (data stored when reading from Redshift)**: According to the Redshift documentation
    on [_Unloading Data to S3_](http://docs.aws.amazon.com/redshift/latest/dg/t_Unloading_tables.html),
    "UNLOAD automatically encrypts data files using Amazon S3 server-side encryption (SSE-S3)."

    Redshift also supports client-side encryption with a custom key
    (see: [_Unloading Encrypted Data Files_](http://docs.aws.amazon.com/redshift/latest/dg/t_unloading_encrypted_files.html))
    but this library currently lacks the capability to specify the required symmetric key.

- **Encrypting `COPY` data stored in S3 (data stored when writing to Redshift)**:
    According to the Redshift documentation on
    [_Loading Encrypted Data Files from Amazon S3_](http://docs.aws.amazon.com/redshift/latest/dg/c_loading-encrypted-files.html):

    > You can use the COPY command to load data files that were uploaded to Amazon S3 using
    > server-side encryption with AWS-managed encryption keys (SSE-S3 or SSE-KMS), client-side
    > encryption, or both. COPY does not support Amazon S3 server-side encryption with a customer-supplied key (SSE-C)

    To use this capability, you should configure your Hadoop S3 FileSystem to use encryption by
    setting the appropriate configuration properties (which will vary depending on whether you
    are using `s3a`, `s3n`, EMRFS, etc.).
    Note that the `MANIFEST` file (a list of all files written) will not be encrypted.


### Parameters

The parameter map or <tt>OPTIONS</tt> provided in Spark SQL supports the following settings.

<table>
 <tr>
    <th>Parameter</th>
    <th>Required</th>
    <th>Default</th>
    <th>Notes</th>
 </tr>

 <tr>
    <td><tt>dbtable</tt></td>
    <td>Yes, unless <tt>query</tt> is specified</td>
    <td>No default</td>
    <td>The table to create or read from in Redshift. This parameter is required when saving data back to Redshift.</td>
 </tr>
 <tr>
    <td><tt>query</tt></td>
    <td>Yes, unless <tt>dbtable</tt> is specified</td>
    <td>No default</td>
    <td>The query to read from in Redshift</td>
 </tr>
 <tr>
    <td><tt>user</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>The Redshift username.  Must be used in tandem with <tt>password</tt> option.  May only be used if the user and password are not passed in the URL, passing both will result in an error.</td>
 </tr>
 <tr>
    <td><tt>password</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>The Redshift password.  Must be used in tandem with <tt>user</tt> option.  May only be used if the user and password are not passed in the URL; passing both will result in an error.</td>
 </tr>
<tr> 
    <td><tt>secret.id</tt></td>
    <td>No</td>
    <td>No default</td>
    <td> The Name or ARN of your secret stored in AWS Secrets Manager. May be used to automatically supply Redshift credentials but only if the user, password and DbUser are not passed in the URL or as options.</td>
</tr>
<tr> 
    <td><tt>secret.region</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>
       <p>The primary AWS region (e.g., <tt>us-east-1</tt>) for searching for the <tt>secret.id</tt> value. </p>
       <p>If the region is not specified, the connector will attempt to use the <a href="https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html">Default Credential Provider Chain</a> for resolving where the <tt>secret.id</tt> region is located. In some cases, such as when the connector is being used outside of an AWS environment, this resolution will fail. Therefore, this setting is highly recommended in the following situations:</p>
       <ol>
          <li>When the connector is running outside of AWS as automatic region discovery will fail and may prevent authenticating with Redshift.</li>
          <li>When the connector is running in a different region than <tt>secret.id</tt> as it improves the connector's access performance of the secret.</li>
       </ol>
    </td>
</tr>
 <tr>
    <td><tt>url</tt></td>
    <td>Yes</td>
    <td>No default</td>
    <td>
<p>A JDBC URL, of the format, <tt>jdbc:redshift://host:port/database?user=username&password=password</tt></p>

<ul>
 <li>If <tt>host</tt> and <tt>port</tt> do not point to the Redshift master node, security groups and/or VPC will
need to be configured to allow access from your driver application.
 <li><tt>database</tt> identifies a Redshift database name</li>
 <li><tt>user</tt> and <tt>password</tt> are credentials to access the database, which can be embedded
    in this URL for JDBC. Using IAM authentication is the recommended authentication mechanism. Your user account should have necessary privileges for the table being referenced. </li>
    </td>
 </tr>
 <tr>
   <td><tt>aws_iam_role</tt></td>
   <td>Only if using IAM roles to authorize Redshift COPY/UNLOAD operations</td>
   <td>No default</td>
   <td>Fully specified ARN of the <a href="http://docs.aws.amazon.com/redshift/latest/mgmt/copy-unload-iam-role.html">IAM Role</a> attached to the Redshift cluster, ex: arn:aws:iam::123456789000:role/redshift_iam_role</td>
 </tr>
  <tr>
    <td><tt>forward_spark_s3_credentials</tt></td>
    <td>No</td>
    <td>false</td>
    <td>
        If <tt>true</tt> then this library will automatically discover the credentials that Spark is
        using to connect to S3 and will forward those credentials to Redshift over JDBC.
        These credentials are sent as part of the JDBC query, so therefore it is strongly
        recommended to enable SSL encryption of the JDBC connection when using this option.
    </td>
  </tr>
 <tr>
    <td><tt>temporary_aws_access_key_id</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>AWS access key, must have write permissions to the S3 bucket.</td>
 </tr>
 <tr>
    <td><tt>temporary_aws_secret_access_key</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>AWS secret access key corresponding to provided access key.</td>
 </tr>
 <tr>
    <td><tt>temporary_aws_session_token</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>AWS session token corresponding to provided access key.</td>
 </tr>
 <tr>
    <td><tt>tempdir</tt></td>
    <td>Yes</td>
    <td>No default</td>
    <td>A writeable location in Amazon S3, to be used for unloaded data when reading and Avro data to be loaded into
Redshift when writing. If you're using Redshift data source for Spark as part of a regular ETL pipeline, it can be useful to
set a <a href="http://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html">Lifecycle Policy</a> on a bucket
and use that as a temp location for this data.
    </td>
 </tr>
 <tr>
    <td><tt>tempdir_region</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>
       <p>AWS region where <tt>tempdir</tt> is located. Setting this option will improve connector performance for interactions with <tt>tempdir</tt> as well as automatically supply this value as part of COPY and UNLOAD operations during connector writes and reads. If the region is not specified, the connector will attempt to use the <a href="https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html">Default Credential Provider Chain</a> for resolving where the <tt>tempdir</tt> region is located. In some cases, such as when the connector is being used outside of an AWS environment, this resolution will fail. Therefore, this setting is highly recommended in the following situations:</p>
       <ol>
          <li>When the connector is running outside of AWS as automatic region discovery will fail and negatively affect connector performance.</li>
          <li>When <tt>tempdir</tt> is in a different region than the Redshift cluster as using this setting alleviates the need to supply the region manually using the <tt>extracopyoptions</tt> and <tt>extraunloadoptions</tt> parameters.</li>
          <li>When the connector is running in a different region than <tt>tempdir</tt> as it improves the connector's access performance of <tt>tempdir</tt>.</li>
       </ol>
    </td>
 </tr>
 <tr>
    <td><tt>jdbcdriver</tt></td>
    <td>No</td>
    <td>Determined by the JDBC URL's subprotocol</td>
    <td>The class name of the JDBC driver to use. This class must be on the classpath. In most cases, it should not be necessary to specify this option, as the appropriate driver classname should automatically be determined by the JDBC URL's subprotocol.</td>
 </tr>
 <tr>
    <td><tt>diststyle</tt></td>
    <td>No</td>
    <td><tt>EVEN</tt></td>
    <td>The Redshift <a href="http://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_sort.html">Distribution Style</a> to
be used when creating a table. Can be one of <tt>EVEN</tt>, <tt>KEY</tt> or <tt>ALL</tt> (see Redshift docs). When using <tt>KEY</tt>, you
must also set a distribution key with the <tt>distkey</tt> option.
    </td>
 </tr>
 <tr>
    <td><tt>distkey</tt></td>
    <td>No, unless using <tt>DISTSTYLE KEY</tt></td>
    <td>No default</td>
    <td>The name of a column in the table to use as the distribution key when creating a table.</td>
 </tr>
 <tr>
    <td><tt>sortkeyspec</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>
<p>A full Redshift <a href="http://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html">Sort Key</a> definition.</p>

<p>Examples include:</p>
<ul>
    <li><tt>SORTKEY(my_sort_column)</tt></li>
    <li><tt>COMPOUND SORTKEY(sort_col_1, sort_col_2)</tt></li>
    <li><tt>INTERLEAVED SORTKEY(sort_col_1, sort_col_2)</tt></li>
</ul>
    </td>
 </tr>
 <tr>
    <td><del><tt>usestagingtable</tt></del> (Deprecated)</td>
    <td>No</td>
    <td><tt>true</tt></td>
    <td>
    <p>
    Setting this deprecated option to <tt>false</tt> will cause an overwrite operation's destination table to be dropped immediately at the beginning of the write, making the overwrite operation non-atomic and reducing the availability of the destination table. This may reduce the temporary disk space requirements for overwrites.
    </p>

    <p>Since setting <tt>usestagingtable=false</tt> operation risks data loss / unavailability, we have chosen to deprecate it in favor of requiring users to manually drop the destination table themselves.</p>
    </td>
 </tr>
  <tr>
    <td><tt>include_column_list</tt></td>
    <td>No</td>
    <td>false</td>
    <td>
        If <tt>true</tt> then this library will automatically extract the columns from the schema
        and add them to the COPY command according to the <a href="http://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-column-mapping.html">Column List docs</a>.
        (e.g. `COPY "PUBLIC"."tablename" ("column1" [,"column2", ...])`).
    </td>
  </tr>
 <tr>
    <td><tt>description</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>
<p>A description for the table. Will be set using the SQL COMMENT command, and should show up in most query tools.
See also the <tt>description</tt> metadata to set descriptions on individual columns.
 </tr>
 <tr>
    <td><tt>preactions</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>
<p>This can be a <tt>;</tt> separated list of SQL commands to be executed before loading <tt>COPY</tt> command.
It may be useful to have some <tt>DELETE</tt> commands or similar run here before loading new data. If the command contains
<tt>%s</tt>, the table name will be formatted in before execution (in case you're using a staging table).</p>

<p>Be warned that if this commands fail, it is treated as an error and you'll get an exception. If using a staging
table, the changes will be reverted and the backup table restored if pre actions fail.</p>
    </td>
 </tr>
 <tr>
    <td><tt>postactions</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>
<p>This can be a <tt>;</tt> separated list of SQL commands to be executed after a successful <tt>COPY</tt> when loading data.
It may be useful to have some <tt>GRANT</tt> commands or similar run here when loading new data. If the command contains
<tt>%s</tt>, the table name will be formatted in before execution (in case you're using a staging table).</p>

<p>Be warned that if this commands fail, it is treated as an error and you'll get an exception. If using a staging
table, the changes will be reverted and the backup table restored if post actions fail.</p>
    </td>
 </tr>
 <tr>
    <td><tt>extracopyoptions</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>
<p>A list extra options to append to the Redshift <tt>COPY</tt> command when loading data, e.g. <tt>TRUNCATECOLUMNS</tt>
or <TT>MAXERROR n</tt> (see the <a href="http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html#r_COPY-syntax-overview-optional-parameters">Redshift docs</a>
for other options).</p>

<p>Note that since these options are appended to the end of the <tt>COPY</tt> command, only options that make sense
at the end of the command can be used, but that should cover most possible use cases.</p>
    </td>
 </tr>
 <tr>
    <td><tt>sse_kms_key</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>
<p>The KMS key ID to use for server-side encryption in S3 during the Redshift <tt>UNLOAD</tt> operation rather than AWS's default
encryption. The Redshift IAM role must have access to the KMS key for writing with it, and the Spark IAM role must have access
to the key for read operations. Reading the encrypted data requires no changes (AWS handles this under-the-hood) so long as
Spark's IAM role has the proper access.</p> 
<p>See the <a href="https://docs.aws.amazon.com/redshift/latest/dg/t_unloading_encrypted_files.html">Redshift docs</a>
for more information.</p>
    </td>
 </tr>
 <tr>
    <td><tt>unload_s3_format</tt></td>
    <td>No</td>
    <td>PARQUET</td>
    <td>
        <p>
        PARQUET - unload the query result to parquet file format.
        </p>
        <p>
        TEXT - unload the query result to pipe-delimited text file format.
        </p>
    </td>
 </tr> 
 <tr>
    <td><tt>tempformat</tt></td>
    <td>No</td>
    <td><tt>AVRO</tt></td>
    <td>
    <p>
        The format in which to save temporary files in S3 when writing to Redshift.
        Defaults to "AVRO"; the other allowed values are "CSV", "CSV GZIP", and "PARQUET" for CSV,
        gzipped CSV, and parquet, respectively.
    </p>
    <p>
        Redshift is significantly faster when loading CSV than when loading Avro files, so
        using that <tt>tempformat</tt> may provide a large performance boost when writing
        to Redshift.
    </p>
    <p>
        Parquet should not be used as the tempformat when using an S3 bucket (tempdir) in a region that is different
        from the region where the redshift cluster you are writing to resides. This is because cross-region copies are
        not supported in redshift when using parquet as the format.
    </p>
    </td>
 </tr>
 <tr>
    <td><tt>csvnullstring</tt></td>
    <td>No</td>
    <td><tt>@NULL@</tt></td>
    <td>
    <p>
        The String value to write for nulls when using the CSV <tt>tempformat</tt>.
        This should be a value which does not appear in your actual data.
    </p>
    </td>
 </tr>
 <tr>
    <td><tt>autopushdown</tt></td>
    <td>No</td>
    <td>True</td>
    <td>
        <p>
                Apply predicate and query pushdown by capturing and analyzing the Spark logical plans for SQL operations. 
                The operations are translated into a SQL query and then executed in Redshift to improve performance.
        </p>
        <p>
                Once autopushdown is enabled, it is enabled for all the Redshift tables in the same Spark session.
        </p>
    </td>
 </tr>
 <tr>
    <td><tt>autopushdown.s3_result_cache</tt></td>
    <td>No</td>
    <td>False</td>
    <td>Cache the query SQL to unload data S3 path mapping in memory so that the same query don't need to execute again in the same Spark session.</td>
 </tr>
<tr>
    <td><tt>extraunloadoptions</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>
        Extra options to append to the Redshift UNLOAD command (e.g. ENCRYPTED).
        A complete list of possible options may be seen in the <a href="https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html">Redshift docs</a> though not all are guaranteed to work and may conflict with other options set in the connector resulting in exceptions.
    </td>
</tr>
<tr>
    <td><tt>copyretrycount</tt></td>
    <td>No</td>
    <td>2</td>
    <td>Number of times to retry a copy operation including dropping and creating any required table before failing.</td>
</tr>
<tr>
    <td><tt>copydelay</tt></td>
    <td>No</td>
    <td>30000</td>
    <td>Number of milliseconds to wait between retrying copy operations. Non-positive values will be treated as 30 seconds.</td>
</tr>
<tr> 
    <td><tt>secret.vpcEndpointUrl</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>The PrivateLink DNS endpoint URL for AWS Secrets Manager when overriding the <a href="https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html">Default Credential Provider Chain</a> </td>
</tr>
<tr> 
    <td><tt>secret.vpcEndpointRegion</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>The PrivateLink DNS endpoint Region for AWS Secrets Manager when overriding the <a href="https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html">Default Credential Provider Chain</a> </td>
</tr>
<tr>
    <td><tt>jdbc.*</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>
        Additional parameters to pass to the underlying JDBC driver where the wildcard is the name of the JDBC parameter (e.g., <tt>jdbc.ssl</tt>). Note that the <tt>jdbc</tt> prefix will be stripped off before passing to the JDBC driver.
        A complete list of possible options for the Redshift JDBC driver may be seen in the <a href="https://docs.aws.amazon.com/redshift/latest/mgmt/jdbc20-configuration-options.html">Redshift docs</a>.
    </td>
</tr>
<tr>
    <td><tt>label</tt></td>
    <td>No</td>
    <td>""</td>
    <td>
        An identifier to include in the query group set when running queries with the connector. Should be 100 or fewer characters and all characters must be valid unicodeIdentifierParts. Characters in excess of 100 will be trimmed.
        When running a query with the connector a json formatted string will be set as the query group (for example `{"spark-redshift-connector":{"svc":"","ver":"6.3.0-spark_3.5","op":"Read","lbl":"","tid":""}}`). 
        This option will be substituted for the value of the `lbl` key.
    </td>
</tr></table>

## Additional configuration options

### Configuring the maximum size of string columns

When creating Redshift tables, this library's default behavior is to create `VARCHAR(MAX)` columns for string columns. These columns have a maximum size of 65535 characters ([source](http://docs.aws.amazon.com/redshift/latest/dg/r_Character_types.html)).

To support smaller columns, you can use the `maxlength` column metadata field to specify the maximum length of individual string columns. This can be done as a space-savings performance optimization in order to declare columns with a smaller maximum length than the default.

> **:warning: Note**: Due to limitations in Spark, metadata modification is unsupported in the Python, SQL, and R language APIs.

Here is an example of updating multiple columns' metadata fields using Spark's Scala API:

```scala
import org.apache.spark.sql.types.MetadataBuilder

// Specify the custom width of each column
val columnLengthMap = Map(
  "language_code" -> 2,
  "country_code" -> 2,
  "url" -> 2083
)

var df = ... // the dataframe you'll want to write to Redshift

// Apply each column metadata customization
columnLengthMap.foreach { case (colName, length) =>
  val metadata = new MetadataBuilder().putLong("maxlength", length).build()
  df = df.withColumn(colName, df(colName).as(colName, metadata))
}

df.write
  .format("io.github.spark_redshift_community.spark.redshift")
  .option("url", jdbcURL)
  .option("tempdir", s3TempDirectory)
  .option("dbtable", sessionTable)
  .save()
```

### Setting a custom column type

If you need to manually set a column type, you can use the `redshift_type` column metadata. For example, if you desire to override
the `Spark SQL Schema -> Redshift SQL` type matcher to assign a user-defined column type, you can do the following:

```scala
import org.apache.spark.sql.types.MetadataBuilder

// Specify the custom width of each column
val columnTypeMap = Map(
  "language_code" -> "CHAR(2)",
  "country_code" -> "CHAR(2)",
  "url" -> "BPCHAR(111)"
)

var df = ... // the dataframe you'll want to write to Redshift

// Apply each column metadata customization
columnTypeMap.foreach { case (colName, colType) =>
  val metadata = new MetadataBuilder().putString("redshift_type", colType).build()
  df = df.withColumn(colName, df(colName).as(colName, metadata))
}
```

### Configuring column encoding

When creating a table, this library can be configured to use a specific compression encoding on individual columns. You can use the `encoding` column metadata field to specify a compression encoding for each column (see [Amazon docs](http://docs.aws.amazon.com/redshift/latest/dg/c_Compression_encodings.html) for available encodings).

### Setting descriptions on columns

Redshift allows columns to have descriptions attached that should show up in most query tools (using the `COMMENT` command). You can set the `description` column metadata field to specify a description for individual columns.

## Auto Pushdown

The Spark Connector applies predicate and query pushdown by capturing and analyzing the Spark logical plans for SQL operations. When the data source is Redshift, the operations are translated into a SQL query and then executed in Redshift to improve performance.

Not all of Spark SQL operators can be pushed down. When pushdown fails, the connector falls back to a less-optimized execution plan. The unsupported operations are instead performed in Spark.

Auto pushdown is enabled by default. To disable it, set the "autopushdown" to false as below:

```scala
import sqlContext.implicits._
val eventsDF = sqlContext.read
  .format("io.github.spark_redshift_community.spark.redshift")
  .option("url",jdbcURL )
  .option("tempdir", tempS3Dir)
  .option("dbtable", "event")
    .option("autopushdown", "false")
  .load()
eventsDF.show()
```

## Spark SQL Configurations

### lazyMode
The Spark Connector pushdown redshift query in lazy mode. This allows spark's adaptive query execution mode to further optimize the query. This behavior is enabled by default.
To disable it, run following command:

```sparksql
SET spark.datasource.redshift.community.autopushdown.lazyMode=false
```

### trace_id
A new tracing identifier field that is added to the existing `label` parameter. When set, the provided string value will be used as part of label. Otherwise, it will default to the Spark application identifier. For example:

`{"spark-redshift-connector":{"svc":"","ver":"6.3.0-spark_3.5","op":"Read","lbl":"","tid":"..."}}`)

To set the value, run the following command:
```sparksql
SET spark.datasource.redshift.community.trace_id="String up to 75 characters in length."
```

## Unload Format

By default, query results are unloaded to S3 as Parquet files. There is an option as shown below to unload the result in pipe-delimited text format instead. However, according to Redshift database developer guide, Parquet format is up to 2x faster to unload and consumes up to 6x less storage in Amazon S3, compared with text formats.

```scala
    .option("unload_s3_format", "TEXT")
```

Refer to integration test cases for supported operations for pushdown.

## Schema for redshift super columns of complex types (Struct, Map, Array)

By default, query results which include a super column will provide the super column as a string.
However, if the schema of the super column is known ahead of time it can be provided as part of the read
and the column will be returned as the provided schema type. This will also enable the pushdown of operations
such as getting a struct field, getting a map value by key, or getting the item at an array index.
Retrieving maps which use key types other than StringType is not supported.
Retrieving nested struct field names or map keys which contain upper case letters will return incorrect results, so the
schema should not be provided to the connector unless all struct field names and map keys are lower case. In a case
where upper case field names or map keys must be retrieved the schema can be used with the `from_json` function to
convert the returned string data from a super into the correct struct, map, or array type.

In the case of a table with a super containing a struct created like:

```sql
create table contains_super (a super);
```

The connector can be used to query a StringType field `hello` from the super column `a` in the table using a schema like:

```scala
import org.apache.spark.sql.types._

val sc = // existing SparkContext
val sqlContext = new SQLContext(sc)

val schema = StructType(StructField("a", StructType(StructField("hello", StringType) ::Nil)) :: Nil)

val helloDF = sqlContext.read
  .format("io.github.spark_redshift_community.spark.redshift")
  .option("url", jdbcURL )
  .option("tempdir", tempS3Dir)
  .option("dbtable", "contains_super")
  .schema(schema)
  .load().selectExpr("a.hello")
```

If the column `a` might be better represented as a map
(it only has keys corresponding to values where all the values are of the same type such as String -> Int)
then a MapType can be provided as the schema, and the value of a map's key `'hello'` can be queried like:

```scala
import org.apache.spark.sql.types._

val sc = // existing SparkContext
val sqlContext = new SQLContext(sc)

val schema = StructType(StructField("a", MapType(StringType, IntegerType))::Nil)

val helloDF = sqlContext.read
  .format("io.github.spark_redshift_community.spark.redshift")
  .option("url", jdbcURL )
  .option("tempdir", tempS3Dir)
  .option("dbtable", "contains_super")
  .schema(schema)
  .load().selectExpr("a['hello']")
```

If the column `a` contained an array instead of a struct, the connector could be used to query for the first element in that array like:

```scala
import org.apache.spark.sql.types._

val sc = // existing SparkContext
val sqlContext = new SQLContext(sc)

val schema = StructType(StructField("a", ArrayType(IntegerType)):: Nil)

val helloDF = sqlContext.read
  .format("io.github.spark_redshift_community.spark.redshift")
  .option("url", jdbcURL )
  .option("tempdir", tempS3Dir)
  .option("dbtable", "contains_super")
  .schema(schema)
  .load().selectExpr("a[0]")
```

### Writing complex types

If a schema is provided for a dataframe that includes a complex type like StructType, ArrayType, or MapType
then the connector can be used to write the field to redshift. There are few important limitations:

1. No nested fields may have names containing upper case letters
2. Any map keys must be StringType
3. No map keys may contain upper case letters
4. tempformat must be one of `CSV`, `CSV GZIP`, or `PARQUET`

Using the connector, writing a struct to a column `a` looks like:

```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql._

val sc = // existing SparkContext
val sqlContext = new SQLContext(sc)

val schema = StructType(StructField("a", StructType(StructField("hello", StringType) ::Nil)) :: Nil)
val data = sc.parallelize(Seq(Row(Row("world"))))
val mydf = sqlContext.createDataFrame(data, schema)

mydf.write.format("io.github.spark_redshift_community.spark.redshift").
  option("url", jdbcUrl).
  option("dbtable", tableName).
  option("tempdir", tempS3Dir).
  option("tempformat", "CSV").
  mode(SaveMode.Append).save
```

## Transactional Guarantees

This section describes the transactional guarantees of the Redshift data source for Spark

### General background on Redshift and S3's properties

For general information on Redshift's transactional guarantees, see the [Managing Concurrent Write Operations](https://docs.aws.amazon.com/redshift/latest/dg/c_Concurrent_writes.html) chapter in the Redshift documentation. In a nutshell, Redshift provides [serializable isolation](https://docs.aws.amazon.com/redshift/latest/dg/c_serial_isolation.html) (according to the documentation for Redshift's [`BEGIN`](https://docs.aws.amazon.com/redshift/latest/dg/r_BEGIN.html) command, "[although] you can use any of the four transaction isolation levels, Amazon Redshift processes all isolation levels as serializable"). According to its [documentation](https://docs.aws.amazon.com/redshift/latest/dg/c_serial_isolation.html), "Amazon Redshift supports a default _automatic commit_ behavior in which each separately-executed SQL command commits individually." Thus, individual commands like `COPY` and `UNLOAD` are atomic and transactional, while explicit `BEGIN` and `END` should only be necessary to enforce the atomicity of multiple commands / queries.

When reading from / writing to Redshift, this library reads and writes data in S3. Both Spark and Redshift produce partitioned output which is stored in multiple files in S3. According to the [Amazon S3 Data Consistency Model](https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel) documentation, S3 bucket listing operations are eventually-consistent, so the files must go to special lengths to avoid missing / incomplete data due to this source of eventual-consistency.

### Guarantees of the Redshift data source for Spark


**Appending to an existing table**: In the [`COPY`](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) command, this library uses [manifests](https://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html) to guard against certain eventually-consistent S3 operations. As a result, it appends to existing tables have the same atomic and transactional properties as regular Redshift `COPY` commands.

**Appending to an existing table**: When inserting rows into Redshift, this library uses the [`COPY`](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) command and specifies [manifests](https://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html) to guard against certain eventually-consistent S3 operations. As a result, `spark-redshift` appends to existing tables have the same atomic and transactional properties as regular Redshift `COPY` commands.


**Creating a new table (`SaveMode.CreateIfNotExists`)**: Creating a new table is a two-step process, consisting of a `CREATE TABLE` command followed by a [`COPY`](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) command to append the initial set of rows. Both of these operations are performed in a single transaction.

**Overwriting an existing table**: By default, this library uses transactions to perform overwrites, which are implemented by deleting the destination table, creating a new empty table, and appending rows to it.

If the deprecated `usestagingtable` setting is set to `false` then this library will commit the `DELETE TABLE` command before appending rows to the new table, sacrificing the atomicity of the overwrite operation but reducing the amount of staging space that Redshift needs during the overwrite.

**Querying Redshift tables**: Queries use Redshift's [`UNLOAD`](https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html) command to execute a query and save its results to S3 and use [manifests](https://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html) to guard against certain eventually-consistent S3 operations. As a result, queries from Redshift data source for Spark should have the same consistency properties as regular Redshift queries.

## Common problems and solutions

### S3 bucket and Redshift cluster are in different AWS regions

By default, S3 <-> Redshift copies will not work if the S3 bucket and Redshift cluster are in different AWS regions.

If you attempt to perform a read of a Redshift table, and the regions are mismatched then you may see a confusing error, such as

```
java.sql.SQLException: [Amazon](500310) Invalid operation: S3ServiceException:The bucket you are attempting to access must be addressed using the specified endpoint. Please send all future requests to this endpoint.
```

Similarly, attempting to write to Redshift using a S3 bucket in a different region may cause the following error:

```
error:  Problem reading manifest file - S3ServiceException:The bucket you are attempting to access must be addressed using the specified endpoint. Please send all future requests to this endpoint.,Status 301,Error PermanentRedirect
```

To support an S3 bucket in a different region than the Redshift cluster for either **reads** or **writes**, set the `tempdir_region` parameter to the region of the S3 bucket. Doing so will automatically supply the region name to Redshift for UNLOAD and COPY commands. For example:
```
.option("tempdir_region", "us-east-1")
```

### Warnings Associated with EC2MetadataUtils 
The connector will attempt to automatically determine the cluster's region when the <tt>tempdir_region</tt> parameter is not set. This can cause performance problems and metadata exceptions to be thrown such as the following:

```
WARN EC2MetadataUtils: Unable to retrieve the requested metadata (/latest/dynamic/instance-identity/document). Failed to connect to service endpoint: 
com.amazonaws.SdkClientException: Failed to connect to service endpoint:
```

To resolve this, set the <tt>tempdir_region</tt> parameter to the AWS region of the S3 bucket specified in <tt>tempdir</tt> whenever the connector is used outside an AWS environment (e.g., local Spark cluster) like so:
```
.option("tempdir_region", "us-east-1")
```

This same problem can also occur when using <tt>secret.id</tt> as the connector will try to determine what AWS region stores the secret. To resolve this, set the <tt>secret.region</tt> parameter like so:

```
.option("secret.region", "us-east-1")
```

### Out of memory error from Redshift Server while running integration tests

The integration tests can run multiple queries at once to accelerate the testing process. The exact number that will be run at once though is determined by the number of cpu cores available to the JVM when the tests are run. If there are too many cpu cores, it is possible to cause an out of memory error in the redshift server.

If this occurs it can be avoided by setting the available number of cpu cores to a lower number using the JVM option `-Dscala.concurrent.context.maxThreads=10`. Set this in the `javaOptions` for testing in the build.sbt file. A setting of 10 has been found to work well.

## Migration Guide

- Version 3.0 now requires `forward_spark_s3_credentials` to be explicitly set before Spark S3
  credentials will be forwarded to Redshift. Users who use the `aws_iam_role` or `temporary_aws_*`
  authentication mechanisms will be unaffected by this change. Users who relied on the old default
  behavior will now need to explicitly set `forward_spark_s3_credentials` to `true` to continue
  using their previous Redshift to S3 authentication mechanism. For a discussion of the three
  authentication mechanisms and their security trade-offs, see the [_Authenticating to S3 and
  Redshift_](#authenticating-to-s3-and-redshift) section of this README.

## Acknowledgments ##
This project was originally forked from the [Databricks Spark-Redshift project](https://github.com/databricks/spark-redshift).
The pushdown improvements were inspired by the work done by Snowflake in their [Spark-Snowflake project](https://github.com/snowflakedb/spark-snowflake).
We would like to acknowledge and thank the developers of these projects for their significant contributions to the open source community.
