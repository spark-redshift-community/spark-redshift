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

import com.typesafe.sbt.pgp.PgpKeys
import org.scalastyle.sbt.ScalastylePlugin.rawScalastyleSettings
import sbt.Keys._
import sbt._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import sbtrelease.ReleasePlugin.autoImport._
import scoverage.ScoverageKeys
import java.util.Properties
import java.io.FileInputStream

val buildScalaVersion = sys.props.get("scala.buildVersion").getOrElse("2.12.15")
val sparkVersion = "3.3.2"
val isCI = "true" equalsIgnoreCase System.getProperty("config.CI")

// Define a custom test configuration so that unit test helper classes can be re-used under
// the integration tests configuration; see http://stackoverflow.com/a/20635808.
lazy val IntegrationTest = config("it") extend Test
val testSparkVersion = sys.props.get("spark.testVersion").getOrElse(sparkVersion)
val testHadoopVersion = sys.props.get("hadoop.testVersion").getOrElse("3.3.3")
// DON't UPGRADE AWS-SDK-JAVA if not compatible with hadoop version
val testAWSJavaSDKVersion = sys.props.get("aws.testVersion").getOrElse("1.11.1033")
// access tokens for aws/shared and our own internal CodeArtifacts repo
// these are retrieved during CodeBuild steps
val awsSharedRepoPass = sys.props.get("ci.internalCentralMvnPassword").getOrElse("")
val internalReleaseRepoPass = sys.props.get("ci.internalTeamMvnPassword").getOrElse("")
// remove the PATCH portion of the spark version number for use in release binary
// e.g. MAJOR.MINOR.PATCH => MAJOR.MINOR
val releaseSparkVersion = testSparkVersion.substring(0, testSparkVersion.lastIndexOf("."))

def ciPipelineSettings[P](condition: Boolean): Seq[Def.Setting[_]] = {
  if (condition) {
    val (fetchRealm, publishRealm, fetchUrl, publishUrl, fetchRepo, publishRepo, userName) =
      try {
        val prop = new Properties()
        prop.load(new FileInputStream("ci/internal_ci.properties"))
        (
          prop.getProperty("fetchRealm"),
          prop.getProperty("publishRealm"),
          prop.getProperty("fetchUrl"),
          prop.getProperty("publishUrl"),
          prop.getProperty("fetchRepo"),
          prop.getProperty("publishRepo"),
          prop.getProperty("userName")
        )
      } catch {
        case e: Exception =>
          e.printStackTrace()
          sys.exit(1)
      }
    Seq(
      credentials := Seq(
        Credentials(fetchRealm, fetchUrl, userName, awsSharedRepoPass),
        Credentials(publishRealm, publishUrl, userName, internalReleaseRepoPass)
      ),
      resolvers := Seq(fetchRealm at fetchRepo),
      externalResolvers := Seq(fetchRealm at fetchRepo),
      publishTo := Some (publishRealm at publishRepo),
      pomIncludeRepository := { (_: MavenRepository) => false },
//      skip in publish := true, // skip release to bintray
      releaseProcess := Seq[ReleaseStep](publishArtifacts)
    )
  }
  else Seq(
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    },
    // Add publishing to spark packages as another step.
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runTest,
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      publishArtifacts,
      setNextVersion,
      commitNextVersion,
      pushChanges
    )
  )
}

lazy val root = Project("spark-redshift", file("."))
  .enablePlugins(BuildInfoPlugin)
  .configs(IntegrationTest)
  .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
  .settings(Project.inConfig(IntegrationTest)(rawScalastyleSettings()): _*)
  .settings(Defaults.coreDefaultSettings: _*)
  .settings(Defaults.itSettings: _*)
  .settings(ciPipelineSettings(isCI))
  .settings(
    name := "spark-redshift",
    version += s"-spark_${releaseSparkVersion}",
    organization := "io.github.spark-redshift-community",
    scalaVersion := buildScalaVersion,
    licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"),
    credentials += Credentials(Path.userHome / ".sbt" / ".credentials"),
    scalacOptions ++= Seq("-target:jvm-1.8"),
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % "1.7.32",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.1",

      "com.google.guava" % "guava" % "27.0.1-jre" % "test",
      "org.scalatest" %% "scalatest" % "3.0.5" % "test",
      "org.mockito" % "mockito-inline" % "4.9.0" % "test",

      "com.amazon.redshift" % "redshift-jdbc42" % "2.1.0.9" % "provided",

      "com.amazonaws" % "aws-java-sdk" % testAWSJavaSDKVersion % "provided" excludeAll
        (ExclusionRule(organization = "com.fasterxml.jackson.core")),

      "org.apache.hadoop" % "hadoop-client" % testHadoopVersion % "provided" exclude("javax.servlet", "servlet-api") force(),
      "org.apache.hadoop" % "hadoop-common" % testHadoopVersion % "provided" exclude("javax.servlet", "servlet-api") force(),
      "org.apache.hadoop" % "hadoop-common" % testHadoopVersion % "provided" classifier "tests" force(),

      "org.apache.hadoop" % "hadoop-aws" % testHadoopVersion excludeAll
        (ExclusionRule(organization = "com.fasterxml.jackson.core"))
        exclude("org.apache.hadoop", "hadoop-common")
        exclude("com.amazonaws", "aws-java-sdk-s3")  force(),

      "org.apache.spark" %% "spark-core" % testSparkVersion % "provided" exclude("org.apache.hadoop", "hadoop-client") force(),
      "org.apache.spark" %% "spark-sql" % testSparkVersion % "provided" exclude("org.apache.hadoop", "hadoop-client") force(),
      "org.apache.spark" %% "spark-hive" % testSparkVersion % "provided" exclude("org.apache.hadoop", "hadoop-client") force(),
      "org.apache.spark" %% "spark-avro" % testSparkVersion % "provided" exclude("org.apache.avro", "avro-mapred") force()
    ),
    retrieveManaged := true,
    ScoverageKeys.coverageHighlighting := true,
    logBuffered := false,
    // Display full-length stacktraces from ScalaTest:
    testOptions in Test += Tests.Argument("-oF"),
    fork in Test := true,
    javaOptions in Test ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M"),

    /********************
     * Release settings *
     ********************/

    publishMavenStyle := true,
    releaseCrossBuild := true,
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,

    pomExtra :=
    <url>https://github.com:spark_redshift_community/spark.redshift</url>
    <scm>
      <url>git@github.com:spark_redshift_community/spark.redshift.git</url>
      <connection>scm:git:git@github.com:spark_redshift_community/spark.redshift.git</connection>
    </scm>
    <developers>
      <developer>
        <id>meng</id>
        <name>Xiangrui Meng</name>
        <url>https://github.com/mengxr</url>
      </developer>
      <developer>
        <id>JoshRosen</id>
        <name>Josh Rosen</name>
        <url>https://github.com/JoshRosen</url>
      </developer>
      <developer>
        <id>marmbrus</id>
        <name>Michael Armbrust</name>
        <url>https://github.com/marmbrus</url>
      </developer>
      <developer>
        <id>lucagiovagnoli</id>
        <name>Luca Giovagnoli</name>
        <url>https://github.com/lucagiovagnoli</url>
      </developer>
    </developers>,

    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "io.github.spark_redshift_community.spark.redshift"
  )
