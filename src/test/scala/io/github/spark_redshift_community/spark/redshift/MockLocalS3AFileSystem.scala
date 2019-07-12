package com.spark_redshift_community.spark.redshift

import java.io.{File, FileInputStream, FileOutputStream}

import org.apache.hadoop.fs
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, Path}
import org.apache.hadoop.fs.s3a.{S3AFileStatus, S3AFileSystem}


class MockLocalS3AFileSystem extends S3AFileSystem {

  override def open(f: Path): FSDataInputStream = new fs.FSDataInputStream(
    new FileInputStream("test_file.txt")
  )

  override def create(f: Path): FSDataOutputStream = new FSDataOutputStream(
    new FileOutputStream(new File("test_file.txt"))
  )

  override def getFileStatus(f: Path): S3AFileStatus =
    new S3AFileStatus(false, false, new Path("somePath"))
}
