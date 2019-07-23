package io.github.spark_redshift_community.spark.redshift

import java.io.{File, FileInputStream, FileOutputStream}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileStatus, Path}
import org.apache.hadoop.fs.s3a.{S3AFileStatus, S3AFileSystem}


class MockLocalS3AFileSystem extends S3AFileSystem {

  override def open(f: Path): FSDataInputStream = new fs.FSDataInputStream(
    new FileInputStream("test_file.txt")
  )



  override def create(f: Path): FSDataOutputStream = new FSDataOutputStream(
    new FileOutputStream(new File("test_file.txt"))
  )

//  override def getFileStatus(f: Path): S3AFileStatus =
//    new S3AFileStatus(false, false, new Path("test_file.txt"))
//
//  override def getScheme: String = super.getScheme
//
//  override def getUri: URI = super.getUri
//
//  override def listStatus(f: Path): Array[FileStatus] = super.listStatus(f)
//
//  override def exists(f: Path): Boolean = super.exists(f)
//
//  override def isDirectory(f: Path): Boolean = false
//
//  override def isFile(f: Path): Boolean = super.isFile(f)
}
