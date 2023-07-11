/*
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.io.FileNotFoundException

import org.apache.hadoop.fs.{FileAlreadyExistsException, FileStatus, Path}
import org.apache.hadoop.fs.s3a.Tristate
import org.scalatest.matchers.should._
import org.scalatest.funsuite.AnyFunSuite

class InMemoryS3AFileSystemSuite extends AnyFunSuite with Matchers  {

  test("Create a file creates all prefixes in the hierarchy") {
    val inMemoryS3AFileSystem = new InMemoryS3AFileSystem()
    val path = new Path("s3a://test-bucket/temp-dir/ba7e0bf3-25a0-4435-b7a5-fdb6b3d2d328/_SUCCESS")

    inMemoryS3AFileSystem.create(path)

    assert(
      inMemoryS3AFileSystem.exists(
        new Path("s3a://test-bucket/temp-dir/ba7e0bf3-25a0-4435-b7a5-fdb6b3d2d328/_SUCCESS")))

    assert(
      inMemoryS3AFileSystem.exists(
        new Path("s3a://test-bucket/temp-dir/ba7e0bf3-25a0-4435-b7a5-fdb6b3d2d328/")))

    assert(inMemoryS3AFileSystem.exists(new Path("s3a://test-bucket/temp-dir/")))

  }

  test("List all statuses for a dir") {
    val inMemoryS3AFileSystem = new InMemoryS3AFileSystem()
    val path = new Path("s3a://test-bucket/temp-dir/ba7e0bf3-25a0-4435-b7a5-fdb6b3d2d328/_SUCCESS")
    val path2 = new Path(
      "s3a://test-bucket/temp-dir/ba7e0bf3-25a0-4435-b7a5-fdb6b3d2d328/manifest.json")

    inMemoryS3AFileSystem.create(path)
    inMemoryS3AFileSystem.create(path2)

    assert(
      inMemoryS3AFileSystem.listStatus(
        new Path("s3a://test-bucket/temp-dir/ba7e0bf3-25a0-4435-b7a5-fdb6b3d2d328")
      ).length == 2)

    assert(
      inMemoryS3AFileSystem.listStatus(
        new Path("s3a://test-bucket/temp-dir/ba7e0bf3-25a0-4435-b7a5-fdb6b3d2d328")
      ) === Array[FileStatus] (
        inMemoryS3AFileSystem.getFileStatus(path2),
        inMemoryS3AFileSystem.getFileStatus(path))
    )

    assert(
      inMemoryS3AFileSystem.listStatus(
        new Path("s3a://test-bucket/temp-dir/")).length == 1)
  }

  test("getFileStatus for file and dir") {
    val inMemoryS3AFileSystem = new InMemoryS3AFileSystem()
    val path = new Path("s3a://test-bucket/temp-dir/ba7e0bf3-25a0-4435-b7a5-fdb6b3d2d328/_SUCCESS")

    inMemoryS3AFileSystem.create(path)

    assert(inMemoryS3AFileSystem.getFileStatus(path).isDirectory === false)

    val dirPath = new Path(
      "s3a://test-bucket/temp-dir/ba7e0bf3-25a0-4435-b7a5-fdb6b3d2d328")
    val dirPathFileStatus = inMemoryS3AFileSystem.getFileStatus(dirPath)
    assert(dirPathFileStatus.isDirectory === true)
    assert(dirPathFileStatus.isEmptyDirectory === Tristate.FALSE)

  }

  test("Open a file from InMemoryS3AFileSystem") {
    val inMemoryS3AFileSystem = new InMemoryS3AFileSystem()
    val path = new Path("s3a://test-bucket/temp-dir/ba7e0bf3-25a0-4435-b7a5-fdb6b3d2d328/part0000")

    inMemoryS3AFileSystem.create(path).write("some data".getBytes())

    var result = new Array[Byte](9)
    inMemoryS3AFileSystem.open(path).read(result)

    assert(result === "some data".getBytes())

  }

  test ("delete file from FileSystem") {
    val inMemoryS3AFileSystem = new InMemoryS3AFileSystem()
    val path = new Path("s3a://test-bucket/temp-dir/ba7e0bf3-25a0-4435-b7a5-fdb6b3d2d328/part0000")

    inMemoryS3AFileSystem.create(path)

    assert(inMemoryS3AFileSystem.exists(path))

    inMemoryS3AFileSystem.delete(path, false)
    assert(inMemoryS3AFileSystem.exists(path) === false)

  }

  test("create already existing file throws FileAlreadyExistsException"){
    val inMemoryS3AFileSystem = new InMemoryS3AFileSystem()
    val path = new Path("s3a://test-bucket/temp-dir/ba7e0bf3-25a0-4435-b7a5-fdb6b3d2d328/part0000")
    inMemoryS3AFileSystem.create(path)
    assertThrows[FileAlreadyExistsException](inMemoryS3AFileSystem.create(path))
  }

  test("getFileStatus can't find file"){
    val inMemoryS3AFileSystem = new InMemoryS3AFileSystem()

    val path = new Path("s3a://test-bucket/temp-dir/ba7e0bf3-25a0-4435-b7a5-fdb6b3d2d328/part0000")
    assertThrows[FileNotFoundException](inMemoryS3AFileSystem.getFileStatus(path))
  }

  test("listStatus can't find path"){
    val inMemoryS3AFileSystem = new InMemoryS3AFileSystem()

    val path = new Path("s3a://test-bucket/temp-dir/ba7e0bf3-25a0-4435-b7a5-fdb6b3d2d328/part0000")
    assertThrows[FileNotFoundException](inMemoryS3AFileSystem.listStatus(path))
  }

}
