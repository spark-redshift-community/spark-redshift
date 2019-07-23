/*
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

package io.github.spark_redshift_community.spark.redshift;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.mockito.Mockito;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Relays FS calls to the mocked FS, allows for some extra logging with
 * stack traces to be included, stubbing out other methods
 * where needed to avoid failures.
 *
 * The logging is useful for tracking
 * down why there are extra calls to a method than a test would expect:
 * changes in implementation details often trigger such false-positive
 * test failures.
 *
 * This class is in the s3a package so that it has access to methods
 */
public class MockS3AFileSystem extends S3AFileSystem {
    public static final String BUCKET = "bucket-name";
    public static final URI FS_URI = URI.create("s3a://" + BUCKET + "/");

    private final S3AFileSystem mock = Mockito.mock(S3AFileSystem.class);

    private final Path root = new Path(FS_URI.toString());

    private Configuration conf;

//    public MockS3AFileSystem(S3AFileSystem mock) {
//        this.mock = mock;
//        root = new Path(FS_URI.toString());
//    }

    @Override
    public URI getUri() {
        return FS_URI;
    }

    @Override
    public Path getWorkingDirectory() {
        return new Path(root, "work");
    }

    @Override
    public void initialize(URI name, Configuration originalConf)
            throws IOException {
        conf = originalConf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public boolean exists(Path f) throws IOException {
        return true;
//        return mock.exists(f);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        return mock.open(f, bufferSize);
    }

    @Override
    public FSDataOutputStream create(Path f) throws IOException {
        return new FSDataOutputStream(new FileOutputStream(new File("test_file.txt")));
    }

    @Override
    public FSDataOutputStream create(Path f,
                                     FsPermission permission,
                                     boolean overwrite,
                                     int bufferSize,
                                     short replication,
                                     long blockSize,
                                     Progressable progress) throws IOException {
        return mock.create(f, permission, overwrite, bufferSize, replication,
                blockSize, progress);
    }

    @Override
    public FSDataOutputStream append(Path f,
                                     int bufferSize,
                                     Progressable progress) throws IOException {
        return mock.append(f, bufferSize, progress);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return mock.rename(src, dst);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        return mock.delete(f, recursive);
    }

    @Override
    public FileStatus[] listStatus(Path f)
            throws IOException {
        return mock.listStatus(f);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive)
            throws IOException {
        return new EmptyIterator();
    }

    @Override
    public void setWorkingDirectory(Path newDir) {
        mock.setWorkingDirectory(newDir);
    }

    @Override
    public boolean mkdirs(Path f) throws IOException {
        return mock.mkdirs(f);
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        return mock.mkdirs(f, permission);
    }

    @Override
    public S3AFileStatus getFileStatus(Path f) throws IOException {
        return checkNotNull(mock.getFileStatus(f),
                "Mock getFileStatus(%s) returned null", f);
    }

    @Override
    public long getDefaultBlockSize(Path f) {
        return mock.getDefaultBlockSize(f);
    }

    @Override
    @SuppressWarnings("deprecation")
    public long getDefaultBlockSize() {
        return mock.getDefaultBlockSize();
    }

    private static class EmptyIterator implements
            RemoteIterator<LocatedFileStatus> {
        @Override
        public boolean hasNext() throws IOException {
            return false;
        }

        @Override
        public LocatedFileStatus next() throws IOException {
            return null;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(
                "MockS3AFileSystem{");
        sb.append("inner mockFS=").append(mock);
        sb.append('}');
        return sb.toString();
    }
}