/*
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.coprocessor.Coprocessor.Priority;
import org.apache.hadoop.hbase.regionserver.CoprocessorHost;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class TestClassloading extends HBaseClusterTestCase {
  static final Log LOG = LogFactory.getLog(TestClassloading.class);

  // This is a jar that contains a basic manifest and a single class
  // org/apache/hadoop/hbase/coprocessor/TestClassloading_Main which
  // implements the Coprocessor interface
  final static String encJar =
    "UEsDBBQACAAIALCOGj0AAAAAAAAAAAAAAAAJAAQATUVUQS1JTkYv/soAAAMAUEsHCAAAAAACAAAA"+
    "AAAAAFBLAwQUAAgACACwjho9AAAAAAAAAAAAAAAAFAAAAE1FVEEtSU5GL01BTklGRVNULk1G803M"+
    "y0xLLS7RDUstKs7Mz7NSMNQz4OVyLkpNLElN0XWqBAmY6RnEG1ooaASX5in4ZiYX5RdXFpek5hYr"+
    "eOYl62nycvFyAQBQSwcIBLuBuEcAAABHAAAAUEsDBBQACAAIACCLGj0AAAAAAAAAAAAAAAA/AAAA"+
    "b3JnL2FwYWNoZS9oYWRvb3AvaGJhc2UvY29wcm9jZXNzb3IvVGVzdENsYXNzbG9hZGluZ19NYWlu"+
    "LmNsYXNzpVTdThNREJ7Tf9qFAkoVFKQoClW6gqISCEoaGy5QEiBccGNOt8d2yXZP3d3Wh/EVvDHR"+
    "mHjhA/hQxpmz2zRZjkHszfx1vm/mzMz21+8fPwFgA7bykIPHY5CFdRIbJJ6QeEpiMwvPsvA8Cy8Y"+
    "ZGRXuKLJgJ2hYznSJ2fMkp0utwKys++dnt8mK+13HTvAtB3btYNdBsmV1VMGqZpsCgbFA9sVb3ud"+
    "hvBOeMPByPSBtLhzyj2b/CiYCtq2z2D7QHotk2ORtjDbvCll12w3uC9MS3Y9aQnfl555Ivyg5nDf"+
    "dyRv2m7r3Rtuu9vUtXuIfTN4ufJPPLWh/drt2550O8INtql7hj3tjEKCE5JujQbH4NVI7ZxRPxO8"+
    "Ib3gSHzo4dvVLpA93AaDvdH4VYEc7dYRAbY79tF2nONwqfiIOi1aWVHs00jl/or1RMuWri+8vvDM"+
    "/SPlXTFbbc5hUL0aDEEePZv7h9HZ4wmfhZFadPt5tOuDkzfoh+G3kEM3mk3+WPY8S9RtOuo57aFW"+
    "z3mfGzAJUwbkoWCAQWKcxASJIhQYbP33d8BgkgqYDndb5mHjXNCFmFdcGJTxDyEHOAJIQ4LaxD+Q"+
    "BHWq9HikJyJdVLqA+fgqlNPomagZ6nTlO7AvKu0ayowK5uA6SiNMgBkooWZwA25G4F3MTlB2JfUN"+
    "EkN0XkWnEDOtGEphVsRA1izMqcK34HbEtY85yQFXMs41g1wlxbUYZl3gImseH5dAewHuRKxH6KcG"+
    "rKk46yyyzinWSpilZV1UrGSV0UpidEk/gXScfx75Fy6ZwF24p+s1E+cqI9fSpb0uw/2o0gO0aFIr"+
    "sHph1V8h8Tm26mXNqqmUDpyMg1e14IdacDoOXtOCH2nBqTh4XQte04IzcfCm9ryrKsv8A1BLBwgH"+
    "vyMKoQIAAI8HAABQSwECFAAUAAgACACwjho9AAAAAAIAAAAAAAAACQAEAAAAAAAAAAAAAAAAAAAA"+
    "TUVUQS1JTkYv/soAAFBLAQIUABQACAAIALCOGj0Eu4G4RwAAAEcAAAAUAAAAAAAAAAAAAAAAAD0A"+
    "AABNRVRBLUlORi9NQU5JRkVTVC5NRlBLAQIUABQACAAIACCLGj0HvyMKoQIAAI8HAAA/AAAAAAAA"+
    "AAAAAAAAAMYAAABvcmcvYXBhY2hlL2hhZG9vcC9oYmFzZS9jb3Byb2Nlc3Nvci9UZXN0Q2xhc3Ns"+
    "b2FkaW5nX01haW4uY2xhc3NQSwUGAAAAAAMAAwDqAAAA1AMAAAAA";

  final static String className = "TestClassloading_Main";
  final static String classFullName =
    "org.apache.hadoop.hbase.coprocessor.TestClassloading_Main";

  public void testClassLoadingFromHDFS() throws Exception {
    MiniDFSCluster dfs = this.dfsCluster;
    FileSystem fs = dfs.getFileSystem();

    // write the jar into dfs
    Path path = new Path(fs.getUri() + Path.SEPARATOR +
      "TestClassloading.jar");
    FSDataOutputStream os = fs.create(path, true);
    os.write(Base64.decode(encJar));
    os.close();

    // create a table that references the jar
    HTableDescriptor htd = new HTableDescriptor(getClass().getName());
    htd.addFamily(new HColumnDescriptor("test"));
    htd.setValue("Coprocessor$1",
      path.toString() +
      ":" + classFullName +
      ":" + Coprocessor.Priority.USER);
    HBaseAdmin admin = new HBaseAdmin(this.conf);
    admin.createTable(htd);

    // verify that the coprocessor was loaded
    boolean found = false;
    MiniHBaseCluster hbase = this.cluster;
    for (HRegion region: hbase.getRegionServer(0).getOnlineRegionsLocalContext()) {
      if (region.getRegionNameAsString().startsWith(getClass().getName())) {
        Coprocessor c = region.getCoprocessorHost().findCoprocessor(className);
        found = (c != null);
      }
    }
    assertTrue(found);
  }
}

