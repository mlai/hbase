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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class TestClassloading extends HBaseClusterTestCase {

  // This is a jar that contains a basic manifest and a single class
  // org/apache/hadoop/hbase/coprocessor/TestClassloading_Main which
  // implements the Coprocessor interface
  final static String encJar =
"UEsDBAoAAAAAAPkMTTwAAAAAAAAAAAAAAAAJABwATUVUQS1JTkYvVVQJAANlc3ZLaHN2S3V4CwAB"+
"BOgDAAAE6AMAAFBLAwQUAAIACAD4DE08WQqiYI4AAACtAAAAFAAcAE1FVEEtSU5GL01BTklGRVNU"+
"Lk1GVVQJAANkc3ZLcHN2S3V4CwABBOgDAAAE6AMAAE2OMQ+CMBBG9yb9Dx11KAVjYsKGTA6dNK7m"+
"KCc0wTvSw4F/b2Fyfd/Ly+eB4htlsU9MEplqUxWlVg39kWaGMKLJLI+XbW4TwoK9va7ZPxcn25WV"+
"Ody/ZHwMiWWVBT9ibhSKo1YeItl2ApHacBoc7D03Qs88u7EDQRd4ThxQhJN75D+7PjH0kYbXFtBK"+
"qx9QSwMECgAAAAAAxQxNPAAAAAAAAAAAAAAAAAQAHABvcmcvVVQJAAMBc3ZLaHN2S3V4CwABBOgD"+
"AAAE6AMAAFBLAwQKAAAAAADFDE08AAAAAAAAAAAAAAAACwAcAG9yZy9hcGFjaGUvVVQJAAMBc3ZL"+
"CnN2S3V4CwABBOgDAAAE6AMAAFBLAwQKAAAAAADFDE08AAAAAAAAAAAAAAAAEgAcAG9yZy9hcGFj"+
"aGUvaGFkb29wL1VUCQADAXN2Swpzdkt1eAsAAQToAwAABOgDAABQSwMECgAAAAAAxQxNPAAAAAAA"+
"AAAAAAAAABgAHABvcmcvYXBhY2hlL2hhZG9vcC9oYmFzZS9VVAkAAwFzdksLc3ZLdXgLAAEE6AMA"+
"AAToAwAAUEsDBAoAAAAAAMoMTTwAAAAAAAAAAAAAAAAkABwAb3JnL2FwYWNoZS9oYWRvb3AvaGJh"+
"c2UvY29wcm9jZXNzb3IvVVQJAAMLc3ZLaHN2S3V4CwABBOgDAAAE6AMAAFBLAwQUAAIACAC6DE08"+
"ccr0WAwCAADVBQAAPwAcAG9yZy9hcGFjaGUvaGFkb29wL2hiYXNlL2NvcHJvY2Vzc29yL1Rlc3RD"+
"bGFzc2xvYWRpbmdfTWFpbi5jbGFzc1VUCQAD8HJ2S/Bydkt1eAsAAQToAwAABOgDAAClU8tuEzEU"+
"PU7STF6ktJQC5VkoELroSEgsUHhHVFQKrdRWXXSDnImVceXYwZ6En+EnWCGx4AP4KMQdZyQQBIUw"+
"C/vea/uce2zf++37l68AHqIVoMDw2NhByEc8ikUY874xozDucSfCyIysiYRzxobHwiUdxZ1Thvel"+
"Hrx7y6UOUGI4f8YnPFRcD8KD3pmIkgBlhvBfSDs/fYbyE6ll8oyh2HpwwlDqmL6ooYh6AwEqDMtd"+
"qcX+eNgT9pj3lGBY7ZqIqxNuZRpni6Uklo6h3f3vW7VJi9EHI6EZXrS6C15k67WeSGv0UOiknV6E"+
"kainuVgYAqM7yjhieplP0GmqqMl7xiaH4v2Yri/6JPHUp9hVYxczVCmZGVKChOFVznQ+XyUiOiUS"+
"kl/9IJU6GimZ+IyZ9zFflr+CrRhIo52wE2HDN4c+WvC0/0HFsLMYjEBU0rUjM7aR2JVpYW7MLLad"+
"tHsYGntaC+t3hQtwjwo4x4sw1H+JsAlqKhpL1PRlstRONFcpCskyskvbn1H7RE4BjewQLeIczY3M"+
"b2KZLLU7VjLwI3+e1n4HVjxwfbqZAVNvFRd8vjVczCie0yjOoqh7ilvTzT8oUm8dlwjBcHm+nuYc"+
"PVewkVHs0SjNoljxFNvTzZl6rno9qXeNvPS5r+PGPNq1ubQ36RGmtJv+OQq47TnuYItsjdbv0ufc"+
"L1d/AFBLAQIeAwoAAAAAAPkMTTwAAAAAAAAAAAAAAAAJABgAAAAAAAAAEADtQQAAAABNRVRBLUlO"+
"Ri9VVAUAA2Vzdkt1eAsAAQToAwAABOgDAABQSwECHgMUAAIACAD4DE08WQqiYI4AAACtAAAAFAAY"+
"AAAAAAABAAAApIFDAAAATUVUQS1JTkYvTUFOSUZFU1QuTUZVVAUAA2Rzdkt1eAsAAQToAwAABOgD"+
"AABQSwECHgMKAAAAAADFDE08AAAAAAAAAAAAAAAABAAYAAAAAAAAABAA7UEfAQAAb3JnL1VUBQAD"+
"AXN2S3V4CwABBOgDAAAE6AMAAFBLAQIeAwoAAAAAAMUMTTwAAAAAAAAAAAAAAAALABgAAAAAAAAA"+
"EADtQV0BAABvcmcvYXBhY2hlL1VUBQADAXN2S3V4CwABBOgDAAAE6AMAAFBLAQIeAwoAAAAAAMUM"+
"TTwAAAAAAAAAAAAAAAASABgAAAAAAAAAEADtQaIBAABvcmcvYXBhY2hlL2hhZG9vcC9VVAUAAwFz"+
"dkt1eAsAAQToAwAABOgDAABQSwECHgMKAAAAAADFDE08AAAAAAAAAAAAAAAAGAAYAAAAAAAAABAA"+
"7UHuAQAAb3JnL2FwYWNoZS9oYWRvb3AvaGJhc2UvVVQFAAMBc3ZLdXgLAAEE6AMAAAToAwAAUEsB"+
"Ah4DCgAAAAAAygxNPAAAAAAAAAAAAAAAACQAGAAAAAAAAAAQAO1BQAIAAG9yZy9hcGFjaGUvaGFk"+
"b29wL2hiYXNlL2NvcHJvY2Vzc29yL1VUBQADC3N2S3V4CwABBOgDAAAE6AMAAFBLAQIeAxQAAgAI"+
"ALoMTTxxyvRYDAIAANUFAAA/ABgAAAAAAAAAAACkgZ4CAABvcmcvYXBhY2hlL2hhZG9vcC9oYmFz"+
"ZS9jb3Byb2Nlc3Nvci9UZXN0Q2xhc3Nsb2FkaW5nX01haW4uY2xhc3NVVAUAA/Bydkt1eAsAAQTo"+
"AwAABOgDAABQSwUGAAAAAAgACADpAgAAIwUAAAAA";

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
    for (HRegion region: hbase.getRegionServer(0).getOnlineRegions()) {
      if (region.getRegionNameAsString().startsWith(getClass().getName())) {
        Coprocessor c = region.getCoprocessorHost().findCoprocessor(className);
        found = (c != null);
      }
    }
    assertTrue(found);
  }
}

