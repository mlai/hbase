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
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class TestClassloading extends HBaseClusterTestCase {
  static final Log LOG = LogFactory.getLog(TestClassloading.class);

  // This is a jar that contains a basic manifest and a single class
  // org/apache/hadoop/hbase/coprocessor/TestClassloading_Main which
  // implements the Coprocessor interface
  final static String encJar =
    "UEsDBBQACAAIAKGRKD0AAAAAAAAAAAAAAAAJAAQATUVUQS1JTkYv/soAAAMAUEsHCAAAAAACAAAA"+
    "AAAAAFBLAwQUAAgACAChkSg9AAAAAAAAAAAAAAAAFAAAAE1FVEEtSU5GL01BTklGRVNULk1G803M"+
    "y0xLLS7RDUstKs7Mz7NSMNQz4OVyLkpNLElN0XWqBAmY6RnEG1ooaASX5in4ZiYX5RdXFpek5hYr"+
    "eOYl62nycvFyAQBQSwcIBLuBuEcAAABHAAAAUEsDBBQACAAIAIqRKD0AAAAAAAAAAAAAAAA/AAAA"+
    "b3JnL2FwYWNoZS9oYWRvb3AvaGJhc2UvY29wcm9jZXNzb3IvVGVzdENsYXNzbG9hZGluZ19NYWlu"+
    "LmNsYXNzpZNLbxMxEIDHTZptt2kLpRQor5RX0xxiCakSKBUFRS0ghVZqqx56Qd6NlXW1WS/2JvBf"+
    "+BWckDjwA/hRiLF3Kx4N2k1ysD0ee77xPPzj57fvAPAUGi6U4LEDTxzYdKBOoLIjIpG8IFCqb50S"+
    "KLdllxNY7oiIHwz6HlcnzAtRs9KRPgtPmRJmnynLSSA0gVZHqh5lMfMDTgPWlTKmgcc0p76MlfS5"+
    "1lLRE66Tdsi0DiXriqj3/h0TUYvAfCzNgdQIfFkvhGr/lveioVAy6vMoaZ2ZCAhidqahEFhinlTJ"+
    "Ef8wwCfzLjLPCLh7n3weJ0JG2oEtAgv22bKPThIM4qMIw+M4FEkW0H440AGB3akCMvHMGdphzKMM"+
    "nDn5PBX4v7aK90yEXA25om+O7G7M27YIIYHmeGZopEy0ime94BrxIr9Gn6XUQTHNh1Fm6XCP5UD5"+
    "fF+Ytlwf2WrNczZkVZiFCoHnEzcsgSuGQ0MW9eihd87N6+iYpSDwbNziXTQfbMAMfmH8e/ifF3DF"+
    "eHB2cEdxJbjONr4C+YLCDMzhXLHKCszjXE0vgIumgHIVFvGWMd7FUTK6fw0XrWEtPcwMjbQEy/Yc"+
    "EwJX0WIF5TLuruFYzceu5mKvX8KuwY0Mu21pI7DrFruWHl7C/gm7mQ+rFYbdQscp7K3VjoBtWlgj"+
    "PRwZ8G24k7m6i1LpLwf38jPanKBQ9/Ox2xMUqpaf21bh3G7kw14Vhj3Ih70uDHtoLR/9AlBLBwj3"+
    "MWISLwIAAF0HAABQSwECFAAUAAgACAChkSg9AAAAAAIAAAAAAAAACQAEAAAAAAAAAAAAAAAAAAAA"+
    "TUVUQS1JTkYv/soAAFBLAQIUABQACAAIAKGRKD0Eu4G4RwAAAEcAAAAUAAAAAAAAAAAAAAAAAD0A"+
    "AABNRVRBLUlORi9NQU5JRkVTVC5NRlBLAQIUABQACAAIAIqRKD33MWISLwIAAF0HAAA/AAAAAAAA"+
    "AAAAAAAAAMYAAABvcmcvYXBhY2hlL2hhZG9vcC9oYmFzZS9jb3Byb2Nlc3Nvci9UZXN0Q2xhc3Ns"+
    "b2FkaW5nX01haW4uY2xhc3NQSwUGAAAAAAMAAwDqAAAAYgMAAAAA";

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

