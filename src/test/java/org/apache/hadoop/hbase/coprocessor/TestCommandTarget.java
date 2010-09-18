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

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.mortbay.log.Log;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TestCommandTarget {
  /* Test protocol */
  private static interface PingProtocol extends CoprocessorProtocol {
    public String ping();
    public String hello(String name);
  }

  private static final byte[] TEST_TABLE = Bytes.toBytes("test");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("f1");

  private static final byte[] ROW_A = Bytes.toBytes("aaa");
  private static final byte[] ROW_B = Bytes.toBytes("bbb");
  private static final byte[] ROW_C = Bytes.toBytes("ccc");

  private static final byte[] ROW_AB = Bytes.toBytes("abb");
  private static final byte[] ROW_BC = Bytes.toBytes("bcc");

  private static HBaseTestingUtility util = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster = null;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util.startMiniCluster(2);
    cluster = util.getMiniHBaseCluster();
    HTable table = util.createTable(TEST_TABLE, TEST_FAMILY);
    util.createMultiRegions(util.getConfiguration(), table, TEST_FAMILY,
        new byte[][]{ HConstants.EMPTY_BYTE_ARRAY,
            ROW_B, ROW_C});

    Put puta = new Put( ROW_A );
    puta.add(TEST_FAMILY, Bytes.toBytes("col1"), Bytes.toBytes(1));
    table.put(puta);

    Put putb = new Put( ROW_B );
    putb.add(TEST_FAMILY, Bytes.toBytes("col1"), Bytes.toBytes(1));
    table.put(putb);

    Put putc = new Put( ROW_C );
    putc.add(TEST_FAMILY, Bytes.toBytes("col1"), Bytes.toBytes(1));
    table.put(putc);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public void testSingleRegionAggregation() throws Exception {
//    HTable table = new HTable(util.getConfiguration(), TEST_TABLE);
//
//    SumProtocol sum = table.proxy(SumProtocol.class, 
//        new Get(ROW_A));
//    String result = sum.sum(ROW_A);
//    Log.warn("---- get " + result);
  }


  
  private void verifyRegionResults(HTable table,
      Map<byte[],String> results, byte[] row) throws Exception {
    verifyRegionResults(table, results, "pong", row);
  }

  private void verifyRegionResults(HTable table,
      Map<byte[],String> results, String expected, byte[] row) throws Exception {
    HRegionLocation loc = table.getRegionLocation(row);
    byte[] region = loc.getRegionInfo().getRegionName();
    assertNotNull("Results should contain region " +
        Bytes.toStringBinary(region)+" for row '"+Bytes.toStringBinary(row)+"'",
        results.get(region));
    assertEquals("Invalid result for row '"+Bytes.toStringBinary(row)+"'",
        expected, results.get(region));
  }
}
