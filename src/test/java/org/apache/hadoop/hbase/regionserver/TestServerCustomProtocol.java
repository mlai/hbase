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
package org.apache.hadoop.hbase.regionserver;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.junit.*;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TestServerCustomProtocol {
  /* Test protocol */
  private static interface PingProtocol extends CoprocessorProtocol {
    public String ping();
    public String hello(String name);
  }

  /* Test protocol implementation */
  private static class PingHandler implements PingProtocol, HBaseRPCProtocolVersion {
    @Override
    public String ping() {
      return "pong";
    }

    @Override
    public String hello(String name) {
      return "Hello, "+name;
    }

    @Override
    public long getProtocolVersion(String s, long l) throws IOException {
      return versionID;
    }
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

    // TODO: use a test coprocessor for registration (once merged with CP code)
    // sleep here is an ugly hack to allow region transitions to finish
    Thread.sleep(5000);
    for (JVMClusterUtil.RegionServerThread t : cluster.getRegionServerThreads()) {
      for (HRegionInfo r : t.getRegionServer().getOnlineRegions()) {
        t.getRegionServer().registerProtocol(r.getRegionName(),
            PingProtocol.class,
            new PingHandler());
      }
    }
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
  public void testSingleProxy() throws Exception {
    HTable table = new HTable(util.getConfiguration(), TEST_TABLE);

    PingProtocol pinger = table.proxy(PingProtocol.class, new Get(ROW_A));
    String result = pinger.ping();
    assertEquals("Invalid custom protocol response", "pong", result);
    result = pinger.hello("George");
    assertEquals("Invalid custom protocol response", "Hello, George", result);
  }

  @Test
  public void testRowList() throws Exception {
    HTable table = new HTable(util.getConfiguration(), TEST_TABLE);

    List<? extends Row> rows = Lists.newArrayList(
        new Get(ROW_A), new Get(ROW_B), new Get(ROW_C));
    Map<byte[],String> results = table.exec(PingProtocol.class, rows,
        new HTable.BatchCall<PingProtocol,String>() {
          public String call(PingProtocol instance) {
            return instance.ping();
          }
        });

    verifyRegionResults(table, results, ROW_A);
    verifyRegionResults(table, results, ROW_B);
    verifyRegionResults(table, results, ROW_C);
  }

  @Test
  public void testRowRange() throws Exception {
    HTable table = new HTable(util.getConfiguration(), TEST_TABLE);

    // test empty range
    Scan scan = new Scan();
    Map<byte[],String> results = table.exec(PingProtocol.class, scan,
        new HTable.BatchCall<PingProtocol,String>() {
          public String call(PingProtocol instance) {
            return instance.ping();
          }
        });
    // should contain all three rows/regions
    verifyRegionResults(table, results, ROW_A);
    verifyRegionResults(table, results, ROW_B);
    verifyRegionResults(table, results, ROW_C);

    // test start row + empty end
    scan = new Scan(ROW_BC);
    results = table.exec(PingProtocol.class, scan,
        new HTable.BatchCall<PingProtocol,String>() {
          public String call(PingProtocol instance) {
            return instance.ping();
          }
        });
    // should contain last 2 regions
    HRegionLocation loc = table.getRegionLocation(ROW_A);
    assertNull("Should be missing region for row aaa (prior to start row)",
        results.get(loc.getRegionInfo().getRegionName()));
    verifyRegionResults(table, results, ROW_B);
    verifyRegionResults(table, results, ROW_C);

    // test empty start + end
    scan = new Scan(HConstants.EMPTY_START_ROW, ROW_BC);
    results = table.exec(PingProtocol.class, scan,
        new HTable.BatchCall<PingProtocol,String>() {
          public String call(PingProtocol instance) {
            return instance.ping();
          }
        });
    // should contain the first 2 regions
    verifyRegionResults(table, results, ROW_A);
    verifyRegionResults(table, results, ROW_B);
    loc = table.getRegionLocation(ROW_C);
    assertNull("Should be missing region for row ccc (past stop row)",
        results.get(loc.getRegionInfo().getRegionName()));

    // test explicit start + end
    scan = new Scan(ROW_AB, ROW_BC);
    results = table.exec(PingProtocol.class, scan,
        new HTable.BatchCall<PingProtocol,String>() {
          public String call(PingProtocol instance) {
            return instance.ping();
          }
        });
    // should contain first 2 regions
    verifyRegionResults(table, results, ROW_A);
    verifyRegionResults(table, results, ROW_B);
    loc = table.getRegionLocation(ROW_C);
    assertNull("Should be missing region for row ccc (past stop row)",
        results.get(loc.getRegionInfo().getRegionName()));

    // test single region
    scan = new Scan(ROW_B, ROW_BC);
    results = table.exec(PingProtocol.class, scan,
        new HTable.BatchCall<PingProtocol,String>() {
          public String call(PingProtocol instance) {
            return instance.ping();
          }
        });
    // should only contain region bbb
    verifyRegionResults(table, results, ROW_B);
    loc = table.getRegionLocation(ROW_A);
    assertNull("Should be missing region for row aaa (prior to start)",
        results.get(loc.getRegionInfo().getRegionName()));
    loc = table.getRegionLocation(ROW_C);
    assertNull("Should be missing region for row ccc (past stop row)",
        results.get(loc.getRegionInfo().getRegionName()));
  }

  private void verifyRegionResults(HTable table,
      Map<byte[],String> results, byte[] row) throws Exception {
    HRegionLocation loc = table.getRegionLocation(row);
    byte[] region = loc.getRegionInfo().getRegionName();
    assertNotNull("Results should contain region " +
        Bytes.toStringBinary(region)+" for row '"+Bytes.toStringBinary(row)+"'",
        results.get(region));
    assertEquals("Invalid result for row '"+Bytes.toStringBinary(row)+"'",
        "pong", results.get(region));
  }
}
