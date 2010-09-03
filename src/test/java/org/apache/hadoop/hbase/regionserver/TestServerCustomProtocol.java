package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;

public class TestServerCustomProtocol {
  /* Test protocol */
  private static interface PingProtocol extends VersionedProtocol {
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

  private HBaseTestingUtility util = new HBaseTestingUtility();
  private MiniHBaseCluster cluster = null;

  @Before
  public void setup() throws Exception {
    util.startMiniCluster();
    cluster = util.getMiniHBaseCluster();
    for (JVMClusterUtil.RegionServerThread t : cluster.getRegionServerThreads()) {
      t.getRegionServer()
       .server.registerProtocol(PingProtocol.class, new PingHandler());
    }
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public void testProtocolRegistration() throws Exception {
    HTable table = util.createTable(TEST_TABLE, TEST_FAMILY);
    Put put = new Put( Bytes.toBytes("r1") );
    put.add(TEST_FAMILY, Bytes.toBytes("col1"), Bytes.toBytes(1));
    table.put(put);

    PingProtocol pinger = table.exec(PingProtocol.class, put);
    String result = pinger.ping();
    assertEquals("Invalid custom protocol response", "pong", result);
    result = pinger.hello("George");
    assertEquals("Invalid custom protocol response", "Hello, George", result);
  }
}
