package org.apache.hadoop.hbase.regionserver;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
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

  private static HBaseTestingUtility util = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster = null;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util.startMiniCluster(2);
    cluster = util.getMiniHBaseCluster();
    for (JVMClusterUtil.RegionServerThread t : cluster.getRegionServerThreads()) {
      t.getRegionServer()
       .server.registerProtocol(PingProtocol.class, new PingHandler());
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public void testProtocolRegistration() throws Exception {
    HTable table = util.createTable(TEST_TABLE, TEST_FAMILY);
    util.createMultiRegions(table, TEST_FAMILY);

    Put puta = new Put( Bytes.toBytes("aaa") );
    puta.add(TEST_FAMILY, Bytes.toBytes("col1"), Bytes.toBytes(1));
    table.put(puta);

    PingProtocol pinger = table.proxy(PingProtocol.class, puta);
    String result = pinger.ping();
    assertEquals("Invalid custom protocol response", "pong", result);
    result = pinger.hello("George");
    assertEquals("Invalid custom protocol response", "Hello, George", result);

    Put putb = new Put( Bytes.toBytes("bbb") );
    putb.add(TEST_FAMILY, Bytes.toBytes("col1"), Bytes.toBytes(1));
    table.put(putb);

    Put putc = new Put( Bytes.toBytes("ccc") );
    putc.add(TEST_FAMILY, Bytes.toBytes("col1"), Bytes.toBytes(1));
    table.put(putc);

    List<? extends Row> rows = Lists.newArrayList(puta, putb, putc);
    Map<Row,String> results = table.exec(PingProtocol.class, rows,
        new HTable.BatchCall<PingProtocol,String>() {
          public String call(PingProtocol instance) {
            return instance.ping();
          }
        });
    assertNotNull("Results should contain row 'aaa'", results.get(puta));
    assertEquals("Invalid result for row 'aaa'", "pong", results.get(puta));
    assertNotNull("Results should contain row 'bbb'", results.get(putb));
    assertEquals("Invalid result for row 'bbb'", "pong", results.get(putb));
    assertNotNull("Results should contain row 'ccc'", results.get(putc));
    assertEquals("Invalid result for row 'ccc'", "pong", results.get(putc));
  }
}
