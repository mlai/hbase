package org.apache.hadoop.hbase.client;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.List;

public class ProxyRPCInvoker implements InvocationHandler {
  private static Log LOG = LogFactory.getLog(ProxyRPCInvoker.class);

  private Configuration conf;
  private Class<? extends VersionedProtocol> protocol;
  private byte[] table;
  private List<? extends Row> rows;

  public ProxyRPCInvoker(Configuration conf,
      Class<? extends VersionedProtocol> protocol,
      byte[] table, Row row) {
    this(conf, protocol, table, Lists.newArrayList(row));
  }

  /*
  public ProxyRPCInvoker(RowRange range) {

  }
  */

  public ProxyRPCInvoker(Configuration conf,
      Class<? extends VersionedProtocol> protocol,
      byte[] table, List<? extends Row> rows) {
    this.conf = conf;
    this.protocol = protocol;
    this.table = table;
    this.rows = rows;
  }

  private InetSocketAddress[] getRowLocations()
      throws ZooKeeperConnectionException {
    final HConnection conn = HConnectionManager.getConnection(conf);
    List<InetSocketAddress> locations = Lists.transform(rows,
        new Function<Row,InetSocketAddress>() {
          public InetSocketAddress apply(Row r) {
            try {
              return conn.getRegionLocation(table, r.getRow(), true)
                  .getServerAddress().getInetSocketAddress();
            }
            catch (IOException ioe) {
              LOG.warn("Failed retrieving row location: "+
                  Bytes.toStringBinary(r.getRow()), ioe);
              return null;
            }
          }
        });

    return locations.toArray(new InetSocketAddress[0]);
  }

  @Override
  public Object invoke(Object instance, Method method, Object[] args)
      throws Throwable {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Call: "+method.getName()+", "+(args != null ? args.length : 0));
    }

    InetSocketAddress[] locs = getRowLocations();
    if (locs != null) {
      Object[] results = HBaseRPC.call(method, new Object[][]{args}, locs,
          protocol, null, conf);

      if (results != null && results.length > 0) {
        return results[0];
      }
    }
    return null;
  }
}
