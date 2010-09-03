package org.apache.hadoop.hbase.client;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.List;

public class ProxyRPCInvoker implements InvocationHandler {
  private Configuration conf;
  private Class<? extends VersionedProtocol> protocol;
  private byte[] table;
  private List<? extends Row> rows;

  public ProxyRPCInvoker(Class<? extends VersionedProtocol> protocol,
      byte[] table, Row row) {
    this(protocol, table, Lists.newArrayList(row));
  }

  /*
  public ProxyRPCInvoker(RowRange range) {

  }
  */

  public ProxyRPCInvoker(Class<? extends VersionedProtocol> protocol,
      byte[] table, List<? extends Row> rows) {
    this.protocol = protocol;
    this.table = table;
    this.rows = rows;
    this.conf = HBaseConfiguration.create();
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
              return null;
            }
          }
        });

    return locations.toArray(new InetSocketAddress[0]);
  }

  @Override
  public Object invoke(Object instance, Method method, Object[] args)
      throws Throwable {

    InetSocketAddress[] locs = getRowLocations();
    // is it weird to have a proxy wrapping a proxy?  yes.
    // but rpc code doesn't make it easy to replace the inner proxy here
    if (locs != null) {
      Object[] results = HBaseRPC.call(method, new Object[][]{args}, locs,
          protocol, UserGroupInformation.getCurrentUGI(), conf);

      if (results != null && results.length > 0) {
        return results[0];
      }
    }
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
