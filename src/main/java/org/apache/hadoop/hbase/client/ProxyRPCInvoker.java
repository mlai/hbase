package org.apache.hadoop.hbase.client;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class ProxyRPCInvoker<R> implements InvocationHandler {
  private static Log LOG = LogFactory.getLog(ProxyRPCInvoker.class);

  private Configuration conf;
  private HConnection connection;
  private Class<? extends CoprocessorProtocol> protocol;
  private byte[] table;
  private List<? extends Row> rows;
  private ExecutorService pool;
  private HTable.BatchCallback<R> callback;

  public ProxyRPCInvoker(Configuration conf,
      HConnection connection,
      Class<? extends CoprocessorProtocol> protocol,
      byte[] table,
      Row row,
      ExecutorService pool,
      HTable.BatchCallback<R> callback) {
    this(conf, connection, protocol, table, Lists.newArrayList(row), pool, callback);
  }

  /*
  public ProxyRPCInvoker(RowRange range) {

  }
  */

  public ProxyRPCInvoker(Configuration conf,
      HConnection connection,
      Class<? extends CoprocessorProtocol> protocol,
      byte[] table,
      List<? extends Row> rows,
      ExecutorService pool,
      HTable.BatchCallback<R> callback) {
    this.conf = conf;
    this.connection = connection;
    this.protocol = protocol;
    this.table = table;
    this.rows = rows;
    this.pool = pool;
    this.callback = callback;
  }

  @Override
  public Object invoke(Object instance, final Method method, final Object[] args)
      throws Throwable {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Call: "+method.getName()+", "+(args != null ? args.length : 0));
    }

    if (rows != null) {
      List<Exec> execs = Lists.transform(rows,
          new Function<Row,Exec>() {
            public Exec apply(Row r) {
              return new Exec(conf, r.getRow(), protocol, method, args);
            }
          });
      R[] results = connection.processBatchCallback(execs, table, pool, callback);
      if (results != null && results.length > 0) {
        return results[0];
      }
    }

    return null;
  }
}
