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
package org.apache.hadoop.hbase.ipc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class ExecRPCInvoker implements InvocationHandler {
  private static Log LOG = LogFactory.getLog(ExecRPCInvoker.class);

  private Configuration conf;
  private final HConnection connection;
  private Class<? extends CoprocessorProtocol> protocol;
  private final byte[] table;
  private final Row row;
  private byte[] regionName;

  public ExecRPCInvoker(Configuration conf,
      HConnection connection,
      Class<? extends CoprocessorProtocol> protocol,
      byte[] table,
      Row row) {
    this.conf = conf;
    this.connection = connection;
    this.protocol = protocol;
    this.table = table;
    this.row = row;
  }

  @Override
  public Object invoke(Object instance, final Method method, final Object[] args)
      throws Throwable {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Call: "+method.getName()+", "+(args != null ? args.length : 0));
    }

    if (row != null) {
      try {
        final Exec exec = new Exec(conf, row.getRow(), protocol, method, args);
        ServerCallable<ExecResult> callable =
            new ServerCallable<ExecResult>(connection, table, row.getRow()) {
              public ExecResult call() throws Exception {
                return server.regionExec(location.getRegionInfo().getRegionName(),
                    exec);
              }
            };
        ExecResult result = connection.getRegionServerWithRetries(callable);
        this.regionName = result.getRegionName();
        LOG.debug("Result is region="+ Bytes.toStringBinary(regionName) +
            ", value="+result.getValue());
        return result.getValue();
      } catch (IOException ioe) {
        LOG.error("Error invoking "+method.getName(), ioe);
      }
    }

    return null;
  }

  public byte[] getRegionName() {
    return regionName;
  }
}
