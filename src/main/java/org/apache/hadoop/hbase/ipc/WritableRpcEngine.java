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

import java.lang.reflect.Proxy;
import java.lang.reflect.Method;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.io.*;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.net.SocketFactory;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import com.google.common.collect.ClassToInstanceMap;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.MutableClassToInstanceMap;
import org.apache.commons.logging.*;

import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

/** An RpcEngine implementation for Writable data. */
class WritableRpcEngine implements RpcEngine {
  private static final Log LOG = LogFactory.getLog(HBaseRPC.class);

  /** A method invocation, including the method name and its parameters.*/
  private static class Invocation implements Writable, Configurable {
    private String methodName;
    @SuppressWarnings("unchecked")
    private Class[] parameterClasses;
    private Object[] parameters;
    private Configuration conf;

    public Invocation() {}

    public Invocation(Method method, Object[] parameters) {
      this.methodName = method.getName();
      this.parameterClasses = method.getParameterTypes();
      this.parameters = parameters;
    }

    /** @return The name of the method invoked. */
    public String getMethodName() { return methodName; }

    /** @return The parameter classes. */
    @SuppressWarnings("unchecked")
    public Class[] getParameterClasses() { return parameterClasses; }

    /** @return The parameter instances. */
    public Object[] getParameters() { return parameters; }

    public void readFields(DataInput in) throws IOException {
      methodName = in.readUTF();
      parameters = new Object[in.readInt()];
      parameterClasses = new Class[parameters.length];
      HbaseObjectWritable objectWritable = new HbaseObjectWritable();
      for (int i = 0; i < parameters.length; i++) {
        parameters[i] = HbaseObjectWritable.readObject(in, objectWritable,
          this.conf);
        parameterClasses[i] = objectWritable.getDeclaredClass();
      }
    }

    public void write(DataOutput out) throws IOException {
      out.writeUTF(this.methodName);
      out.writeInt(parameterClasses.length);
      for (int i = 0; i < parameterClasses.length; i++) {
        HbaseObjectWritable.writeObject(out, parameters[i], parameterClasses[i],
                                   conf);
      }
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder(256);
      buffer.append(methodName);
      buffer.append("(");
      for (int i = 0; i < parameters.length; i++) {
        if (i != 0)
          buffer.append(", ");
        buffer.append(parameters[i]);
      }
      buffer.append(")");
      return buffer.toString();
    }

    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    public Configuration getConf() {
      return this.conf;
    }

  }

  /* Cache a client using its socket factory as the hash key */
  static private class ClientCache {
    private Map<SocketFactory, HBaseClient> clients =
      new HashMap<SocketFactory, HBaseClient>();

    protected ClientCache() {}

    /**
     * Construct & cache an IPC client with the user-provided SocketFactory
     * if no cached client exists.
     *
     * @param conf Configuration
     * @param factory socket factory
     * @return an IPC client
     */
    protected synchronized HBaseClient getClient(Configuration conf,
        SocketFactory factory) {
      // Construct & cache client.  The configuration is only used for timeout,
      // and Clients have connection pools.  So we can either (a) lose some
      // connection pooling and leak sockets, or (b) use the same timeout for all
      // configurations.  Since the IPC is usually intended globally, not
      // per-job, we choose (a).
      HBaseClient client = clients.get(factory);
      if (client == null) {
        // Make an hbase client instead of hadoop Client.
        client = new HBaseClient(HbaseObjectWritable.class, conf, factory);
        clients.put(factory, client);
      } else {
        client.incCount();
      }
      return client;
    }

    /**
     * Construct & cache an IPC client with the default SocketFactory
     * if no cached client exists.
     *
     * @param conf Configuration
     * @return an IPC client
     */
    protected synchronized HBaseClient getClient(Configuration conf) {
      return getClient(conf, SocketFactory.getDefault());
    }

    /**
     * Stop a RPC client connection
     * A RPC client is closed only when its reference count becomes zero.
     * @param client client to stop
     */
    protected void stopClient(HBaseClient client) {
      synchronized (this) {
        client.decCount();
        if (client.isZeroReference()) {
          clients.remove(client.getSocketFactory());
        }
      }
      if (client.isZeroReference()) {
        client.stop();
      }
    }
  }

  protected final static ClientCache CLIENTS = new ClientCache();

  private static class Invoker implements InvocationHandler {
    private Class<? extends VersionedProtocol> protocol;
    private InetSocketAddress address;
    private UserGroupInformation ticket;
    private HBaseClient client;
    private boolean isClosed = false;

    public Invoker(Class<? extends VersionedProtocol> protocol,
                   InetSocketAddress address, UserGroupInformation ticket,
                   Configuration conf, SocketFactory factory) {
      this.protocol = protocol;
      this.address = address;
      this.ticket = ticket;
      this.client = CLIENTS.getClient(conf, factory);
    }

    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      final boolean logDebug = LOG.isDebugEnabled();
      long startTime = 0;
      if (logDebug) {
        startTime = System.currentTimeMillis();
      }

      HbaseObjectWritable value = (HbaseObjectWritable)
        client.call(new Invocation(method, args), address, 
                    protocol, ticket);
      if (logDebug) {
        long callTime = System.currentTimeMillis() - startTime;
        LOG.debug("Call: " + method.getName() + " " + callTime);
      }
      return value.get();
    }

    /* close the IPC client that's responsible for this invoker's RPCs */
    synchronized protected void close() {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
      }
    }
  }
  
  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. */
  public VersionedProtocol getProxy(
      Class<? extends VersionedProtocol> protocol, long clientVersion,
      InetSocketAddress addr, UserGroupInformation ticket,
      Configuration conf, SocketFactory factory)
    throws IOException {    

      VersionedProtocol proxy = 
          (VersionedProtocol) Proxy.newProxyInstance(
              protocol.getClassLoader(), new Class[] { protocol },
              new Invoker(protocol, addr, ticket, conf, factory));
    if (proxy instanceof VersionedProtocol) {
      long serverVersion = ((VersionedProtocol)proxy)
        .getProtocolVersion(protocol.getName(), clientVersion);
      if (serverVersion != clientVersion) {
        throw new HBaseRPC.VersionMismatch(protocol.getName(), clientVersion,
                                      serverVersion);
      }
    }
    return proxy;
  }

  /**
   * Stop this proxy and release its invoker's resource
   * @param proxy the proxy to be stopped
   */
  public void stopProxy(VersionedProtocol proxy) {
    if (proxy!=null) {
      ((Invoker)Proxy.getInvocationHandler(proxy)).close();
    }
  }

  
  /** Expert: Make multiple, parallel calls to a set of servers. */
  public Object[] call(Method method, Object[][] params,
                       InetSocketAddress[] addrs,
                       Class<? extends VersionedProtocol> protocol,
                       UserGroupInformation ticket, Configuration conf)
    throws IOException, InterruptedException {

    Invocation[] invocations = new Invocation[params.length];
    for (int i = 0; i < params.length; i++)
      invocations[i] = new Invocation(method, params[i]);
    HBaseClient client = CLIENTS.getClient(conf);
    try {
    Writable[] wrappedValues =
      client.call(invocations, addrs, protocol, ticket);
    
    if (method.getReturnType() == Void.TYPE) {
      return null;
    }

    Object[] values =
      (Object[])Array.newInstance(method.getReturnType(), wrappedValues.length);
    for (int i = 0; i < values.length; i++)
      if (wrappedValues[i] != null)
        values[i] = ((HbaseObjectWritable)wrappedValues[i]).get();

    return values;
    } finally {
      CLIENTS.stopClient(client);
    }
  }

  /** Construct a server for a protocol implementation instance listening on a
   * port and address. */
  public Server getServer(Class protocol,
                          Object instance, String bindAddress, int port,
                          int numHandlers, boolean verbose, Configuration conf) 
    throws IOException {
    return new Server(instance, conf, bindAddress, port, numHandlers, verbose);
  }

  /** An RPC Server. */
  public static class Server extends HBaseServer {
    private Object instance;
    private Class<?> implementation;
    private boolean verbose;
    private boolean authorize = false;

    /** Construct an RPC server.
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @throws IOException e
     */
    public Server(Object instance, Configuration conf, String bindAddress, int port) 
      throws IOException {
      this(instance, conf,  bindAddress, port, 1, false);
    }
    
    private static String classNameBase(String className) {
      String[] names = className.split("\\.", -1);
      if (names == null || names.length == 0) {
        return className;
      }
      return names[names.length-1];
    }
    
    /** Construct an RPC server.
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     * @throws IOException e
     */
    public Server(Object instance, Configuration conf, String bindAddress,  int port,
                  int numHandlers, boolean verbose) throws IOException {
      super(bindAddress, port, Invocation.class, numHandlers, conf, classNameBase(instance.getClass().getName()));
      this.instance = instance;
      this.implementation = instance.getClass();
      this.verbose = verbose;
      this.authorize = 
        conf.getBoolean(ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, 
                        false);
    }

    public Writable call(Class<? extends VersionedProtocol> protocol,
        Writable param, long receivedTime) 
    throws IOException {
      try {
        Invocation call = (Invocation)param;
        if(call.getMethodName() == null) {
          throw new IOException("Could not find requested method, the usual " +
              "cause is a version mismatch between client and server.");
        }
        if (verbose) log("Call: " + call);

        Method method =
          protocol.getMethod(call.getMethodName(),
                                   call.getParameterClasses());
        method.setAccessible(true);

        Object impl = null;
        if (protocol.isAssignableFrom(this.implementation)) {
          impl = this.instance;
        }
        else {
          throw new IOException("No matching implementation for protocol "+
              protocol.getName());
        }

        long startTime = System.currentTimeMillis();
        Object value = method.invoke(impl, call.getParameters());
        int processingTime = (int) (System.currentTimeMillis() - startTime);
        int qTime = (int) (startTime-receivedTime);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Served: " + call.getMethodName() +
                    " queueTime= " + qTime +
                    " procesingTime= " + processingTime);
        }
        rpcMetrics.rpcQueueTime.inc(qTime);
        rpcMetrics.rpcProcessingTime.inc(processingTime);
        rpcMetrics.inc(call.getMethodName(), processingTime);
        if (verbose) log("Return: "+value);

        return new HbaseObjectWritable(method.getReturnType(), value);

      } catch (InvocationTargetException e) {
        Throwable target = e.getTargetException();
        if (target instanceof IOException) {
          throw (IOException)target;
        }
        IOException ioe = new IOException(target.toString());
        ioe.setStackTrace(target.getStackTrace());
        throw ioe;
      } catch (Throwable e) {
        if (!(e instanceof IOException)) {
          LOG.error("Unexpected throwable object ", e);
        }
        IOException ioe = new IOException(e.toString());
        ioe.setStackTrace(e.getStackTrace());
        throw ioe;
      }
    }
  }

  protected static void log(String value) {
    String v = value;
    if (v != null && v.length() > 55)
      v = v.substring(0, 55)+"...";
    LOG.info(v);
  }
}
