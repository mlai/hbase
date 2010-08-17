/**
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
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;

import javax.net.SocketFactory;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.*;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;

/** A simple RPC mechanism.
 *
 * This is a local hbase copy of the hadoop RPC so we can do things like
 * address HADOOP-414 for hbase-only and try other hbase-specific
 * optimizations like using our own version of ObjectWritable.  Class has been
 * renamed to avoid confusing it w/ hadoop versions.
 * <p>
 *
 *
 * A <i>protocol</i> is a Java interface.  All parameters and return types must
 * be one of:
 *
 * <ul> <li>a primitive type, <code>boolean</code>, <code>byte</code>,
 * <code>char</code>, <code>short</code>, <code>int</code>, <code>long</code>,
 * <code>float</code>, <code>double</code>, or <code>void</code>; or</li>
 *
 * <li>a {@link String}; or</li>
 *
 * <li>a {@link org.apache.hadoop.io.Writable}; or</li>
 *
 * <li>an array of the above types</li> </ul>
 *
 * All methods in the protocol should throw only IOException.  No field data of
 * the protocol instance is transmitted.
 */
public class SecureRpcEngine implements RpcEngine {
  // Leave this out in the hadoop ipc package but keep class name.  Do this
  // so that we dont' get the logging of this class's invocations by doing our
  // blanket enabling DEBUG on the o.a.h.h. package.
  protected static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.ipc.HbaseRPC");

  private SecureRpcEngine() {
    super();
  }                                  // no public ctor

  /** A method invocation, including the method name and its parameters.*/
  private static class Invocation implements Writable, Configurable {
    private String methodName;
    @SuppressWarnings("unchecked")
    private Class[] parameterClasses;
    private Object[] parameters;
    private Configuration conf;

    /** default constructor */
    public Invocation() {
      super();
    }

    /**
     * @param method method to call
     * @param parameters parameters of call
     */
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
    private Map<SocketFactory, SecureClient> clients =
      new HashMap<SocketFactory, SecureClient>();

    protected ClientCache() {}

    /**
     * Construct & cache an IPC client with the user-provided SocketFactory
     * if no cached client exists.
     *
     * @param conf Configuration
     * @param factory socket factory
     * @return an IPC client
     */
    protected synchronized SecureClient getClient(Configuration conf,
        SocketFactory factory) {
      // Construct & cache client.  The configuration is only used for timeout,
      // and Clients have connection pools.  So we can either (a) lose some
      // connection pooling and leak sockets, or (b) use the same timeout for all
      // configurations.  Since the IPC is usually intended globally, not
      // per-job, we choose (a).
      SecureClient client = clients.get(factory);
      if (client == null) {
        // Make an hbase client instead of hadoop Client.
        client = new SecureClient(HbaseObjectWritable.class, conf, factory);
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
    protected synchronized SecureClient getClient(Configuration conf) {
      return getClient(conf, SocketFactory.getDefault());
    }

    /**
     * Stop a RPC client connection
     * A RPC client is closed only when its reference count becomes zero.
     * @param client client to stop
     */
    protected void stopClient(SecureClient client) {
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
    private SecureClient client;
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
		  client.call(new Invocation(method, args), address, protocol, ticket);
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

  /**
   * A version mismatch for the RPC protocol.
   */
  @SuppressWarnings("serial")
  public static class VersionMismatch extends IOException {
    private String interfaceName;
    private long clientVersion;
    private long serverVersion;

    /**
     * Create a version mismatch exception
     * @param interfaceName the name of the protocol mismatch
     * @param clientVersion the client's version of the protocol
     * @param serverVersion the server's version of the protocol
     */
    public VersionMismatch(String interfaceName, long clientVersion,
                           long serverVersion) {
      super("Protocol " + interfaceName + " version mismatch. (client = " +
            clientVersion + ", server = " + serverVersion + ")");
      this.interfaceName = interfaceName;
      this.clientVersion = clientVersion;
      this.serverVersion = serverVersion;
    }

    /**
     * Get the interface name
     * @return the java class name
     *          (eg. org.apache.hadoop.mapred.InterTrackerProtocol)
     */
    public String getInterfaceName() {
      return interfaceName;
    }

    /**
     * @return the client's preferred version
     */
    public long getClientVersion() {
      return clientVersion;
    }

    /**
     * @return the server's agreed to version.
     */
    public long getServerVersion() {
      return serverVersion;
    }
  }

  /**
   * Get a proxy connection to a remote server
   * @param protocol protocol interface
   * @param clientVersion which client version we expect
   * @param addr address of remote service
   * @param conf configuration
   * @param maxAttempts max attempts
   * @param timeout timeout in milliseconds
   * @return proxy
   * @throws java.io.IOException e
   */
  @SuppressWarnings("unchecked")
  public VersionedProtocol waitForProxy(Class<? extends VersionedProtocol> protocol,
                                               long clientVersion,
                                               InetSocketAddress addr,
                                               Configuration conf,
                                               int maxAttempts,
                                               long timeout
                                               ) throws IOException {
    // HBase does limited number of reconnects which is different from hadoop.
    long startTime = System.currentTimeMillis();
    IOException ioe;
    int reconnectAttempts = 0;
    while (true) {
      try {
        return getProxy(protocol, clientVersion, addr, conf);
      } catch(ConnectException se) {  // namenode has not been started
        ioe = se;
        if (maxAttempts >= 0 && ++reconnectAttempts >= maxAttempts) {
          LOG.info("Server at " + addr + " could not be reached after " +
            reconnectAttempts + " tries, giving up.");
          throw new RetriesExhaustedException("Failed setting up proxy to " +
            addr.toString() + " after attempts=" + reconnectAttempts);
        }
      } catch(SocketTimeoutException te) {  // namenode is busy
        LOG.info("Problem connecting to server: " + addr);
        ioe = te;
      }
      // check if timed out
      if (System.currentTimeMillis()-timeout >= startTime) {
        throw ioe;
      }

      // wait for retry
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        // IGNORE
      }
    }
  }

  /**
   * Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address.
   *
   * @param protocol interface
   * @param clientVersion version we are expecting
   * @param addr remote address
   * @param conf configuration
   * @param factory socket factory
   * @return proxy
   * @throws java.io.IOException e
   */
  public VersionedProtocol getProxy(Class<? extends VersionedProtocol> protocol,
      long clientVersion, InetSocketAddress addr, Configuration conf,
      SocketFactory factory) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    return getProxy(protocol, clientVersion, addr, ugi, conf, factory);
  }

  /**
   * Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address.
   *
   * @param protocol interface
   * @param clientVersion version we are expecting
   * @param addr remote address
   * @param ticket ticket
   * @param conf configuration
   * @param factory socket factory
   * @return proxy
   * @throws java.io.IOException e
   */
  public VersionedProtocol getProxy(Class<? extends VersionedProtocol> protocol,
      long clientVersion, InetSocketAddress addr, UserGroupInformation ticket,
      Configuration conf, SocketFactory factory)
  throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      HBaseSaslRpcServer.init(conf);
    }
    VersionedProtocol proxy =
        (VersionedProtocol) Proxy.newProxyInstance(
            protocol.getClassLoader(), new Class[] { protocol },
            new Invoker(protocol, addr, ticket, conf, factory));
    long serverVersion = proxy.getProtocolVersion(protocol.getName(),
                                                  clientVersion);
    if (serverVersion == clientVersion) {
      return proxy;
    } else {
      throw new VersionMismatch(protocol.getName(), clientVersion,
                                serverVersion);
    }
  }

  /**
   * Construct a client-side proxy object with the default SocketFactory
   *
   * @param protocol interface
   * @param clientVersion version we are expecting
   * @param addr remote address
   * @param conf configuration
   * @return a proxy instance
   * @throws java.io.IOException e
   */
  public VersionedProtocol getProxy(
      Class<? extends VersionedProtocol> protocol,
      long clientVersion, InetSocketAddress addr, Configuration conf)
      throws IOException {

    return getProxy(protocol, clientVersion, addr, conf,
        NetUtils.getDefaultSocketFactory(conf));
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

  /**
   * Expert: Make multiple, parallel calls to a set of servers.
   * @deprecated Use {@link #call(java.lang.reflect.Method, Object[][], java.net.InetSocketAddress[], org.apache.hadoop.security.UserGroupInformation, org.apache.hadoop.conf.Configuration)} instead
   */
  public Object[] call(Method method, Object[][] params,
                              InetSocketAddress[] addrs, Configuration conf)
    throws IOException, InterruptedException {
    return call(method, params, addrs, null, conf);
  }

  /** Expert: Make multiple, parallel calls to a set of servers. */
  public Object[] call(Method method, Object[][] params,
                              InetSocketAddress[] addrs,
                              UserGroupInformation ticket, Configuration conf)
    throws IOException, InterruptedException {

    Invocation[] invocations = new Invocation[params.length];
    for (int i = 0; i < params.length; i++)
      invocations[i] = new Invocation(method, params[i]);
    SecureClient client = CLIENTS.getClient(conf);
    try {
      Writable[] wrappedValues =
          client.call(invocations, addrs, (Class)method.getDeclaringClass(), ticket);

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

  /**
   * Construct a server for a protocol implementation instance listening on a
   * port and address.
   *
   * @param instance instance
   * @param bindAddress bind address
   * @param port port to bind to
   * @param conf configuration
   * @return Server
   * @throws java.io.IOException e
   */
  public Server getServer(Class<? extends VersionedProtocol> protocol,
      final Object instance, final String bindAddress,
      final int port, Configuration conf)
    throws IOException {
    return getServer(protocol, instance, bindAddress, port, 1, false, conf);
  }

  /**
   * Construct a server for a protocol implementation instance listening on a
   * port and address.
   *
   * @param instance instance
   * @param bindAddress bind address
   * @param port port to bind to
   * @param numHandlers number of handlers to start
   * @param verbose verbose flag
   * @param conf configuration
   * @return Server
   * @throws java.io.IOException e
   */
  public Server getServer(Class<? extends VersionedProtocol> protocol,
      final Object instance, final String bindAddress, final int port,
      final int numHandlers,
      final boolean verbose, Configuration conf)
    throws IOException {
    return getServer(protocol, instance, bindAddress, port, numHandlers, verbose, conf, null);
  }

  /** Construct a server for a protocol implementation instance listening on a
   * port and address, with a secret manager. */
  public static Server getServer(Class<? extends VersionedProtocol> protocol,
      final Object instance, final String bindAddress, final int port,
      final int numHandlers,
      final boolean verbose, Configuration conf,
      SecretManager<? extends TokenIdentifier> secretManager)
    throws IOException {
    return
        new Server(instance, conf, bindAddress, port,
            numHandlers, verbose, secretManager);
  }

  /** An RPC Server. */
  public static class Server extends SecureServer {
    private Object instance;
    private Class<?> implementation;
    private boolean verbose;

    /**
     * Construct an RPC server.
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @throws java.io.IOException e
     */
    public Server(Object instance, Configuration conf, String bindAddress, int port)
      throws IOException {
      this(instance, conf,  bindAddress, port, 1, false, null);
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
     * @throws java.io.IOException e
     */
    public Server(Object instance, Configuration conf, String bindAddress,  int port,
                  int numHandlers, boolean verbose,
                  SecretManager<? extends TokenIdentifier> secretManager)
        throws IOException {
      super(bindAddress, port, Invocation.class, numHandlers, conf,
          classNameBase(instance.getClass().getName()), secretManager);
      this.instance = instance;
      this.implementation = instance.getClass();
      this.verbose = verbose;
    }

    @Override
    public Writable call(Class<? extends VersionedProtocol> protocol, Writable param, long receivedTime) throws IOException {
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

        long startTime = System.currentTimeMillis();
        Object value = method.invoke(instance, call.getParameters());
        int processingTime = (int) (System.currentTimeMillis() - startTime);
        int qTime = (int) (startTime-receivedTime);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Served: " + call.getMethodName() +
            " queueTime= " + qTime +
            " procesingTime= " + processingTime);
        }
        rpcMetrics.rpcQueueTime.inc(qTime);
        rpcMetrics.rpcProcessingTime.inc(processingTime);

        MetricsTimeVaryingRate m =
         (MetricsTimeVaryingRate) rpcDetailedMetrics.registry.get(call.getMethodName());
        if (m == null) {
          try {
            m = new MetricsTimeVaryingRate(call.getMethodName(),
                rpcDetailedMetrics.registry);
          } catch (IllegalArgumentException iae) {
            // the metrics has been registered; re-fetch the handle
            LOG.info("Error register " + call.getMethodName(), iae);
            m = (MetricsTimeVaryingRate) rpcDetailedMetrics.registry.get(
                call.getMethodName());
          }
        }
        m.inc(processingTime);

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