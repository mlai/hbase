/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.Coprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implements the coprocessor environment and runtime support.
 */
public class CoprocessorHost {

    /**
   * Environment priority comparator.
   * Coprocessors are chained in sorted order.
   */
  static class EnvironmentPriorityComparator implements Comparator<Environment> {
    public int compare(Environment env1, Environment env2) {
      if (env1.priority.intValue() < env2.priority.intValue()) {
        return -1;
      } else if (env1.priority.intValue() > env2.priority.intValue()) {
        return 1;
      }
      return 0;
    }
  }

  /**
   * Encapsulation of the environment of each coprocessor
   */
  class Environment implements Coprocessor.Environment {

    /**
     * A wrapper for HTable. Can be used to restrict privilege.
     *
     * Currently it just helps to track tables opened by a Coprocessor and
     * facilitate close of them if it is aborted.
     *
     * We also disallow row locking.
     *
     * There is nothing now that will stop a coprocessor from using HTable
     * objects directly instead of this API, but in the future we intend to
     * analyze coprocessor implementations as they are loaded and reject those
     * which attempt to use objects and methods outside the Environment
     * sandbox.
     */
    class HTableWrapper implements HTableInterface {

      private byte[] tableName;
      private HTable table;

      public HTableWrapper(byte[] tableName) throws IOException {
        this.tableName = tableName;
        this.table = new HTable(tableName);
        openTables.add(this);
      }

      void internalClose() throws IOException {
        table.close();
      }

      public Configuration getConfiguration() {
        return table.getConfiguration();
      }

      public void close() throws IOException {
        try {
          internalClose();
        } finally {
          openTables.remove(this);
        }
      }

      public Result getRowOrBefore(byte[] row, byte[] family)
          throws IOException {
        return table.getRowOrBefore(row, family);
      }

      public Result get(Get get) throws IOException {
        return table.get(get);
      }

      public boolean exists(Get get) throws IOException {
        return table.exists(get);
      }

      public void put(Put put) throws IOException {
        table.put(put);
      }

      public void put(List<Put> puts) throws IOException {
        table.put(puts);
      }

      public void delete(Delete delete) throws IOException {
        table.delete(delete);
      }

      public void delete(List<Delete> deletes) throws IOException {
        table.delete(deletes);
      }

      public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
          byte[] value, Put put) throws IOException {
        return table.checkAndPut(row, family, qualifier, value, put);
      }

      public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
          byte[] value, Delete delete) throws IOException {
        return table.checkAndDelete(row, family, qualifier, value, delete);
      }

      public long incrementColumnValue(byte[] row, byte[] family,
          byte[] qualifier, long amount) throws IOException {
        return table.incrementColumnValue(row, family, qualifier, amount);
      }

      public long incrementColumnValue(byte[] row, byte[] family,
          byte[] qualifier, long amount, boolean writeToWAL)
          throws IOException {
        return table.incrementColumnValue(row, family, qualifier, amount,
          writeToWAL);
      }

      public void flushCommits() throws IOException {
        table.flushCommits();
      }

      public boolean isAutoFlush() {
        return table.isAutoFlush();
      }

      public ResultScanner getScanner(Scan scan) throws IOException {
        return table.getScanner(scan);
      }

      public ResultScanner getScanner(byte[] family) throws IOException {
        return table.getScanner(family);
      }

      public ResultScanner getScanner(byte[] family, byte[] qualifier)
          throws IOException {
        return table.getScanner(family, qualifier);
      }

      public HTableDescriptor getTableDescriptor() throws IOException {
        return table.getTableDescriptor();
      }

      public byte[] getTableName() {
        return tableName;
      }

      public RowLock lockRow(byte[] row) throws IOException {
        throw new RuntimeException(
          "row locking is not allowed within the coprocessor environment");
      }

      public void unlockRow(RowLock rl) throws IOException {
        throw new RuntimeException(
          "row locking is not allowed within the coprocessor environment");
      }
    }

    /** The coprocessor */
    Coprocessor impl;
    /** Environment variables */
    Map<Object,Object> vars = new ConcurrentHashMap<Object,Object>();
    /** Chaining priority */
    Coprocessor.Priority priority = Coprocessor.Priority.USER;
    /** Accounting for tables opened by the coprocessor */
    List<HTableInterface> openTables =
      Collections.synchronizedList(new ArrayList<HTableInterface>());

    /**
     * Constructor
     * @param impl the coprocessor instance
     * @param priority chaining priority
     */
    public Environment(final Coprocessor impl, Coprocessor.Priority priority) {
      this.impl = impl;
      this.priority = priority;
    }

    /** Clean up the environment */
    void shutdown() {
      // clean up any table references
      for (HTableInterface table: openTables) {
        try {
          ((HTableWrapper)table).internalClose();
        } catch (IOException e) {
          // nothing can be done here
          LOG.warn(StringUtils.stringifyException(e));
        }
      }
    }

    /** @return the coprocessor environment version */
    public int getVersion() {
      return Coprocessor.VERSION;
    }

    /** @return the HBase release */
    public String getHBaseVersion() {
      return VersionInfo.getVersion();
    }

    /** @return the region */
    public HRegion getRegion() {
      return region;
    }

    /**
     * Open a table from within the Coprocessor environment
     * @param tableName the table name
     * @return an interface for manipulating the table
     * @throws IOException
     */
    public HTableInterface getTable(byte[] tableName) throws IOException {
      // TODO: Access check
      return new HTableWrapper(tableName);
    }

    /**
     * @param key the key
     * @return the value, or null if it does not exist
     */
    public Object get(Object key) {
      return vars.get(key);
    }

    /**
     * @param key the key
     * @param value the value
     */
    public void put(Object key, Object value) {
      vars.put(key, value);
    }

    /**
     * @param key the key
     */
    public Object remove(Object key) {
      return vars.remove(key);
    }
  }

  static final Log LOG = LogFactory.getLog(CoprocessorHost.class);
  static final Pattern attrSpecMatch = Pattern.compile("(.+):(.+):(.+)");

  /** The region */
  HRegion region;
  /** Ordered set of loaded coprocessors with lock */
  final ReentrantReadWriteLock coprocessorLock = new ReentrantReadWriteLock();
  Set<Environment> coprocessors =
    new TreeSet<Environment>(new EnvironmentPriorityComparator());

  /**
   * Constructor
   * @param region the region
   */
  public CoprocessorHost(HRegion region) {
    this.region = region;
  }

  /**
   * Load a coprocessor implementation into the host
   * @param path path to implementation jar
   * @param className the main class name
   * @param priority chaining priority
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  public void load(Path path, String className, Coprocessor.Priority priority)
      throws IOException {
    Class<?> implClass = null;

    // Have we already loaded the class, perhaps from an earlier region open
    // for the same table?
    try {
      implClass = getClass().getClassLoader().loadClass(className);
    } catch (ClassNotFoundException e) {
      // ignore
    }

    // If not, load
    if (implClass == null) {
      // copy the jar to the local filesystem
      if (!path.toString().endsWith(".jar")) {
        throw new IOException(path.toString() + ": not a jar file?");
      }
      FileSystem fs = path.getFileSystem(HBaseConfiguration.create());
      Path dst = new Path("/tmp/." +
        region.getRegionNameAsString().replace(',', '_') +
        "." + className + "." + System.currentTimeMillis() + ".jar");
      fs.copyToLocalFile(path, dst);
      fs.deleteOnExit(dst);

      // TODO: code weaving goes here

        // TODO: wrap heap allocations and enforce maximum usage limits

        /* TODO: inject code into loop headers that monitors CPU use and
           aborts runaway user code */

      // load the jar and get the implementation main class
      String cp = System.getProperty("java.class.path");
      // NOTE: Path.toURL is deprecated (toURI instead) but the URLClassLoader
      // unsuprisingly wants URLs, not URIs; so we will use the deprecated
      // method which returns URLs for as long as it is available
      List<URL> paths = new ArrayList<URL>();
      paths.add(new File(dst.toString()).getCanonicalFile().toURL());
      StringTokenizer st = new StringTokenizer(cp, File.pathSeparator);
      while (st.hasMoreTokens()) {
        paths.add((new File(st.nextToken())).getCanonicalFile().toURL());
      }
      ClassLoader cl = new URLClassLoader(paths.toArray(new URL[]{}),
        ClassLoader.getSystemClassLoader());
      Thread.currentThread().setContextClassLoader(cl);
      try {
        implClass = cl.loadClass(className);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }

    load(implClass, priority);
  }

  public void load(Class<?> implClass, Coprocessor.Priority priority)
      throws IOException {
    // create the instance
    Coprocessor impl;
    try {
      Object o = implClass.newInstance();
      impl = (Coprocessor)o;
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
    // create the environment
    Environment env = new Environment(impl, priority);
    try {
      coprocessorLock.writeLock().lock();
      coprocessors.add(env);
    } finally {
      coprocessorLock.writeLock().unlock();
    }
  }

  /**
   * Find a coprocessor implementation by class name
   * @param className the class name
   * @return the coprocessor, or null if not found
   */
  public Coprocessor findCoprocessor(String className) {
    // initialize the coprocessors
    try {
      coprocessorLock.readLock().lock();
      for (Environment env: coprocessors) {
        if (env.impl.getClass().getName().equals(className)) {
          return env.impl;
        }
      }
      for (Environment env: coprocessors) {
        if (env.impl.getClass().getName().endsWith(className)) {
          return env.impl;
        }
      }
      return null;
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * Invoked to handle a region open
   */
  public void onOpen() {
    // scan the table attributes for coprocessor load specifications
    for (Map.Entry<ImmutableBytesWritable,ImmutableBytesWritable> e:
        region.getTableDesc().getValues().entrySet()) {
      String key = Bytes.toString(e.getKey().get());
      if (key.startsWith("Coprocessor")) {
        // found one
        try {
          String spec = Bytes.toString(e.getValue().get());
          Matcher matcher = attrSpecMatch.matcher(spec);
          if (matcher.matches()) {
            Path path = new Path(matcher.group(1));
            String className = matcher.group(2);
            Coprocessor.Priority priority =
              Coprocessor.Priority.valueOf(matcher.group(3));
            load(path, className, priority);
          } else {
            LOG.warn("attribute '" + key + "' has invalid coprocessor spec");
          }
        } catch (IOException ex) {
            LOG.warn(StringUtils.stringifyException(ex));
        }
      }
    }
    // initialize the coprocessors
    try {
      coprocessorLock.readLock().lock();
      for (Environment env: coprocessors) {
        env.impl.onOpen(env);
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * Invoked when a region is closed
   * @param abortRequested true if the server is aborting
   */
  public void onClose(boolean abortRequested) {
    try {
      coprocessorLock.writeLock().lock();
      for (Environment env: coprocessors) {
        env.impl.onClose(env, abortRequested);
        env.shutdown();
      }
    } finally {
      coprocessorLock.writeLock().unlock();
    }
  }

  /**
   * Invoked before and after a region is compacted.
   * @param complete false if the region is about to be compacted, true if
   * compaction has just completed
   * @param willSplit true if the compaction is about to trigger a split
   */
  public void onCompact(boolean complete, boolean willSplit) {
    try {
      coprocessorLock.readLock().lock();
      for (Environment env: coprocessors) {
        env.impl.onCompact(env, complete, willSplit);
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * Invoked after a memstore flush
   */
  public void onFlush() {
    try {
      coprocessorLock.readLock().lock();
      for (Environment env: coprocessors) {
        env.impl.onFlush(env);
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * Invoked just after a split
   * @param l the new left-hand daughter region
   * @param r the new right-hand daughter region
   */
  public void onSplit(HRegion l, HRegion r) {
    try {
      coprocessorLock.readLock().lock();
      for (Environment env: coprocessors) {
        env.impl.onSplit(env, l, r);
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  // RegionObserver support

  /**
   * @param row the row key
   * @param family the family
   * @param result the result set from the region
   * @return the result set to return to the client
   */
  public Result onGetClosestRowBefore(final byte[] row, final byte[] family,
      Result result) {
    try {
      coprocessorLock.readLock().lock();
      for (Environment env: coprocessors) {
        if (env.impl instanceof RegionObserver) {
          result = ((RegionObserver)env.impl)
            .onGetClosestRowBefore(env, row, family, result);
        }
      }
      return result;
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param get the Get request
   * @param results the result set
   * @return the possibly transformed result set to use
   */
  public List<KeyValue> onGet(final Get get, List<KeyValue> results) {
    try {
      coprocessorLock.readLock().lock();
      for (Environment env: coprocessors) {
        if (env.impl instanceof RegionObserver) {
          results = ((RegionObserver)env.impl).onGet(env, get, results);
        }
      }
      return results;
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param get the Get request
   * @param exists the result returned by the region server
   * @return the result to return to the client
   */
  public boolean onExists(final Get get, boolean exists) {
    try {
      coprocessorLock.readLock().lock();
      for (Environment env: coprocessors) {
        if (env.impl instanceof RegionObserver) {
          exists = ((RegionObserver)env.impl).onExists(env, get, exists);
        }
      }
      return exists;
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param familyMap map of family to edits for the given family.
   * @return the possibly transformed map to actually use
   */
  public Map<byte[], List<KeyValue>> onPut(Map<byte[], List<KeyValue>> familyMap) {
    try {
      coprocessorLock.readLock().lock();
      for (Environment env: coprocessors) {
        if (env.impl instanceof RegionObserver) {
          familyMap = ((RegionObserver)env.impl).onPut(env, familyMap);
        }
      }
      return familyMap;
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param kv KeyValue to store
   * @return the possibly transformed KeyValue to actually use
   */
  public KeyValue onPut(KeyValue kv) {
    try {
      coprocessorLock.readLock().lock();
      for (Environment env: coprocessors) {
        if (env.impl instanceof RegionObserver) {
          kv = ((RegionObserver)env.impl).onPut(env, kv);
        }
      }
      return kv;
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param familyMap map of family to edits for the given family.
   * @return the possibly transformed map to actually use
   */
  public Map<byte[], List<KeyValue>> onDelete(Map<byte[], List<KeyValue>> familyMap) {
    try {
      coprocessorLock.readLock().lock();
      for (Environment env: coprocessors) {
        if (env.impl instanceof RegionObserver) {
          familyMap = ((RegionObserver)env.impl).onDelete(env, familyMap);
        }
      }
      return familyMap;
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param scan the Scan specification
   * @param scannerId the scanner id allocated by the region server
   */
  public void onScannerOpen(final Scan scan, long scannerId) {
    try {
      coprocessorLock.readLock().lock();
      for (Environment env: coprocessors) {
        if (env.impl instanceof RegionObserver) {
          ((RegionObserver)env.impl).onScannerOpen(env, scan, scannerId);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param scannerId the scanner id
   * @param results the result set returned by the region server
   * @return the possibly transformed result set to actually return
   */
  public List<KeyValue> onScannerNext(final long scannerId,
      List<KeyValue> results) {
    try {
      coprocessorLock.readLock().lock();
      for (Environment env: coprocessors) {
        if (env.impl instanceof RegionObserver) {
          results = ((RegionObserver)env.impl).onScannerNext(env, scannerId,
            results);
        }
      }
      return results;
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param scannerId the scanner id
   */
  public void onScannerClose(final long scannerId) {
    try {
      coprocessorLock.readLock().lock();
      for (Environment env: coprocessors) {
        if (env.impl instanceof RegionObserver) {
          ((RegionObserver)env.impl).onScannerClose(env, scannerId);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }
}
