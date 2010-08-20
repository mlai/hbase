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

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * Coprocessors are code that runs in-process on each region server. Regions
 * contain references to the coprocessor implementation classes associated
 * with them. Coprocessor classes will be loaded on demand either from local
 * jars on the region server's classpath or via the HDFS classloader.
 * <p>
 * While a coprocessor can have arbitrary function, it is required to
 * implement this interface.
 * <p>
 * The design goal of this interface is to provide simple features for
 * making coprocessors useful, while exposing no more internal state or
 * control actions of the region server than necessary and not exposing them
 * directly.
 * <p>
 * Over the lifecycle of a region, the methods of this interface are invoked
 * when the corresponding events happen. The master transitions regions
 * through the following states:<br>
 * &nbsp;&nbsp;&nbsp;
 * unassigned -> pendingOpen -> open -> pendingClose -> closed.<br>
 * Coprocessors have opportunity to intercept and handle events in
 * pendingOpen, open, and pendingClose states.
 * <p>
 * <dl>
 * <dt>PendingOpen</dt>
 * <dd>
 * <p>
 * The region server is opening a region to bring it online. Coprocessors
 * can piggyback or fail this process.
 * <p>
 * <ul>
 *   <li>onOpen: Called when the region is reported as online to the master.</li><p>
 * </ul>
 * <p>
 * </dd>
 * <dt>Open</dt>
 * <dd>
 * <p>
 * The region is open on the region server and is processing both client
 * requests (get, put, scan, etc.) and administrative actions (flush, compact,
 * split, etc.). Coprocessors can piggyback administrative actions via:
 * <p>
 * <ul>
 *   <li>onFlush: Called after the memstore is flushed into a new store file.</li><p>
 *   <li>onCompact: Called before and after compaction.</li><p>
 *   <li>onSplit: Called after the region is split but before the split is reported to the
 * master.</li><p>
 * </ul>
 * <p>
 * If the coprocessor implements the <tt>RegionObserver</tt> interface it can observe
 * and mediate client actions on the region:
 * <p>
 * <ul>
 *   <li>onGet: Called when a client makes a Get request.</li><p>
 *   <li>onGetResult: Called when the region server is ready to return a Get result.</li><p>
 *   <li>onExists: Called when the client tests for existence using a Get.</li><p>
 *   <li>onPut: Called when the client stores a value.</li><p>
 *   <li>onDelete: Called when the client deletes a value.</li><p>
 *   <li>onScannerOpen: Called when the client opens a new scanner.</li><p>
 *   <li>onScannerNext: Called when the client asks for the next row on a scanner.</li><p>
 *   <li>onScannerClose: Called when the client closes a scanner.</li><p>
 * </ul>
 * <p>
 * If the coprocessor implements the <tt>CommandTarget</tt> interface it can
 * support a generic ioctl-like command mechanism which the client side
 * library will wrap with Callable and Futures:
 * <p>
 * <ul>
 *   <li>onCommand: Invoked when the client wants to run a synchronous command
 *    on the coprocessor.</li><p>
 *   <li>onCommandAsyncSubmit: Invoked when the client wants to run an
 *   asynchronous command on the coprocessor.</li><p>
 *   <li>onCommandAsyncPoll: Invoked when the client polls asynchronous command
 *   results.</li><p>
 * </ul>
 * <p>
 * If the coprocessor implements the <tt>FilterInterface</tt> interface to
 * provide server side support for query filters for Gets and Scans:
 * <p>
 * <ul>
 *   <li>onReset: Reset the state of the filter between rows.</li><p>
 *   <li>onFilterRowKey: Filter a row based on the row key.</li><p>
 *   <li>onFilterAllRemaining: Called to provide opportunities to terminate
 *   the entire scan.</li><p>
 *   <li>onFilterKeyValue: Filter based on the column family, column qualifier
 *    and/or the column value.</li><p>
 *   <li>onFilterRow: Last chance to veto a row given results from earlier
 *   upcalls.</li><p>
 * </ul>
 * <p>
 * </dd>
 * <dt>PendingClose</dt>
 * <dd>
 * <p>
 * The region server is closing the region. This can happen as part of normal
 * operations or may happen when the region server is aborting due to fatal
 * conditions such as OOME, health check failure, or fatal filesystem
 * problems. Coprocessors can piggyback this event. If the server is aborting
 * an indication to this effect will be passed as an argument.
 * <p>
 * <ul>
 *   <li>onClose: Called when the region is reported as closed to the master.</li><p>
 * </ul>
 * <p>
 * </dd>
 * </dl>
 */
public interface Coprocessor {
  public static final int VERSION = 1;

  ///////////////////////////////////////////////////////////////////////////
  // Environment

  // Operational state

  public enum State {
    OPENING,
    OPEN,
    CLOSING
  }

  // Installation priority
  public enum Priority {

    HIGHEST(0),
    SYSTEM(Integer.MAX_VALUE/4),
    USER(Integer.MAX_VALUE/2),
    LOWEST(Integer.MAX_VALUE);

    private int prio;

    Priority(int prio) {
      this.prio = prio;
    }

    public int intValue() {
      return prio;
    }

  }

  /**
   * Coprocessor environment state.
   */
  public interface Environment {

    /** @return the Coprocessor interface version */
    public int getVersion();

    /** @return the HBase version as a string (e.g. "0.21.0") */
    public String getHBaseVersion();

    /** @return the region associated with this coprocessor */
    public HRegion getRegion();

    /**
     * @return an interface for accessing the given table
     * @throws IOException
     */
    public HTableInterface getTable(byte[] tableName) throws IOException;

    // environment variables

    /**
     * Get an environment variable
     * @param key the key
     * @return the object corresponding to the environment variable, if set
     */
    public Object get(Object key);

    /**
     * Set an environment variable
     * @param key the key
     * @param value the value
     */
    public void put(Object key, Object value);

    /**
     * Remove an environment variable
     * @param key the key
     * @return the object corresponding to the environment variable, if set
     */
    public Object remove(Object key);

  }

  // Interface

  /**
   * Called when the region is reported as open to the master.
   * @param e the environment provided by the region server
   */
  public void onOpen(final Environment e);

  /**
   * Called after the memstore is flushed to disk.
   * @param e the environment provided by the region server
   */
  public void onFlush(final Environment e);

  /**
   * Called before and after compaction.
   * @param e the environment provided by the region server
   * @param complete false before compaction, true after
   * @param willSplit true if compaction will result in a split, false
   * otherwise
   */
  public void onCompact(final Environment e, final boolean complete,
    final boolean willSplit);

  /**
   * Called after the region is split but before the split is reported to the
   * master.
   * @param e the environment provided by the region server
   * (e.getRegion() returns the parent region)
   * @param l the left daughter region
   * @param r the right daughter region
   */
  public void onSplit(final Environment e, final HRegion l, final HRegion r);

  /**
   * Called when the region is reported as closed to the master.
   * @param e the environment provided by the region server
   * @param abortRequested true if the region server is aborting
   */
  public void onClose(final Environment e, boolean abortRequested);

}
