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

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.Coprocessor;
import org.apache.hadoop.hbase.coprocessor.Coprocessor.Priority;
import org.apache.hadoop.hbase.regionserver.CoprocessorHost;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.SplitTransaction;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PairOfSameType;

public class TestCoprocessorInterface extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestCoprocessorInterface.class);
  static final String DIR = "test/build/data/TestCoprocessorInterface/";

  public static class CoprocessorImpl implements Coprocessor {

    private boolean opened;
    private boolean closed;
    private boolean compacted;
    private boolean flushed;
    private boolean split;

    public void onOpen(Environment e) {
      LOG.info("onOpen");
      opened = true;
    }

    public void onClose(Environment e, boolean abortRequested) {
      LOG.info("onClose abortRequested=" + abortRequested);
      closed = true;
    }

    public void onCompact(Environment e, boolean complete, boolean willSplit) {
      LOG.info("onCompact: complete=" + complete + " willSplit=" + willSplit);
      compacted = true;
    }

    public void onFlush(Environment e) {
      LOG.info("onFlush");
      flushed = true;
    }

    public void onSplit(Environment e, HRegion l, HRegion r) {
      LOG.info("onSplit: this=" + e.getRegion().getRegionNameAsString() +
        " l=" + l.getRegionNameAsString() +
        " r=" + r.getRegionNameAsString());
      split = true;
    }

    boolean wasOpened() {
      return opened;
    }

    boolean wasClosed() {
      return closed;
    }

    boolean wasFlushed() {
      return flushed;
    }

    boolean wasCompacted() {
      return compacted;
    }

    boolean wasSplit() {
      return split;
    }
  }

  public void testCoprocessorInterface() throws IOException {
    byte [] tableName = Bytes.toBytes("testtable");
    byte [][] families = { fam1, fam2, fam3 };

    Configuration hc = initSplit();
    HRegion region = initHRegion(tableName, getName(), hc,
      CoprocessorImpl.class, families);

    addContent(region, fam3);
    region.flushcache();
    byte [] splitRow = region.compactStores();
    assertNotNull(splitRow);
    HRegion [] regions = split(region, splitRow);
    for (int i = 0; i < regions.length; i++) {
      regions[i] = reopenRegion(regions[i], CoprocessorImpl.class);
    }
    region.close();
    region.getLog().closeAndDelete();

    Coprocessor c = region.getCoprocessorHost()
      .findCoprocessor(CoprocessorImpl.class.getName());
    assertTrue(((CoprocessorImpl)c).wasOpened());
    assertTrue(((CoprocessorImpl)c).wasClosed());
    assertTrue(((CoprocessorImpl)c).wasFlushed());
    assertTrue(((CoprocessorImpl)c).wasCompacted());
    assertTrue(((CoprocessorImpl)c).wasSplit());

    for (int i = 0; i < regions.length; i++) {
      regions[i].close();
      regions[i].getLog().closeAndDelete();
      c = region.getCoprocessorHost()
            .findCoprocessor(CoprocessorImpl.class.getName());
      assertTrue(((CoprocessorImpl)c).wasOpened());
      assertTrue(((CoprocessorImpl)c).wasClosed());
      assertTrue(((CoprocessorImpl)c).wasCompacted());
    }
  }

  HRegion reopenRegion(final HRegion closedRegion, Class<?> implClass)
      throws IOException {
    HRegion r = new HRegion(closedRegion.getRegionDir(), closedRegion.getLog(),
        closedRegion.getFilesystem(), closedRegion.getConf(),
        closedRegion.getRegionInfo(), null);
    r.initialize();
    CoprocessorHost host = r.getCoprocessorHost();
    host.load(implClass, Priority.USER);
    host.onOpen();
    return r;
  }

  HRegion initHRegion (byte [] tableName, String callingMethod,
      Configuration conf, Class<?> implClass, byte [] ... families)
      throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for(byte [] family : families) {
      htd.addFamily(new HColumnDescriptor(family));
    }
    HRegionInfo info = new HRegionInfo(htd, null, null, false);
    Path path = new Path(DIR + callingMethod);
    HRegion r = HRegion.createHRegion(info, path, conf);
    CoprocessorHost host = r.getCoprocessorHost();
    host.load(implClass, Priority.USER);
    host.onOpen();
    return r;
  }

  Configuration initSplit() {
    Configuration conf = HBaseConfiguration.create();
    // Always compact if there is more than one store file.
    conf.setInt("hbase.hstore.compactionThreshold", 2);
    // Make lease timeout longer, lease checks less frequent
    conf.setInt("hbase.master.lease.thread.wakefrequency", 5 * 1000);
    conf.setInt("hbase.regionserver.lease.period", 10 * 1000);
    // Increase the amount of time between client retries
    conf.setLong("hbase.client.pause", 15 * 1000);
    // This size should make it so we always split using the addContent
    // below.  After adding all data, the first region is 1.3M
    conf.setLong("hbase.hregion.max.filesize", 1024 * 128);
    return conf;
  }

  private HRegion [] split(final HRegion r, final byte [] splitRow)
      throws IOException {

    HRegion[] regions = new HRegion[2];

    SplitTransaction st = new SplitTransaction(r, splitRow);
    int i = 0;

    if (!st.prepare()) {
      // test fails.
      assertTrue(false);
    }
    try {
      PairOfSameType<HRegion> daughters = st.execute(null);
      for (HRegion each_daughter: daughters) {
	regions[i] = each_daughter;
	i++;
      }
    }
    catch (IOException ioe) {
      LOG.info("Split transaction of " + r.getRegionNameAsString() + " failed:" + ioe.getMessage());
      assertTrue(false);
    }
    catch (RuntimeException e) {
      LOG.info("Failed rollback of failed split of " + r.getRegionNameAsString() + e.getMessage());
    }

    assertTrue(i == 2);
    return regions;
  }
}


