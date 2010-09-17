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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.Coprocessor;
import org.apache.hadoop.hbase.coprocessor.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.Coprocessor.Priority;
import org.apache.hadoop.hbase.regionserver.CoprocessorHost;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.SplitTransaction;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.Server;
import org.mockito.Mockito;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import static org.mockito.Mockito.when;

public class TestCoprocessorInterface extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestCoprocessorInterface.class);
  static final String DIR = "test/build/data/TestCoprocessorInterface/";
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  
  public static class CoprocessorImpl implements Coprocessor {

    private boolean preOpenCalled;
    private boolean postOpenCalled;
    private boolean preCloseCalled;
    private boolean postCloseCalled;
    private boolean preCompactCalled;
    private boolean postCompactCalled;
    private boolean preFlushCalled;
    private boolean postFlushCalled;
    private boolean preSplitCalled;
    private boolean postSplitCalled;

    @Override
    public void preOpen(CoprocessorEnvironment e) {
      preOpenCalled = true;
    }
    @Override
    public void postOpen(CoprocessorEnvironment e) {
      postOpenCalled = true;
    }
    @Override
    public void preClose(CoprocessorEnvironment e, boolean abortRequested) {
      preCloseCalled = true;
    }
    @Override
    public void postClose(CoprocessorEnvironment e, boolean abortRequested) {
      postCloseCalled = true;
    }
    @Override
    public void preCompact(CoprocessorEnvironment e, boolean willSplit) {
      preCompactCalled = true;
    }
    @Override
    public void postCompact(CoprocessorEnvironment e, boolean willSplit) {
      postCompactCalled = true;
    }
    @Override
    public void preFlush(CoprocessorEnvironment e) {
      preFlushCalled = true;
    }
    @Override
    public void postFlush(CoprocessorEnvironment e) {
      postFlushCalled = true;
    }
    @Override
    public void preSplit(CoprocessorEnvironment e) {
      preSplitCalled = true;
    }
    @Override
    public void postSplit(CoprocessorEnvironment e, HRegion l, HRegion r) {
      postSplitCalled = true;
    }

    boolean wasOpened() {
      return (preOpenCalled && postOpenCalled);
    }

    boolean wasClosed() {
      return (preCloseCalled && postCloseCalled);
    }

    boolean wasFlushed() {
      return (preFlushCalled && postFlushCalled);
    }

    boolean wasCompacted() {
      return (preCompactCalled && postCompactCalled);
    }

    boolean wasSplit() {
      return (preSplitCalled && postSplitCalled);
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
    // we need to manually call pre- and postOpen here since the 
    // above load() is not the real case for CP loading. A CP is
    // expected to be loaded by default from 1) configuration; or 2)
    // HTableDescriptor. If it's loaded after HRegion initialized, 
    // the pre- and postOpen() won't be triggered automatically. 
    // Here we have to call pre and postOpen explicitly.
    host.preOpen();
    host.postOpen();
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
    
    // Here we have to call pre and postOpen explicitly.
    host.preOpen();
    host.postOpen();
    return r;
  }

  Configuration initSplit() {
    // Always compact if there is more than one store file.
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compactionThreshold", 2);
    // Make lease timeout longer, lease checks less frequent
    TEST_UTIL.getConfiguration().setInt("hbase.master.lease.thread.wakefrequency", 5 * 1000);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.lease.period", 10 * 1000);
    // Increase the amount of time between client retries
    TEST_UTIL.getConfiguration().setLong("hbase.client.pause", 15 * 1000);
    // This size should make it so we always split using the addContent
    // below.  After adding all data, the first region is 1.3M
    TEST_UTIL.getConfiguration().setLong("hbase.hregion.max.filesize", 1024 * 128);
    TEST_UTIL.getConfiguration().setBoolean("hbase.testing.nocluster", true);

    return TEST_UTIL.getConfiguration();
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
      Server mockServer = Mockito.mock(Server.class);
      when(mockServer.getConfiguration()).thenReturn(TEST_UTIL.getConfiguration());
      PairOfSameType<HRegion> daughters = st.execute(mockServer, null);
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


