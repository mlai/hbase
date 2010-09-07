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

import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * RBAC coprocessor
 */
public class RBACCoprocessor implements Coprocessor, RegionObserver {
  public static final Log LOG = LogFactory.getLog(RBACCoprocessor.class);
 
  @Override
  public boolean onExists(CoprocessorEnvironment e, Get get, boolean exists)
      throws CoprocessorException {
    return false;
  }

  @Override
  public void onClose(CoprocessorEnvironment e, boolean abortRequested)
      throws CoprocessorException {
  }

  @Override
  public void onCompact(CoprocessorEnvironment e, boolean complete,
      boolean willSplit) throws CoprocessorException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onFlush(CoprocessorEnvironment e) throws CoprocessorException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onOpen(CoprocessorEnvironment e) throws CoprocessorException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onSplit(CoprocessorEnvironment e, HRegion l, HRegion r)
      throws CoprocessorException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Result onGetClosestRowBefore(CoprocessorEnvironment e, byte[] row,
      byte[] family, Result result) throws CoprocessorException {
    return result;
  }

  @Override
  public void onScannerClose(CoprocessorEnvironment e, long scannerId)
      throws CoprocessorException {
  }

  @Override
  public List<KeyValue> onScannerNext(CoprocessorEnvironment e, long scannerId,
      List<KeyValue> results) throws CoprocessorException {
    return results;
  }

  @Override
  public void onScannerOpen(CoprocessorEnvironment e, Scan scan, long scannerId)
      throws CoprocessorException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Map<byte[], List<KeyValue>> postDelete(CoprocessorEnvironment e,
      Map<byte[], List<KeyValue>> familyMap) throws CoprocessorException {
    return familyMap;
  }

  @Override
  public List<KeyValue> postGet(CoprocessorEnvironment e, Get get,
      List<KeyValue> results) throws CoprocessorException {
    return results;
  }

  @Override
  public KeyValue postPut(CoprocessorEnvironment e, KeyValue kv)
      throws CoprocessorException {
    return kv;
  }

  @Override
  public Map<byte[], List<KeyValue>> postPut(CoprocessorEnvironment e,
      Map<byte[], List<KeyValue>> familyMap) throws CoprocessorException {
    return familyMap;
  }

  @Override
  public Map<byte[], List<KeyValue>> preDelete(CoprocessorEnvironment e,
      Map<byte[], List<KeyValue>> familyMap) throws CoprocessorException {
    // TODO Auto-generated method stub
    return familyMap;
  }

  @Override
  public List<KeyValue> preGet(CoprocessorEnvironment e, Get get)
      throws CoprocessorException {
    // check permissions
    return null;
  }

  @Override
  public KeyValue prePut(CoprocessorEnvironment e, KeyValue kv)
      throws CoprocessorException {
    // TODO Auto-generated method stub
    return kv;
  }

  @Override
  public Map<byte[], List<KeyValue>> prePut(CoprocessorEnvironment e,
      Map<byte[], List<KeyValue>> familyMap) throws CoprocessorException {
    return familyMap;
  }
}
