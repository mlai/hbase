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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorEnvironment;

/**
 * Coprocessors implement this interface to observe and mediate client actions
 * on the region.
 */
public interface RegionObserver {

  /**
   * Called before a client makes a GetClosestRowBefore request.
   * @param e the environment provided by the region server
   * @param row the row
   * @param result the result set
   * @return the result set to return to the client
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public Result preGetClosestRowBefore(final CoprocessorEnvironment e,
      final byte [] row, final byte [] family, final Result result)
    throws CoprocessorException;
  
  /**
   * Called after a client makes a GetClosestRowBefore request.
   * @param e the environment provided by the region server
   * @param row the row
   * @param family the desired family
   * @param result the result set
   * @return the result set to return to the client
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public Result postGetClosestRowBefore(final CoprocessorEnvironment e,
      final byte [] row, final byte [] family, final Result result)
    throws CoprocessorException;

  /**
   * Called before the client perform a get()
   * @param e the environment provided by the region server
   * @param get the Get request
   * @param results the result list
   * @return the possibly returned result by coprocessor
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public List<KeyValue> preGet(final CoprocessorEnvironment e, final Get get,
      final List<KeyValue> results)
    throws CoprocessorException;

  /**
   * Called after the client perform a get()
   * @param e the environment provided by the region server
   * @param get the Get request
   * @param results the result list
   * @return the possibly transformed result list to use
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public List<KeyValue> postGet(final CoprocessorEnvironment e, final Get get,
      final List<KeyValue> results)
    throws CoprocessorException;
  
  /**
   * Called before the client tests for existence using a Get.
   * @param e the environment provided by the region server
   * @param get the Get request
   * @param exists the result returned by the region server
   * @return the result to return to the client
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public boolean preExists(final CoprocessorEnvironment e, final Get get,
      final boolean exists)
    throws CoprocessorException;
  
  /**
   * Called after the client tests for existence using a Get.
   * @param e the environment provided by the region server
   * @param get the Get request
   * @param exists the result returned by the region server
   * @return the result to return to the client
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public boolean postExists(final CoprocessorEnvironment e, final Get get,
      final boolean exists)
    throws CoprocessorException;

  /**
   * Called before the client stores a value.
   * @param e the environment provided by the region server
   * @param familyMap map of family to edits for the given family.
   * @return the possibly transformed map to actually use
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public Map<byte[], List<KeyValue>> prePut(final CoprocessorEnvironment e,
      final Map<byte[], List<KeyValue>> familyMap)
    throws CoprocessorException;

  /**
   * Called after the client stores a value.
   * @param e the environment provided by the region server
   * @param familyMap map of family to edits for the given family.
   * @return the possibly transformed map to actually use
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public Map<byte[], List<KeyValue>> postPut(final CoprocessorEnvironment e,
      final Map<byte[], List<KeyValue>> familyMap)
    throws CoprocessorException;
  
  /**
   * Called before the client stores a value.
   * @param e the environment provided by the region server
   * @param kv a KeyValue to store
   * @return the possibly transformed KeyValue to actually use
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public KeyValue prePut(final CoprocessorEnvironment e, final KeyValue kv)
    throws CoprocessorException;
  
  /**
   * Called before the client stores a value.
   * @param e the environment provided by the region server
   * @param kv a KeyValue to store
   * @return the possibly transformed KeyValue to actually use
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public KeyValue postPut(final CoprocessorEnvironment e, final KeyValue kv)
    throws CoprocessorException;

  /**
   * Called before the client deletes a value.
   * @param e the environment provided by the region server
   * @param familyMap map of family to edits for the given family.
   * @return the possibly transformed map to actually use
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public Map<byte[], List<KeyValue>> preDelete(final CoprocessorEnvironment e,
      final Map<byte[], List<KeyValue>> familyMap)
    throws CoprocessorException;

  /**
   * Called after the client deletes a value.
   * @param e the environment provided by the region server
   * @param familyMap map of family to edits for the given family.
   * @return the possibly transformed map to actually use
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public Map<byte[], List<KeyValue>> postDelete(final CoprocessorEnvironment e,
      final Map<byte[], List<KeyValue>> familyMap)
    throws CoprocessorException;

  /**
   * Called before checkAndPut
   * @param e the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param value the expected value
   * @param put data to put if check succeeds
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public void preCheckAndPut(final CoprocessorEnvironment e, 
      final byte [] row, final byte [] family, final byte [] qualifier,
      final byte [] value, final Put put)
    throws CoprocessorException;

  /**
   * Called after checkAndPut
   * @param e the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param value the expected value
   * @param put data to put if check succeeds
   * @param result true if the new put was executed, false otherwise
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public boolean postCheckAndPut(final CoprocessorEnvironment e, 
      final byte [] row, final byte [] family, final byte [] qualifier,
      final byte [] value, final Put put, final boolean result)
    throws CoprocessorException;

  /**
   * Called before checkAndPut
   * @param e the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param value the expected value
   * @param delete delete to commit if check succeeds
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public void preCheckAndDelete(final CoprocessorEnvironment e, 
      final byte [] row, final byte [] family, final byte [] qualifier,
      final byte [] value, final Delete delete)
    throws CoprocessorException;

  /**
   * Called after checkAndDelete
   * @param e the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param value the expected value
   * @param delete delete to commit if check succeeds
   * @param result true if the new put was executed, false otherwise
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public boolean postCheckAndDelete(final CoprocessorEnvironment e, 
      final byte [] row, final byte [] family, final byte [] qualifier,
      final byte [] value, final Delete delete, final boolean result)
    throws CoprocessorException;

  /**
   * Called before incrementColumnValue
   * @param e the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param amount long amount to increment
   * @param writeToWAL whether to write the increment to the WAL
   * @return new amount to increment
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public long preIncrementColumnValue(final CoprocessorEnvironment e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final long amount, final boolean writeToWAL)
    throws CoprocessorException;

  /**
   * Called after incrementColumnValue
   * @param e the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param amount long amount to increment
   * @param writeToWAL whether to write the increment to the WAL
   * @param result the result returned by incrementColumnValue
   * @return the result to return to the client
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public long postIncrementColumnValue(final CoprocessorEnvironment e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final long amount, final boolean writeToWAL, long result)
    throws CoprocessorException;

  /**
   * Called before the client opens a new scanner.
   * @param e the environment provided by the region server
   * @param scan the Scan specification
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public void preScannerOpen(final CoprocessorEnvironment e, final Scan scan)
    throws CoprocessorException;

  /**
   * Called after the client opens a new scanner.
   * @param e the environment provided by the region server
   * @param scan the Scan specification
   * @param scannerId the scanner id allocated by the region server
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public void postScannerOpen(final CoprocessorEnvironment e, final Scan scan,
      final long scannerId)
    throws CoprocessorException;

  /**
   * Called before the client asks for the next row on a scanner.
   * @param e the environment provided by the region server
   * @param scannerId the scanner id
   * @param results the result set returned by the region server
   * @return the possibly transformed result set to actually return
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public List<KeyValue> preScannerNext(final CoprocessorEnvironment e,
      final long scannerId, final List<KeyValue> results)
    throws CoprocessorException;
  
  /**
   * Called after the client asks for the next row on a scanner.
   * @param e the environment provided by the region server
   * @param scannerId the scanner id
   * @param results the result set returned by the region server
   * @return the possibly transformed result set to actually return
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public List<KeyValue> postScannerNext(final CoprocessorEnvironment e,
      final long scannerId, final List<KeyValue> results)
    throws CoprocessorException;

  /**
   * Called before the client closes a scanner.
   * @param e the environment provided by the region server
   * @param scannerId the scanner id
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public void preScannerClose(final CoprocessorEnvironment e,
      final long scannerId)
    throws CoprocessorException;
  
  /**
   * Called after the client closes a scanner.
   * @param e the environment provided by the region server
   * @param scannerId the scanner id
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public void postScannerClose(final CoprocessorEnvironment e,
      final long scannerId)
    throws CoprocessorException;
}
