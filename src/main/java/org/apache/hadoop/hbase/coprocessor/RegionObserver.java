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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.Coprocessor.Environment;

/**
 * Coprocessors implement this interface to observe and mediate client actions
 * on the region.
 */
public interface RegionObserver {

  /**
   * Called when a client makes a GetClosestRowBefore request.
   * @param e the environment provided by the region server
   * @param row the row
   * @param family the desired family
   * @param result the result set
   * @return the result set to return to the client
   */
  public Result onGetClosestRowBefore(final Environment e, final byte [] row,
    final byte [] family, final Result result);

  /**
   * Called as part of processing checkAndPut
   * @param e the environment provided by the region server
   * @param get the Get request
   * @param results the result list
   * @return the possibly transformed result list to use
   */
  public List<KeyValue> onGet(final Environment e, final Get get,
      final List<KeyValue> results);

  /**
   * Called when the client tests for existence using a Get.
   * @param e the environment provided by the region server
   * @param get the Get request
   * @param exists the result returned by the region server
   * @return the result to return to the client
   */
  public boolean onExists(final Environment e, final Get get,
    final boolean exists);

  /**
   * Called when the client stores a value.
   * @param e the environment provided by the region server
   * @param familyMap map of family to edits for the given family.
   * @return the possibly transformed map to actually use
   */
  public Map<byte[], List<KeyValue>> onPut(final Environment e,
    final Map<byte[], List<KeyValue>> familyMap);

  /**
   * Called when the client stores a value.
   * @param e the environment provided by the region server
   * @param kv a KeyValue to store
   * @return the possibly transformed KeyValue to actually use
   */
  public KeyValue onPut(final Environment e, final KeyValue kv);

  /**
   * Called when the client deletes a value.
   * @param e the environment provided by the region server
   * @param familyMap map of family to edits for the given family.
   * @return the possibly transformed map to actually use
   */
  public Map<byte[], List<KeyValue>> onDelete(final Environment e,
    final Map<byte[], List<KeyValue>> familyMap);

  /**
   * Called when the client opens a new scanner.
   * @param e the environment provided by the region server
   * @param scan the Scan specification
   * @param scannerId the scanner id allocated by the region server
   */
  public void onScannerOpen(final Environment e, final Scan scan,
    final long scannerId);

  /**
   * Called when the client asks for the next row on a scanner.
   * @param e the environment provided by the region server
   * @param scannerId the scanner id
   * @param results the result set returned by the region server
   * @return the possibly transformed result set to actually return
   */
  public List<KeyValue> onScannerNext(final Environment e,
    final long scannerId, final List<KeyValue> results);

  /**
   * Called when the client closes a scanner.
   * @param e the environment provided by the region server
   * @param scannerId the scanner id
   */
  public void onScannerClose(final Environment e, final long scannerId);
}
