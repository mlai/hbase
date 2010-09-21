/*
 * Copyright 2009 The Apache Software Foundation
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

package org.apache.hadoop.hbase.client;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.HServerAddress;

import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 * A container for Result objects, grouped by regionName.
 */
public class MultiResponse<R> implements Writable {

  // map of regionName to list of (Results paired to the original index for that
  // Result)
  private Map<byte[], List<Pair<Integer, R>>> results = new TreeMap<byte[], List<Pair<Integer, R>>>(
      Bytes.BYTES_COMPARATOR);

  public MultiResponse() {
  }

  /**
   * @return Number of pairs in this container
   */
  public int size() {
    int size = 0;
    for (Collection<?> c : results.values()) {
      size += c.size();
    }
    return size;
  }

  /**
   * Add the pair to the container, grouped by the regionName
   *
   * @param regionName
   * @param r
   *          First item in the pair is the original index of the Action
   *          (request). Second item is the Result. Result will be empty for
   *          successful Put and Delete actions.
   */
  public void add(byte[] regionName, Pair<Integer, R> r) {
    List<Pair<Integer, R>> rs = results.get(regionName);
    if (rs == null) {
      rs = new ArrayList<Pair<Integer, R>>();
      results.put(regionName, rs);
    }
    rs.add(r);
  }

  public Map<byte[], List<Pair<Integer, R>>> getResults() {
    return results;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(results.size());
    for (Map.Entry<byte[], List<Pair<Integer, R>>> e : results.entrySet()) {
      Bytes.writeByteArray(out, e.getKey());
      List<Pair<Integer, R>> lst = e.getValue();
      out.writeInt(lst.size());
      for (Pair<Integer, R> r : lst) {
        if (r == null) {
          out.writeInt(-1); // Cant have index -1; on other side we recognize -1 as 'null'
        } else {
          out.writeInt(r.getFirst()); // Can this can npe!?!
          R value = r.getSecond();
          HbaseObjectWritable.writeObject(out, r.getSecond(), value.getClass(), null);
        }
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    results.clear();
    int mapSize = in.readInt();
    for (int i = 0; i < mapSize; i++) {
      byte[] key = Bytes.readByteArray(in);
      int listSize = in.readInt();
      List<Pair<Integer, R>> lst = new ArrayList<Pair<Integer, R>>(
          listSize);
      for (int j = 0; j < listSize; j++) {
        Integer idx = in.readInt();
        if (idx == -1) {
          lst.add(null); 
        } else {
          R r = (R) HbaseObjectWritable.readObject(in, null);
          lst.add(new Pair<Integer, R>(idx, r));
        }
      }
      results.put(key, lst);
    }
  }

}
