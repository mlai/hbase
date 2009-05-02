/**
 * Copyright 2008 The Apache Software Foundation
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
package org.apache.hadoop.hbase.rest.filter;

import org.apache.hadoop.hbase.rest.exception.HBaseRestException;

public interface FilterFactoryConstants {
  static String TYPE = "type";
  static String ARGUMENTS = "args";
  static String COLUMN_NAME = "column_name";
  static String COMPARE_OP = "compare_op";
  static String VALUE = "value";

  static class MalformedFilterException extends HBaseRestException {

    public MalformedFilterException() {
    }

    @Override
    public String toString() {
      return "malformed filter expression";
    }
  }
}