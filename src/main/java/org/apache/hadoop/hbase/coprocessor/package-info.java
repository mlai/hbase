/*
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

/**
<h2>Table of Contents</h2>
<ul>
<li><a href="#overview">Overview</a></li>
<li><a href="#lifecycle">Lifecycle hooks</a></li>
<li><a href="#regionobserver">Region observer</a></li>
</ul>

 <h2><a name="overview">Overview</a></h2>
Coprocessors are code that runs in-process on each region server. Regions
contain references to the coprocessor implementation classes associated
with them. Coprocessor classes will be loaded either on demand or by 
default by configuration. On demand loading can be from local
jars on the region server's classpath or via the HDFS classloader.
<p>
While a coprocessor can have arbitrary function, it is required to
implement this interface.
<p>
The design goal CommandTargetof this interface is to provide simple features for
making coprocessors useful, while exposing no more internal state or
control actions of the region server than necessary and not exposing them
directly.
<p>
Over the lifecycle of a region, the methods of this interface are invoked
when the corresponding events happen. The master transitions regions
through the following states:
<p>
&nbsp;&nbsp;&nbsp;
unassigned -> pendingOpen -> open -> pendingClose -> closed.
<p>
Coprocessors have opportunity to intercept and handle events in
pendingOpen, open, and pendingClose states.
<p>
<h2><a name="lifecycle">Lifecycle hooks</a></h2>
<h3>PendingOpen</h3>
<p>
The region server is opening a region to bring it online. Coprocessors
can piggyback or fail this process.
<p>
<ul>
  <li>preOpen, postOpen: Called before and the region is reported as online 
  to the master.</li><p>
</ul>
<p>
<h3>Open</h3>
The region is open on the region server and is processing both client
requests (get, put, scan, etc.) and administrative actions (flush, compact,
split, etc.). Coprocessors can piggyback administrative actions via:
<p>
<ul>
  <li>preFlush, postFlush: Called before and after the memstore is flushed 
  into a new store file.</li><p>
  <li>preCompact, postCompact: Called before and after compaction.</li><p>
  <li>preSplit, postSplit: Called after the region is split.</li><p>
</ul>
<p>
<h3>PendingClose</h3>
The region server is closing the region. This can happen as part of normal
operations or may happen when the region server is aborting due to fatal
conditions such as OOME, health check failure, or fatal filesystem
problems. Coprocessors can piggyback this event. If the server is aborting
an indication to this effect will be passed as an argument.
<p>
<ul>
  <li>preClose and post Close: Called before and after the region is 
  reported as closed to the master.</li><p>
</ul>
<p>

<h2><a name="regionobserver">RegionObserver</a></h2>
If the coprocessor implements the <tt>RegionObserver</tt> interface it can observe
and mediate client actions on the region:
<p>
<ul>
  <li>preGet, postGet: Called before and after a client makes a Get 
  request.</li><p>
  <li>preExists, postExists: Called before and after the client tests 
  for existence using a Get.</li><p>
  <li>prePut and postPut: Called before and after the client stores a value.
  </li><p>
  <li>preDelete and postDelete: Called before and after the client 
  deletes a value.</li><p>
  <li>preScannerOpenm postScannerOpen: Called before and after the client 
  opens a new scanner.</li><p>
  <li>preScannerNext, postScannerNext: Called before and after the client 
  asks for the next row on a scanner.</li><p>
  <li>preScannerClose, postScannerClose: Called before and after the client 
  closes a scanner.</li><p>
</ul>

*/
package org.apache.hadoop.hbase.coprocessor;
