/*
 * Copyright 2009 The Apache Software Foundation
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

import org.apache.hadoop.hbase.coprocessor.CoprocessorEnvironment;

/**
 * Coprocessors which implement this interface support a generic ioctl-like
 * command mechanism.
 */
public interface CommandTarget {

  /**
   * Invoked when the client wants to run a synchronous command on the
   * coprocessor.
   * @param e the environment provided by the region server
   * @param cmd the command selector
   * @param args command arguments
   * @return the command result
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public Object onCommand(final CoprocessorEnvironment e, final int cmd,
    final Object[] args) throws CoprocessorException;

  /**
   * Invoked when the client wants to run an asynchronous command on the
   * coprocessor.
   * @param e the environment provided by the region server
   * @param cmd the command selector
   * @param args command arguments
   * @return the command session identifier
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public long onCommandAsyncSubmit(final CoprocessorEnvironment e,
      final int cmd, final Object[] args) throws CoprocessorException;

  /**
   * Invoked when the client polls asynchronous command results.
   * @param e the environment provided by the region server
   * @param cmdId the command session identifier
   * @return the command result, or <tt>null</tt> if results are not yet
   * available
   * @throws CoprocessorException if an error occurred on the coprocessor
   */
  public Object onCommandAsyncPoll(final CoprocessorEnvironment e,
      final long cmdId)
      throws CoprocessorException;

}