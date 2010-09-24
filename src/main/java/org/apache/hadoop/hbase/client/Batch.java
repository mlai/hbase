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

package org.apache.hadoop.hbase.client;

import org.apache.commons.lang.reflect.MethodUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;


/**
 * A collection of interfaces and utilities used for interacting with custom RPC
 * interfaces exposed by Coprocessors.
 */
public abstract class Batch {
  private static Log LOG = LogFactory.getLog(Batch.class);

  /**
   * Creates a new {@link org.apache.hadoop.hbase.client.Batch.Call} instance that invokes a method
   * with the given parameters and returns the result.
   *
   * <p>
   * Note that currently the method is naively looked up using the method name
   * and class types of the passed arguments, which means that
   * <em>none of the arguments can be <code>null</code></em>.
   * For more flexibility, see {@link Batch#returning(java.lang.reflect.Method, Object...)}.
   * </p>
   *   
   * @param protocol the protocol class being called
   * @param method the method name
   * @param args zero or more arguments to be passed to the method (individual args cannot be <code>null</code>!)
   * @param <T> the class type of the protocol implementation being invoked
   * @param <R> the return type for the method call
   * @return a {@code Callable} instance that will invoke the given method and return the results
   * @throws NoSuchMethodException if the method named, with the given argument
   *     types, cannot be found in the protocol class
   * @see Batch#returning(java.lang.reflect.Method, Object...)
   */
  public static <T extends CoprocessorProtocol,R> Call<T,R> returning(
      final Class<T> protocol, final String method, final Object... args)
  throws NoSuchMethodException {
    Class[] types = new Class[args.length];
    for (int i=0; i<args.length; i++) {
      // TODO: this only allows non-null args
      types[i] = args[i].getClass();
    }

    Method m = MethodUtils.getMatchingAccessibleMethod(protocol, method, types);
    if (m == null) {
      throw new NoSuchMethodException("No matching method found for '"+method+"'");
    }

    m.setAccessible(true);
    return Batch.returning(m, args);
  }

  /**
   * Creates a new {@link org.apache.hadoop.hbase.client.Batch.Call} instance that invokes a method
   * with the given parameters and returns the result.
   *
   * @param method the method reference to invoke
   * @param args zero or more arguments to be passed to the method
   * @param <T> the class type of the protocol implementation being invoked
   * @param <R> the return type for the method call
   * @return a {@code Callable} instance that will invoke the given method and return the results
   */
  public static <T extends CoprocessorProtocol,R> Call<T,R> returning(
      final Method method, final Object... args) {
    return new Call<T,R>() {
        public R call(T instance) throws IOException {
          try {
            if (Proxy.isProxyClass(instance.getClass())) {
              InvocationHandler invoker = Proxy.getInvocationHandler(instance);
              return (R)invoker.invoke(instance, method, args);
            } else {
              LOG.warn("Non proxied invocation of method '"+method.getName()+"'!");
              return (R)method.invoke(instance, args);
            }
          }
          catch (IllegalAccessException iae) {
            throw new IOException("Unable to invoke method '"+
                method.getName()+"'", iae);
          }
          catch (InvocationTargetException ite) {
            IOException ioe = new IOException(ite.toString());
            ioe.setStackTrace(ite.getStackTrace());
            throw ioe;
          }
          catch (Throwable t) {
            IOException ioe = new IOException(t.toString());
            ioe.setStackTrace(t.getStackTrace());
            throw ioe;
          }
        }
    };
  }

  /**
   * Defines a unit of work to be executed.
   * @see HTable#exec(Class, java.util.List, org.apache.hadoop.hbase.client.Batch.Call , org.apache.hadoop.hbase.client.Batch.Callback)
   * @see HTable#exec(Class, RowRange, org.apache.hadoop.hbase.client.Batch.Call , org.apache.hadoop.hbase.client.Batch.Callback)
   * @param <T> the instance type to be passed to {@link org.apache.hadoop.hbase.client.Batch.Call#call(Object)}
   * @param <R> the return type from {@link org.apache.hadoop.hbase.client.Batch.Call#call(Object)}
   */
  public static interface Call<T,R> {
    public R call(T instance) throws IOException;
  }

  /**
   * Defines a generic callback to be triggered for each {@link org.apache.hadoop.hbase.client.Batch.Call#call(Object)}
   * result.
   * @param <R> the return type from the associated {@link org.apache.hadoop.hbase.client.Batch.Call#call(Object)}
   */
  public static interface Callback<R> {
    public void update(byte[] region, byte[] row, R result);
  }
}
