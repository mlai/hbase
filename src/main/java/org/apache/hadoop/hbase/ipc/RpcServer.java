package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 */
public interface RpcServer {

  void setSocketSendBufSize(int size);

  void start();

  void stop();

  void join() throws InterruptedException;

  InetSocketAddress getListenerAddress();

  /** Called for each call.
   * @param param writable parameter
   * @param receiveTime time
   * @return Writable
   * @throws java.io.IOException e
   */
  Writable call(Class<? extends VersionedProtocol> protocol,
      Writable param, long receiveTime)
      throws IOException;

  int getNumOpenConnections();

  int getCallQueueLen();

  void setErrorHandler(HBaseRPCErrorHandler handler);

  <T extends VersionedProtocol> boolean registerProtocol(Class<T> protocol,
      T handler);
}
