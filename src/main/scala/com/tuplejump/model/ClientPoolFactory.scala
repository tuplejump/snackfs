package com.tuplejump.model

import org.apache.commons.pool.BasePoolableObjectFactory
import org.apache.thrift.async.TAsyncClientManager
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.cassandra.thrift.Cassandra.AsyncClient
import org.apache.thrift.transport.TNonblockingSocket
import scala.concurrent.Await
import com.tuplejump.util.AsyncUtil
import org.apache.cassandra.thrift.Cassandra.AsyncClient.set_keyspace_call
import scala.concurrent.duration._

import com.twitter.logging.Logger

class ClientPoolFactory(host: String, port: Int, keyspace: String) extends BasePoolableObjectFactory[ThriftClientAndSocket] {

  private val log = Logger.get(getClass)

  private val clientManager = new TAsyncClientManager()
  private val protocolFactory = new TBinaryProtocol.Factory()
  private val clientFactory = new AsyncClient.Factory(clientManager, protocolFactory)

  def makeObject(): ThriftClientAndSocket = {
    val transport = new TNonblockingSocket(host, port)
    val client = clientFactory.getAsyncClient(transport)
    val x = Await.result(AsyncUtil.executeAsync[set_keyspace_call](client.set_keyspace(keyspace, _)), 10 seconds)
    try {
      x.getResult()
      log.debug("set keyspace %s for client", keyspace)
      ThriftClientAndSocket(client, transport)
    } catch {
      case e: Exception =>
        log.error(e, "failed to set keyspace %s for client ", keyspace)
        throw e
    }
  }

  override def destroyObject(obj: ThriftClientAndSocket) {
    obj.socket.close()
    super.destroyObject(obj)
  }

  def closePool() {
    clientManager.stop()
  }
}
