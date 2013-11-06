/*
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Tuplejump Software Pvt. Ltd. licenses this file
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
 *
 */

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

  private val log = Logger.get("com.tuplejump.model.ClientPoolFactory")

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
