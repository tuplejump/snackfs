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

package com.tuplejump.snackfs.cassandra.model

import org.apache.commons.pool.BasePoolableObjectFactory
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.cassandra.thrift.Cassandra.Client
import org.apache.thrift.transport.{TFramedTransport, TSocket}

import com.twitter.logging.Logger
import scala.util.{Failure, Success, Try}

class ClientPoolFactory(host: String, port: Int, keyspace: String) extends BasePoolableObjectFactory[ThriftClientAndSocket] {

  private lazy val log = Logger.get(getClass)

  val protocolFactory = new TBinaryProtocol.Factory()
  val clientFactory = new Client.Factory()

  def makeObject(): ThriftClientAndSocket = {
    val socket = new TSocket(host, port)
    val transport = new TFramedTransport(socket)
    transport.open()
    val client = clientFactory.getClient(protocolFactory.getProtocol(transport))

    val x = Try(client.set_keyspace(keyspace))
    x match {
      case Success(s) =>
        log.debug("set keyspace %s for client", keyspace)
        ThriftClientAndSocket(client, socket,transport)

      case Failure(e) =>
        log.error(e, "failed to set keyspace %s for client ", keyspace)
        throw e
    }
  }

  override def destroyObject(obj: ThriftClientAndSocket) {
    obj.transport.close()
    obj.socket.close()
    super.destroyObject(obj)
  }
}
