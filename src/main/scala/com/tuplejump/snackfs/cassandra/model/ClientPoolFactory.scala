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

import scala.collection.JavaConversions._

class ClientPoolFactory(host: String, port: Int, keyspace: String, multiNode: Boolean = false) extends BasePoolableObjectFactory[ThriftClientAndSocket] {

  private lazy val log = Logger.get(getClass)

  val protocolFactory = new TBinaryProtocol.Factory()
  val clientFactory = new Client.Factory()

  val nodes: Vector[String] = if (multiNode) {
    val socket = new TSocket(host, port)
    val client = getClient(socket).client
    Try {
      client.describe_ring(keyspace).flatMap(_.getEndpoints).distinct.toVector
    } match {
      case Success(nodes: Vector[String]) => nodes
      case Failure(ex: Throwable) => Vector(host)
    }
  } else {
    Vector.empty[String]
  }

  val ringSize = nodes.length

  var lastNodeIdx = -1


  def makeObject(): ThriftClientAndSocket = {
    val socket =
      if (multiNode) {
        lastNodeIdx = if (lastNodeIdx < ringSize) lastNodeIdx + 1 else 0
        new TSocket(nodes(lastNodeIdx), port)
      } else {
        new TSocket(host, port)
      }

    getClient(socket)
  }

  private def getClient(socket: TSocket) = {
    val transport = new TFramedTransport(socket)
    transport.open()
    val client = clientFactory.getClient(protocolFactory.getProtocol(transport))

    val x = Try(client.set_keyspace(keyspace))
    x match {
      case Success(s) =>
        log.debug("set keyspace %s for client", keyspace)
        ThriftClientAndSocket(client, socket, transport)

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
