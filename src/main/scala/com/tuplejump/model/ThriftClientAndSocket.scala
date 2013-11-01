package com.tuplejump.model

import org.apache.cassandra.thrift.Cassandra.AsyncClient
import org.apache.thrift.transport.TNonblockingSocket

case class ThriftClientAndSocket(client: AsyncClient, socket: TNonblockingSocket)
