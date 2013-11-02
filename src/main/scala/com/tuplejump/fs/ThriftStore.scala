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
package com.tuplejump.fs


import org.apache.cassandra.thrift.Cassandra.AsyncClient
import org.apache.cassandra.thrift.Cassandra.AsyncClient._

import org.apache.cassandra.utils.{FBUtilities, ByteBufferUtil}
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.cassandra.thrift._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.nio.ByteBuffer
import scala.util.Try
import org.apache.hadoop.fs.Path
import java.math.BigInteger
import java.util.UUID
import java.io.InputStream
import com.tuplejump.util.AsyncUtil
import com.tuplejump.model._
import com.tuplejump.model.SubBlockMeta
import com.tuplejump.model.BlockMeta
import com.tuplejump.model.GenericOpSuccess
import org.apache.cassandra.dht.Murmur3Partitioner
import org.apache.thrift.async.TAsyncClientManager
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TNonblockingSocket
import org.apache.commons.pool.ObjectPool
import org.apache.commons.pool.impl.StackObjectPool

import com.twitter.logging.Logger

class ThriftStore(configuration: SnackFSConfiguration) extends FileSystemStore {

  private val log = Logger.get(getClass)
  private val PATH_COLUMN: ByteBuffer = ByteBufferUtil.bytes("path")
  private val PARENT_PATH_COLUMN: ByteBuffer = ByteBufferUtil.bytes("parent_path")
  private val SENTINEL_COLUMN: ByteBuffer = ByteBufferUtil.bytes("sentinel")
  private val SENTINEL_VALUE: ByteBuffer = ByteBufferUtil.bytes("x")
  private val DATA_COLUMN: ByteBuffer = ByteBufferUtil.bytes("data")
  private val GRACE_SECONDS: Int = 60

  private val INODE_COLUMN_FAMILY_NAME = "inode"
  private val BLOCK_COLUMN_FAMILY_NAME = "sblock"

  private val partitioner = new Murmur3Partitioner()

  private var clientPool: ObjectPool[ThriftClientAndSocket] = _

  private def executeWithClient[T](f: AsyncClient => Future[T])(implicit tm: ClassManifest[T]): Future[T] = {
    log.debug("Fetching client from pool")
    val thriftClientAndSocket = clientPool.borrowObject()
    val ret = f(thriftClientAndSocket.client)

    ret.onComplete {
      res =>
        clientPool.returnObject(thriftClientAndSocket)
    }
    ret
  }

  def createKeyspace: Future[Keyspace] = {

    val clientManager = new TAsyncClientManager()
    val protocolFactory = new TBinaryProtocol.Factory()
    val clientFactory = new AsyncClient.Factory(clientManager, protocolFactory)

    val transport = new TNonblockingSocket(configuration.CassandraHost, configuration.CassandraPort)
    val client = clientFactory.getAsyncClient(transport)

    val ksDef = buildSchema
    val prom = promise[Keyspace]()
    val ksDefFuture = AsyncUtil.executeAsync[describe_keyspace_call](client.describe_keyspace(ksDef.getName, _))

    ksDefFuture onSuccess {
      case p => {

        val mayBeKsDef: Try[KsDef] = Try(p.getResult)

        if (mayBeKsDef.isSuccess) {
          log.debug("Using existing keyspace %s", ksDef.getName)
          prom success new Keyspace(ksDef.getName)
        } else {
          log.debug("Creating new keyspace %s", ksDef.getName)
          val response = AsyncUtil.executeAsync[system_add_keyspace_call](
            client.system_add_keyspace(ksDef, _))

          response.onSuccess {
            case r => {
              log.debug("Created keyspace %s", ksDef.getName)
              prom success new Keyspace(r.getResult)
            }
          }

          response.onFailure {
            case f => {
              log.error(f, "Failed to create keyspace %s", f.getMessage)
              prom failure f
            }
          }
        }
      }
    }
    prom.future
  }

  def init {
    log.debug("initializing thrift store with configuration %s", configuration.toString)
    clientPool = new StackObjectPool[ThriftClientAndSocket](
      new ClientPoolFactory(configuration.CassandraHost, configuration.CassandraPort, configuration.keySpace)) {
      override def close() {
        super.close()
        getFactory.asInstanceOf[ClientPoolFactory].closePool()
      }
    }
  }

  def dropKeyspace: Future[Unit] = executeWithClient({
    client =>
      val prom = promise[Unit]()
      val dropFuture = AsyncUtil.executeAsync[system_drop_keyspace_call](client.system_drop_keyspace(configuration.keySpace, _))
      dropFuture onSuccess {
        case p => {
          log.debug("deleted keyspace %s", configuration.keySpace)
          prom success p.getResult
        }
      }
      dropFuture onFailure {
        case f => {
          log.error(f, "failed to delete keyspace %s", configuration.keySpace)
          prom failure f
        }
      }
      prom.future
  })

  def disconnect() = {
    clientPool.close()
  }

  private def createINodeCF(cfName: String) = {

    val PATH_INDEX_LABEL = "path"
    val SENTINEL_INDEX_LABEL = "sentinel"
    val PARENT_PATH_INDEX_LABEL = "parent_path"

    val DATA_TYPE = "BytesType"

    val columnFamily = new CfDef(configuration.keySpace, cfName)
    columnFamily.setComparator_type(DATA_TYPE)
    columnFamily.setGc_grace_seconds(GRACE_SECONDS)
    columnFamily.setComment("Stores file meta data")

    val path = generateColumnDefinition(PATH_COLUMN, PATH_INDEX_LABEL)
    val sentinel = generateColumnDefinition(SENTINEL_COLUMN, SENTINEL_INDEX_LABEL)
    val parentPath = generateColumnDefinition(PARENT_PATH_COLUMN, PARENT_PATH_INDEX_LABEL)

    val metadata = List(path, sentinel, parentPath)
    columnFamily.setColumn_metadata(metadata)

    columnFamily
  }

  private def generateColumnDefinition(columnName: ByteBuffer, indexName: String): ColumnDef = {
    val DATA_TYPE = "BytesType"
    val cfDef = new ColumnDef(columnName, DATA_TYPE).setIndex_type(IndexType.KEYS).setIndex_name(indexName)
    cfDef
  }

  private def createSBlockCF(cfName: String, minCompaction: Int, maxCompaction: Int) = {

    val columnFamily = new CfDef()
    columnFamily.setName(cfName)
    columnFamily.setComparator_type("BytesType")
    columnFamily.setGc_grace_seconds(GRACE_SECONDS)
    columnFamily.setComment("Stores blocks of information associated with a inode")
    columnFamily.setKeyspace(configuration.keySpace)

    columnFamily.setMin_compaction_threshold(minCompaction)
    columnFamily.setMax_compaction_threshold(maxCompaction)

    columnFamily
  }

  private def buildSchema: KsDef = {
    val inode = createINodeCF(INODE_COLUMN_FAMILY_NAME)
    val sblock = createSBlockCF(BLOCK_COLUMN_FAMILY_NAME, 16, 64)

    val ksDef: KsDef = new KsDef(configuration.keySpace, configuration.replicationStrategy,
      List(inode, sblock))
    ksDef.setStrategy_options(Map("replication_factor" -> configuration.replicationFactor.toString))

    ksDef
  }

  private def getPathKey(path: Path): ByteBuffer = {
    val pathBytes: ByteBuffer = ByteBufferUtil.bytes(path.toUri.getPath)
    val pathBytesAsInt: BigInteger = FBUtilities.hashToBigInteger(pathBytes)
    ByteBufferUtil.bytes(pathBytesAsInt.toString(16))
  }

  private def getParentForIndex(path: Path): String = {
    val parent = path.getParent
    var result = "null"
    if (parent != null) {
      result = parent.toUri.getPath
    }
    result
  }

  private def createMutationForCol(colName: ByteBuffer, value: ByteBuffer, ts: Long): Mutation = {
    val result = new Mutation().setColumn_or_supercolumn(
      new ColumnOrSuperColumn().setColumn(
        new Column()
          .setName(colName)
          .setValue(value)
          .setTimestamp(ts)))
    result
  }

  private def generateMutationforINode(data: ByteBuffer, path: Path, timestamp: Long): Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]] = {
    val pathColMutation = createMutationForCol(PATH_COLUMN, ByteBufferUtil.bytes(path.toUri.getPath), timestamp)
    val parentColMutation = createMutationForCol(PARENT_PATH_COLUMN, ByteBufferUtil.bytes(getParentForIndex(path)), timestamp)
    val sentinelMutation = createMutationForCol(SENTINEL_COLUMN, SENTINEL_VALUE, timestamp)
    val dataMutation = createMutationForCol(DATA_COLUMN, data, timestamp)
    val mutations: java.util.List[Mutation] = List(pathColMutation, parentColMutation, sentinelMutation, dataMutation)

    val pathMutation: java.util.Map[String, java.util.List[Mutation]] = Map(INODE_COLUMN_FAMILY_NAME -> mutations)
    val mutationMap: Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]] = Map(getPathKey(path) -> pathMutation)

    mutationMap
  }

  def storeINode(path: Path, iNode: INode): Future[GenericOpSuccess] = executeWithClient({
    client =>
      val data: ByteBuffer = iNode.serialize
      val timestamp = iNode.timestamp
      val mutationMap: Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]] = generateMutationforINode(data, path, timestamp)
      val iNodeFuture = AsyncUtil.executeAsync[batch_mutate_call](client.batch_mutate(mutationMap, configuration.writeConsistencyLevel, _))
      val prom = promise[GenericOpSuccess]()
      iNodeFuture.onSuccess {
        case p => {
          log.debug("stored INode %s", iNode.toString)
          prom success GenericOpSuccess()
        }
      }
      iNodeFuture.onFailure {
        case f => {
          log.error(f, "failed to store INode %s", iNode.toString)
          prom failure f
        }
      }
      prom.future
  })

  private def performGet(client: AsyncClient, key: ByteBuffer, columnPath: ColumnPath): Future[ColumnOrSuperColumn] = {
    val prom = promise[ColumnOrSuperColumn]()
    val getFuture = AsyncUtil.executeAsync[get_call](client.get(key, columnPath, configuration.readConsistencyLevel, _))
    getFuture.onSuccess {
      case p =>
        try {
          val res = p.getResult
          log.debug("fetch INode/subblock data")
          prom success res
        } catch {
          case e: Exception =>
            log.error(e, "failed to get INode/subblock data")
            prom failure e
        }
    }
    getFuture.onFailure {
      case f =>
        log.error(f, "failed to get INode/subblock data")
        prom failure f
    }
    prom.future
  }

  def retrieveINode(path: Path): Future[INode] = executeWithClient({
    client =>
      val pathKey: ByteBuffer = getPathKey(path)
      val inodeDataPath = new ColumnPath(INODE_COLUMN_FAMILY_NAME).setColumn(DATA_COLUMN)

      val inodePromise = promise[INode]()
      val pathInfo = performGet(client, pathKey, inodeDataPath)
      pathInfo.onSuccess {
        case p =>
          log.debug("retrieved Inode for path %s", path)
          inodePromise success INode.deserialize(ByteBufferUtil.inputStream(p.column.value), p.column.getTimestamp)
      }
      pathInfo.onFailure {
        case f =>
          log.error(f, "failed to retrieve Inode for path %s", path)
          inodePromise failure f
      }
      inodePromise.future
  })

  def storeSubBlock(blockId: UUID, subBlockMeta: SubBlockMeta, data: ByteBuffer): Future[GenericOpSuccess] = executeWithClient({
    client =>
      val parentBlockId: ByteBuffer = ByteBufferUtil.bytes(blockId)

      val sblockParent = new ColumnParent(BLOCK_COLUMN_FAMILY_NAME)

      val column = new Column()
        .setName(ByteBufferUtil.bytes(subBlockMeta.id))
        .setValue(data)
        .setTimestamp(System.currentTimeMillis)

      val prom = promise[GenericOpSuccess]()
      val subBlockFuture = AsyncUtil.executeAsync[insert_call](
        client.insert(parentBlockId, sblockParent, column, configuration.writeConsistencyLevel, _))

      subBlockFuture.onSuccess {
        case p =>
          log.debug("stored subBlock %s for block with id %s", subBlockMeta.toString, blockId.toString)
          prom success GenericOpSuccess()
      }
      subBlockFuture.onFailure {
        case f =>
          log.debug(f, " failed to store subBlock %s for block with id %s", subBlockMeta.toString, blockId.toString)
          prom failure f
      }
      prom.future
  })

  def retrieveSubBlock(blockId: UUID, subBlockId: UUID, byteRangeStart: Long): Future[InputStream] = executeWithClient({
    client =>
      val blockIdBuffer: ByteBuffer = ByteBufferUtil.bytes(blockId)
      val subBlockIdBuffer = ByteBufferUtil.bytes(subBlockId)
      val subBlockFuture = performGet(client, blockIdBuffer, new ColumnPath(BLOCK_COLUMN_FAMILY_NAME).setColumn(subBlockIdBuffer))
      val prom = promise[InputStream]()
      subBlockFuture.onSuccess {
        case p =>
          val stream: InputStream = ByteBufferUtil.inputStream(p.column.value)
          log.debug("retrieved subBlock with id %s and block id %s", subBlockId.toString, blockId.toString)
          prom success stream
      }
      subBlockFuture.onFailure {
        case f =>
          log.debug("failed to retrieve subBlock with id %s and block id %s", subBlockId.toString, blockId.toString)
          prom failure f
      }
      prom.future
  })

  def retrieveBlock(blockMeta: BlockMeta): InputStream = {
    log.debug("retrieve Block %s", blockMeta.toString)
    BlockInputStream(this, blockMeta, configuration.atMost)
  }

  def deleteINode(path: Path): Future[GenericOpSuccess] = executeWithClient({
    client =>
      val pathKey = getPathKey(path)
      val iNodeColumnPath = new ColumnPath(INODE_COLUMN_FAMILY_NAME)
      val timestamp = System.currentTimeMillis

      val result = promise[GenericOpSuccess]()

      val deleteInodeFuture = AsyncUtil.executeAsync[remove_call](
        client.remove(pathKey, iNodeColumnPath, timestamp, configuration.writeConsistencyLevel, _))

      deleteInodeFuture.onSuccess {
        case p =>
          log.debug("deleted INode with path %s", path)
          result success GenericOpSuccess()
      }
      deleteInodeFuture.onFailure {
        case f =>
          log.error(f, "failed to delete INode with path %s", path)
          result failure f
      }
      result.future
  })

  def deleteBlocks(iNode: INode): Future[GenericOpSuccess] = executeWithClient({
    client =>
      val mutationMap = generateINodeMutationMap(iNode)

      val result = promise[GenericOpSuccess]()

      val deleteFuture = AsyncUtil.executeAsync[batch_mutate_call](
        client.batch_mutate(mutationMap, configuration.writeConsistencyLevel, _))
      deleteFuture.onSuccess {
        case p => {
          log.debug("deleted blocks for INode %s", iNode.toString)
          result success GenericOpSuccess()
        }
      }
      deleteFuture.onFailure {
        case f => {
          log.error(f, "failed to delete blocks for INode %s", iNode.toString)
          result failure f
        }
      }
      result.future
  })

  private def generateINodeMutationMap(iNode: INode): Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]] = {
    val timestamp = System.currentTimeMillis()
    val deletion = new Deletion()
    deletion.setTimestamp(timestamp)

    iNode.blocks.map {
      block =>
        (ByteBufferUtil.bytes(block.id), Map(BLOCK_COLUMN_FAMILY_NAME ->
          List(new Mutation().setDeletion(deletion)).asJava).asJava)
    }.toMap
  }

  def fetchSubPaths(path: Path, isDeepFetch: Boolean): Future[Set[Path]] = {
    val startPath = path.toUri.getPath
    val startPathBuffer = ByteBufferUtil.bytes(startPath)

    val sentinelIndexExpr = new IndexExpression(SENTINEL_COLUMN, IndexOperator.EQ, SENTINEL_VALUE)
    var startPathIndexExpr = new IndexExpression()
    var indexExpr = List[IndexExpression]()

    if (isDeepFetch) {
      startPathIndexExpr = new IndexExpression(PATH_COLUMN, IndexOperator.GT, startPathBuffer)
      if (startPath.length > 1) {
        indexExpr = indexExprForDeepFetch(startPath)
      }
    } else {
      startPathIndexExpr = new IndexExpression(PARENT_PATH_COLUMN, IndexOperator.EQ, startPathBuffer)
    }

    indexExpr = indexExpr ++ List(sentinelIndexExpr, startPathIndexExpr)

    log.debug("fetching subPaths for %s, %s ", path, if (isDeepFetch) "recursively" else "non-recursively")
    fetchPaths(indexExpr)
  }

  private def fetchPaths(indexExpr: List[IndexExpression]): Future[Set[Path]] = executeWithClient({
    client =>
      val pathPredicate = new SlicePredicate().setColumn_names(List(PATH_COLUMN))
      val iNodeParent = new ColumnParent(INODE_COLUMN_FAMILY_NAME)

      val indexClause = new IndexClause(indexExpr, ByteBufferUtil.EMPTY_BYTE_BUFFER, 100000)
      val rowFuture = AsyncUtil.executeAsync[get_indexed_slices_call](
        client.get_indexed_slices(iNodeParent, indexClause, pathPredicate, configuration.readConsistencyLevel, _))

      val result = promise[Set[Path]]()
      rowFuture.onSuccess {
        case p => {
          val paths = p.getResult.flatMap(keySlice =>
            keySlice.getColumns.map(columnOrSuperColumn =>
              new Path(ByteBufferUtil.string(columnOrSuperColumn.column.value)))
          ).toSet
          log.debug("fetched subpaths for %s", indexExpr.toString())
          result success paths
        }
      }
      rowFuture.onFailure {
        case f =>
          log.error(f, "failed to fetch subpaths for  %s", indexExpr.toString())
          result failure f
      }

      result.future
  })

  private def indexExprForDeepFetch(startPath: String): List[IndexExpression] = {
    val lastChar = (startPath(startPath.length - 1) + 1).asInstanceOf[Char]
    val endPath = startPath.substring(0, startPath.length - 1) + lastChar
    val endPathBuffer = ByteBufferUtil.bytes(endPath)
    val endPathIndexExpr = new IndexExpression(PATH_COLUMN, IndexOperator.LT, endPathBuffer)
    List(endPathIndexExpr)
  }


  def getBlockLocations(path: Path): Future[Map[BlockMeta, List[String]]] = executeWithClient({
    client =>

      val result = promise[Map[BlockMeta, List[String]]]()
      val inodeFuture = retrieveINode(path)

      var response = Map.empty[BlockMeta, List[String]]

      inodeFuture.onSuccess {
        case inode =>
          log.debug("found iNode for %s, getting block locations", path)
          //Get the ring description from the server
          val ringFuture = AsyncUtil.executeAsync[describe_ring_call](
            client.describe_ring(configuration.keySpace, _)
          )


          ringFuture.onSuccess {
            case r =>
              log.debug("fetched ring details for keyspace %", configuration.keySpace)
              val tf = partitioner.getTokenFactory
              val ring = r.getResult.map(p => (p.getEndpoints, p.getStart_token.toLong, p.getEnd_token.toLong))

              //For each block in the file, get the owner node
              inode.blocks.foreach(b => {
                val token = tf.fromByteArray(ByteBufferUtil.bytes(b.id))

                val xr = ring.filter {
                  p =>
                    if (p._2 < p._3) {
                      p._2 <= token.token && p._3 >= token.token
                    } else {
                      (p._2 <= token.token && Long.MaxValue >= token.token) || (p._3 >= token.token && Long.MinValue <= token.token)
                    }
                }


                if (xr.length > 0) {
                  val endpoints: List[String] = xr.flatMap(_._1).toList
                  response += (b -> endpoints)
                } else {
                  response += (b -> ring(0)._1.toList)
                }
              })
              log.debug("found block locations for iNode %s", path)
              result success response
          }

          ringFuture.onFailure {
            case f =>
              log.error(f, "failed to get ring details for keyspace %s", configuration.keySpace)
              result failure f
          }
      }

      inodeFuture.onFailure {
        case e =>
          log.error(e, "iNode for %s not found", path)
          result failure e
      }

      result.future
  })
}
