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
package com.tuplejump.snackfs.cassandra.store

import org.apache.cassandra.thrift.Cassandra.Client

import org.apache.cassandra.utils.{UUIDGen, FBUtilities, ByteBufferUtil}
import org.apache.cassandra.thrift._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.nio.ByteBuffer
import scala.util.{Failure, Success, Try}
import org.apache.hadoop.fs.Path
import java.math.BigInteger
import java.util.UUID
import java.io.InputStream
import com.tuplejump.snackfs.util.LogConfiguration
import org.apache.cassandra.dht.Murmur3Partitioner
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.commons.pool.ObjectPool
import org.apache.commons.pool.impl.StackObjectPool

import com.twitter.logging.Logger
import com.tuplejump.snackfs.fs.model._
import com.tuplejump.snackfs.cassandra.model._
import com.tuplejump.snackfs.cassandra.partial.FileSystemStore
import com.tuplejump.snackfs.fs.model.SubBlockMeta
import com.tuplejump.snackfs.cassandra.model.ThriftClientAndSocket
import com.tuplejump.snackfs.fs.stream.BlockInputStream
import com.tuplejump.snackfs.fs.model.BlockMeta
import com.tuplejump.snackfs.cassandra.model.GenericOpSuccess
import com.tuplejump.snackfs.cassandra.model.Keyspace

class ThriftStore(configuration: SnackFSConfiguration) extends FileSystemStore {

  LogConfiguration.config()

  private lazy val log = Logger.get(getClass)
  private val PATH_COLUMN: ByteBuffer = ByteBufferUtil.bytes("path")
  private val PARENT_PATH_COLUMN: ByteBuffer = ByteBufferUtil.bytes("parent_path")
  private val SENTINEL_COLUMN: ByteBuffer = ByteBufferUtil.bytes("sentinel")
  private val SENTINEL_VALUE: ByteBuffer = ByteBufferUtil.bytes("x")
  private val DATA_COLUMN: ByteBuffer = ByteBufferUtil.bytes("data")
  private val GRACE_SECONDS: Int = 60

  private val INODE_COLUMN_FAMILY_NAME = "inode"
  private val BLOCK_COLUMN_FAMILY_NAME = "sblock"

  private val LOCK_COLUMN_FAMILY_NAME = "createlock"

  private val partitioner = new Murmur3Partitioner()

  private var clientPool: ObjectPool[ThriftClientAndSocket] = _

  private def executeWithClient[T](fn: Client => T)(implicit tm: ClassManifest[T]): T = {
    log.debug("Fetching client from pool")
    val thriftClientAndSocket = clientPool.borrowObject()
    val ret = fn(thriftClientAndSocket.client)
    clientPool.returnObject(thriftClientAndSocket)
    ret
  }

  /**
   * Creates the keyspace required for SnackFS.
   * It does not use executeWithClient since the keyspace is not already there
   * @return
   */
  def createKeyspace: Try[Keyspace] = {

    val protocolFactory = new TBinaryProtocol.Factory()
    val clientFactory = new Client.Factory()

    val socket = new TSocket(configuration.CassandraHost, configuration.CassandraPort)
    val transport = new TFramedTransport(socket)
    transport.open()
    val client = clientFactory.getClient(protocolFactory.getProtocol(transport))

    val ksDef = buildSchema
    val mayBeKsDef = Try(client.describe_keyspace(ksDef.getName))

    val ret: Try[Keyspace] = mayBeKsDef map {
      kd =>
        log.debug("Using existing keyspace %s", ksDef.getName)
        new Keyspace(ksDef.getName)
    } recover {
      case ex: Throwable =>
        log.debug("Creating new keyspace %s", ksDef.getName)
        val r = client.system_add_keyspace(ksDef)

        log.debug("Created keyspace %s", ksDef.getName)
        new Keyspace(r)
    }
    transport.close()
    socket.close()
    ret
  }

  def init {
    log.debug("initializing thrift store with configuration %s", configuration.toString)

    val useMultinode = System.getProperty("com.tuplejump.snackfs.usemultinode") == "true"
    log.debug("Using multinode: %s", useMultinode)

    clientPool = new StackObjectPool[ThriftClientAndSocket](
      new ClientPoolFactory(configuration.CassandraHost, configuration.CassandraPort, configuration.keySpace, useMultinode)) {
      override def close() {
        super.close()
      }
    }
  }

  def dropKeyspace: Try[String] = executeWithClient({
    client =>
      val mayBeDropped = Try(client.system_drop_keyspace(configuration.keySpace))
      mayBeDropped
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

  private def createLockCF(cfName: String, minCompaction: Int, maxCompaction: Int) = {

    val columnFamily = new CfDef()
    columnFamily.setName(cfName)
    columnFamily.setComparator_type("UUIDType")
    columnFamily.setGc_grace_seconds(GRACE_SECONDS)
    columnFamily.setComment("Stores information about which process is trying to write a inode")
    columnFamily.setKeyspace(configuration.keySpace)

    columnFamily.setMin_compaction_threshold(minCompaction)
    columnFamily.setMax_compaction_threshold(maxCompaction)

    columnFamily
  }

  private def buildSchema: KsDef = {
    val MIN_COMPACTION = 16
    val MAX_COMPACTION = 64
    val inode = createINodeCF(INODE_COLUMN_FAMILY_NAME)
    val sblock = createSBlockCF(BLOCK_COLUMN_FAMILY_NAME, MIN_COMPACTION, MAX_COMPACTION)

    val createLock = createLockCF(LOCK_COLUMN_FAMILY_NAME, MIN_COMPACTION, MAX_COMPACTION)

    val ksDef: KsDef = new KsDef(configuration.keySpace,
      configuration.replicationStrategy,
      List(inode, sblock, createLock))

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

  def storeINode(path: Path, iNode: INode): Try[GenericOpSuccess] = executeWithClient({
    client =>
      val data: ByteBuffer = iNode.serialize
      val timestamp = iNode.timestamp
      val mutationMap: Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]] = generateMutationforINode(data, path, timestamp)
      val mayBeINode = Try(client.batch_mutate(mutationMap, configuration.writeConsistencyLevel))
      mayBeINode map {
        u =>
          log.debug("stored INode %s", iNode.toString)
          GenericOpSuccess()
      }
  })

  private def performGet(client: Client, key: ByteBuffer, columnPath: ColumnPath): Try[ColumnOrSuperColumn] = {
    val mayBeColumn = Try(client.get(key, columnPath, configuration.readConsistencyLevel))
    mayBeColumn
  }

  def retrieveINode(path: Path): Try[INode] = executeWithClient({
    client =>
      val pathKey: ByteBuffer = getPathKey(path)
      val inodeDataPath = new ColumnPath(INODE_COLUMN_FAMILY_NAME).setColumn(DATA_COLUMN)

      log.debug("fetching Inode for path %s", path)
      val mayBePathInfo = performGet(client, pathKey, inodeDataPath)

      val result: Try[INode] = mayBePathInfo map {
        col: ColumnOrSuperColumn =>
          log.debug("retrieved Inode for path %s", path)
          INode.deserialize(ByteBufferUtil.inputStream(col.column.value), col.column.getTimestamp)
      }
      result
  })

  def storeSubBlock(blockId: UUID, subBlockMeta: SubBlockMeta, data: ByteBuffer): Try[GenericOpSuccess] = executeWithClient({
    client =>
      val parentBlockId: ByteBuffer = ByteBufferUtil.bytes(blockId)

      val sblockParent = new ColumnParent(BLOCK_COLUMN_FAMILY_NAME)

      val column = new Column()
        .setName(ByteBufferUtil.bytes(subBlockMeta.id))
        .setValue(data)
        .setTimestamp(System.currentTimeMillis)

      val mayBeSubBlock = Try(client.insert(parentBlockId, sblockParent, column, configuration.writeConsistencyLevel))

      mayBeSubBlock map {
        u =>
          log.debug("stored subBlock %s for block with id %s", subBlockMeta.toString, blockId.toString)
          GenericOpSuccess()
      }
  })

  def retrieveSubBlock(blockId: UUID, subBlockId: UUID, byteRangeStart: Long): Try[InputStream] = executeWithClient({
    client =>
      val blockIdBuffer: ByteBuffer = ByteBufferUtil.bytes(blockId)
      val subBlockIdBuffer = ByteBufferUtil.bytes(subBlockId)
      log.debug("fetching subBlock for path %s", subBlockId.toString)

      val mayBeSubBlock = performGet(client, blockIdBuffer, new ColumnPath(BLOCK_COLUMN_FAMILY_NAME).setColumn(subBlockIdBuffer))

      mayBeSubBlock.map {
        col: ColumnOrSuperColumn =>
          val stream: InputStream = ByteBufferUtil.inputStream(col.column.value)
          log.debug("retrieved subBlock with id %s and block id %s", subBlockId.toString, blockId.toString)
          stream
      }
  })

  def retrieveBlock(blockMeta: BlockMeta): InputStream = {
    log.debug("retrieve Block %s", blockMeta.toString)
    BlockInputStream(this, blockMeta, configuration.atMost)
  }

  def deleteINode(path: Path): Try[GenericOpSuccess] = executeWithClient({
    client =>
      val pathKey = getPathKey(path)
      val iNodeColumnPath = new ColumnPath(INODE_COLUMN_FAMILY_NAME)
      val timestamp = System.currentTimeMillis

      val mayBeDeleted = Try(client.remove(pathKey, iNodeColumnPath, timestamp, configuration.writeConsistencyLevel))

      mayBeDeleted map {
        u =>
          log.debug("deleted INode with path %s", path)
          GenericOpSuccess()
      }

  })

  def deleteBlocks(iNode: INode): Try[GenericOpSuccess] = executeWithClient({
    client =>
      val mutationMap = generateINodeMutationMap(iNode)

      val mayBeDeleted = Try(
        client.batch_mutate(mutationMap, configuration.writeConsistencyLevel))

      mayBeDeleted.map {
        u =>
          log.debug("deleted blocks for INode %s", iNode.toString)
          GenericOpSuccess()
      }
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

  def fetchSubPaths(path: Path, isDeepFetch: Boolean): Try[Set[Path]] = {
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

    def recursionStrategy: String = {
      if (isDeepFetch) {
        "recursively"
      } else {
        "non-recursively"
      }
    }

    log.debug("fetching subPaths for %s, %s ", path, recursionStrategy)
    fetchPaths(indexExpr)
  }

  private def fetchPaths(indexExpr: List[IndexExpression]): Try[Set[Path]] = executeWithClient({
    client =>
      val pathPredicate = new SlicePredicate().setColumn_names(List(PATH_COLUMN))
      val iNodeParent = new ColumnParent(INODE_COLUMN_FAMILY_NAME)

      val keyRange = new KeyRange().setRow_filter(indexExpr).setCount(100000)
      keyRange.setStart_key(ByteBufferUtil.EMPTY_BYTE_BUFFER)
      keyRange.setEnd_key(ByteBufferUtil.EMPTY_BYTE_BUFFER)

      val mayBeRow = Try(
        client.get_range_slices(iNodeParent, pathPredicate, keyRange, configuration.readConsistencyLevel))

      mayBeRow map {
        col =>
          val paths = col.flatMap(keySlice =>
            keySlice.getColumns.map(columnOrSuperColumn =>
              new Path(ByteBufferUtil.string(columnOrSuperColumn.column.value)))
          ).toSet
          log.debug("fetched subpaths for %s", indexExpr.toString())
          paths
      }
  })

  private def indexExprForDeepFetch(startPath: String): List[IndexExpression] = {
    val lastChar = (startPath(startPath.length - 1) + 1).asInstanceOf[Char]
    val endPath = startPath.substring(0, startPath.length - 1) + lastChar
    val endPathBuffer = ByteBufferUtil.bytes(endPath)
    val endPathIndexExpr = new IndexExpression(PATH_COLUMN, IndexOperator.LT, endPathBuffer)
    List(endPathIndexExpr)
  }


  def getBlockLocations(path: Path): Try[Map[BlockMeta, List[String]]] = executeWithClient({
    client =>

      val mayBeINode = retrieveINode(path)

      var response = Map.empty[BlockMeta, List[String]]
      mayBeINode flatMap {
        iNode =>
          log.debug("found iNode for %s, getting block locations", path)
          //Get the ring description from the server
          val mayBeRing = Try(client.describe_ring(configuration.keySpace))
          mayBeRing map {
            r =>
              log.debug("fetched ring details for keyspace %s", configuration.keySpace)
              val tf = partitioner.getTokenFactory
              val ring = r.map(p => (p.getEndpoints, p.getStart_token.toLong, p.getEnd_token.toLong))

              //For each block in the file, get the owner node
              iNode.blocks.foreach(b => {
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
              response
          }
      }
  })

  /* Lock for writing a file
   *
   * Use case
   * one more process of the same cluster attempt writing to the same file
   * within a very small fraction of time lag
   *
   * Algorithm
   * 1. Write a column with name timeUUID and value as processId for given file path(rowId).
   * 2. Read back all columns for path
   *      case 1) count>=1 && firstEntry.value == processId
   *                   Got the lock
   *      case 2) No lock
   * 3. Do something in your code assuming the row is locked
   * 4. Release the lock by deleting the row
   *
   */

  private def addLockColumn(path: Path, processId: UUID, client: Client): Try[GenericOpSuccess] = {
    val key = getPathKey(path)
    val columnParent = new ColumnParent(LOCK_COLUMN_FAMILY_NAME)

    val timeStamp = UUIDGen.getTimeUUID

    val column = new Column()
      .setName(ByteBufferUtil.bytes(timeStamp))
      .setValue(ByteBufferUtil.bytes(processId))
      .setTimestamp(System.currentTimeMillis())

    val mayBeAdded = Try(client.insert(key, columnParent, column, ConsistencyLevel.QUORUM))
    mayBeAdded map {
      u =>
        log.debug("adding column")
        GenericOpSuccess()
    }
  }

  private def getLockRow(path: Path, client: Client): Try[java.util.List[ColumnOrSuperColumn]] = {
    val key = getPathKey(path)
    val columnParent = new ColumnParent(LOCK_COLUMN_FAMILY_NAME)
    val sliceRange = new SliceRange().setStart(Array[Byte]()).setFinish(Array[Byte]())
    val slicePredicate = new SlicePredicate().setColumn_names(null).setSlice_range(sliceRange)

    val mayBeRow = Try(client.get_slice(key, columnParent, slicePredicate, ConsistencyLevel.QUORUM))
    mayBeRow
  }

  private def isCreator(processId: UUID, columns: java.util.List[ColumnOrSuperColumn]): Boolean = {
    var result = false
    log.debug("checking for access to create a file for %s", processId)

    if (columns.length >= 1) {
      val firstEntry = columns.head.getColumn
      val entryIdString: String = new String(firstEntry.getValue)
      val processIdString: String = new String(ByteBufferUtil.bytes(processId).array())
      log.debug("value found %s", entryIdString)
      log.debug("given value %s", processIdString)

      if (entryIdString == processIdString) {
        result = true
      }
    }
    result
  }

  def acquireFileLock(path: Path, processId: UUID): Boolean = executeWithClient({
    client =>
      log.debug("adding column for create lock")
      val mayBeAddedColumn = addLockColumn(path, processId, client)
      mayBeAddedColumn match {
        case Success(s) =>
          log.debug("added column for create lock")
          val mayBeRow = getLockRow(path, client)
          log.debug("getting row for create lock")
          mayBeRow match {
            case Success(rowData) =>
              isCreator(processId, rowData)
            case Failure(e) =>
              log.error(e, "failure in the fetching creator details to acquire lock for " + path)
              throw e
          }
        case Failure(e) =>
          log.error(e, "failure in adding column to acquire lock for " + path)
          throw e
      }
  })

  private def deleteLockRow(path: Path, client: Client): Try[Unit] = {
    val columnPath = new ColumnPath(LOCK_COLUMN_FAMILY_NAME)
    val timestamp = System.currentTimeMillis

    val mayBeDeleted = Try(client.remove(getPathKey(path), columnPath, timestamp, ConsistencyLevel.QUORUM))

    mayBeDeleted
  }

  def releaseFileLock(path: Path): Boolean = executeWithClient({
    client =>
      val mayBeDeletedLock = deleteLockRow(path, client)
      mayBeDeletedLock match {
        case Success(status) =>
          log.debug("deleted lock")
          true
        case Failure(e) =>
          log.debug("failed to delete lock for %s", path)
          false
      }
  })

}
