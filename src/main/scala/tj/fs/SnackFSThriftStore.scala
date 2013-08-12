package tj.fs

import tj.util.AsyncUtil

import org.apache.cassandra.thrift.Cassandra.AsyncClient
import org.apache.cassandra.thrift.Cassandra.AsyncClient._

import org.apache.cassandra.utils.{FBUtilities, ByteBufferUtil}
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.cassandra.thrift._
import scala.collection.JavaConversions._
import java.nio.ByteBuffer
import scala.util.Try
import tj.exceptions.KeyspaceAlreadyExistsException
import tj.model._
import org.apache.cassandra.locator.SimpleStrategy
import org.apache.hadoop.fs.Path
import java.math.BigInteger
import java.util.UUID
import org.xerial.snappy.Snappy
import tj.model.SubBlockMeta
import tj.model.GenericOpSuccess
import java.io.InputStream

class SnackFSThriftStore(client: AsyncClient) extends SnackFSStore {

  private val pathCol: ByteBuffer = ByteBufferUtil.bytes("path")
  private val parentPathCol: ByteBuffer = ByteBufferUtil.bytes("parent_path")
  private val sentCol: ByteBuffer = ByteBufferUtil.bytes("sentinel")
  private val sentinelValue: ByteBuffer = ByteBufferUtil.bytes("x")
  private val dataCol: ByteBuffer = ByteBufferUtil.bytes("data")
  private val consistencyLevelWrite = ConsistencyLevel.LOCAL_QUORUM
  private val consistencyLevelRead = ConsistencyLevel.QUORUM

  def createKeyspace(ksDef: KsDef): Future[Keyspace] = {
    val prom = promise[Keyspace]
    val ksDefFuture = AsyncUtil.executeAsync[describe_keyspace_call](client.describe_keyspace(ksDef.getName, _))

    ksDefFuture onSuccess {
      case p => {

        val mayBeKsDef: Try[KsDef] = Try(p.getResult())

        if (mayBeKsDef.isSuccess) {
          prom failure new KeyspaceAlreadyExistsException("%s keyspace already exists".format(ksDef.getName))
        } else {

          val response = AsyncUtil.executeAsync[system_add_keyspace_call](
            client.system_add_keyspace(ksDef, _))

          response.onSuccess {
            case r => prom success new Keyspace(r.getResult())
          }

          response.onFailure {
            case f => prom failure f
          }
        }
      }
    }
    prom.future
  }

  private def createINodeCF(cfName: String, keyspace: String) = {

    val pathIndex = "path"
    val sentinelIndex = "sentinel"
    val parentPathIndex = "parent_path"

    val dataType: String = "BytesType"

    val columnFamily = new CfDef()
    columnFamily.setName(cfName)
    columnFamily.setComparator_type(dataType)
    columnFamily.setGc_grace_seconds(60)
    columnFamily.setComment("Stores file meta data")
    columnFamily.setKeyspace(keyspace)

    val path = new ColumnDef(pathCol, dataType).
      setIndex_type(IndexType.KEYS).
      setIndex_name(pathIndex)

    val sentinel = new ColumnDef(sentCol, dataType).
      setIndex_type(IndexType.KEYS).
      setIndex_name(sentinelIndex)

    val parentPath = new ColumnDef(parentPathCol, dataType).
      setIndex_type(IndexType.KEYS).
      setIndex_name(parentPathIndex)

    val metadata = List(path, sentinel, parentPath)
    columnFamily.setColumn_metadata(metadata)

    columnFamily
  }

  private def createSBlockCF(cfName: String, keyspace: String, minCompaction: Int, maxCompaction: Int) = {

    val columnFamily = new CfDef()
    columnFamily.setName(cfName)
    columnFamily.setComparator_type("BytesType")
    columnFamily.setGc_grace_seconds(60)
    columnFamily.setComment("Stores blocks of information associated with a inode")
    columnFamily.setKeyspace(keyspace)

    columnFamily.setMin_compaction_threshold(minCompaction)
    columnFamily.setMax_compaction_threshold(maxCompaction)

    columnFamily
  }

  def buildSchema(keyspace: String, replicationFactor: Int): KsDef = {
    val inode = createINodeCF("inode", keyspace)
    val sblock = createSBlockCF("sblock", keyspace, 16, 64)

    val ksDef: KsDef = new KsDef(keyspace, classOf[SimpleStrategy].getCanonicalName,
      List(inode, sblock))
    ksDef.setStrategy_options(Map("replication_factor" -> replicationFactor.toString))

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

  private def generateMutationforINode(data: ByteBuffer, path: Path): Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]] = {
    val timestamp = System.currentTimeMillis()

    val pathColMutation = createMutationForCol(pathCol, ByteBufferUtil.bytes(path.toUri.getPath), timestamp)
    val parentColMutation = createMutationForCol(parentPathCol, ByteBufferUtil.bytes(getParentForIndex(path)), timestamp)
    val sentinelMutation = createMutationForCol(sentCol, sentinelValue, timestamp)
    val dataMutation = createMutationForCol(dataCol, data, timestamp)
    val mutations: java.util.List[Mutation] = List(pathColMutation, parentColMutation, sentinelMutation, dataMutation)

    val pathMutation: java.util.Map[String, java.util.List[Mutation]] = Map("inode" -> mutations)
    val mutationMap: Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]] = Map(getPathKey(path) -> pathMutation)

    mutationMap
  }

  def saveINode(path: Path, iNode: INode): GenericOpSuccess = {
    val data: ByteBuffer = iNode.serialize
    val mutationMap: Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]] = generateMutationforINode(data, path)
    val iNodeFuture = AsyncUtil.executeAsync[batch_mutate_call](client.batch_mutate(mutationMap, consistencyLevelWrite, _))
    var result: GenericOpSuccess = null
    iNodeFuture.onSuccess {
      case p => result = GenericOpSuccess()
    }
    result
  }

  def performGet(key: ByteBuffer, columnPath: ColumnPath, consistency: ConsistencyLevel): ColumnOrSuperColumn = {
    val getFuture = AsyncUtil.executeAsync[get_call](client.get(key, columnPath, consistency, _))
    var result: ColumnOrSuperColumn = null
    getFuture.onSuccess {
      case p => result = p.getResult
    }
    result
  }

  def retrieveINode(path: Path): INode = {
    val pathKey: ByteBuffer = getPathKey(path)
    val inodeDataPath = new ColumnPath("inode").setColumn(dataCol)

    val pathInfo = performGet(pathKey, inodeDataPath, consistencyLevelRead)

    /*  // If not found and I already tried with CL= ONE, retry with higher CL.
      if (pathInfo == null && (consistencyLevelRead == ConsistencyLevel.ONE)) {
        pathInfo = performGet(pathKey, inodeDataPath, ConsistencyLevel.QUORUM)
      }*/

    if (pathInfo == null) {
      return null
    }
    INode.deserilize(ByteBufferUtil.inputStream(pathInfo.column.value), pathInfo.column.getTimestamp)
  }

  private def compressData(data: ByteBuffer): ByteBuffer = {
    //Prepare the buffer to hold the compressed data
    val maxCapacity: Int = Snappy.maxCompressedLength(data.capacity)
    val compressedData = ByteBuffer.allocateDirect(maxCapacity)
    compressedData.limit(compressedData.capacity)
    compressedData.rewind

    //compress
    val len: Int = Snappy.compress(data, compressedData)
    compressedData.limit(len)
    compressedData.rewind
    compressedData
  }

  def saveSubBlock(blockId: UUID, subBlockMeta: SubBlockMeta, data: ByteBuffer): GenericOpSuccess = {
    val parentBlockId: ByteBuffer = ByteBufferUtil.bytes(blockId)

    val compressedData = compressData(data)

    val sblockParent = new ColumnParent("sblock")

    val column = new Column()
      .setName(ByteBufferUtil.bytes(subBlockMeta.id))
      .setValue(compressedData)
      .setTimestamp(System.currentTimeMillis)

    val subBlockFuture = AsyncUtil.executeAsync[insert_call](
      client.insert(parentBlockId, sblockParent, column, consistencyLevelWrite, _))

    var result: GenericOpSuccess = null
    subBlockFuture.onSuccess {
      case p => result = GenericOpSuccess()
    }
    result
  }

  def retrieveSubBlock(blockMeta: BlockMeta, subBlockMeta: SubBlockMeta, byteRangeStart: Long): InputStream= {
    val blockId:ByteBuffer = ByteBufferUtil.bytes(blockMeta.id)
    val subBlockId = ByteBufferUtil.bytes (subBlockMeta.id)
    val subBlock = performGet(subBlockId,new ColumnPath("sblock").setColumn(blockId),consistencyLevelRead)
    ByteBufferUtil.inputStream(subBlock.column.value)
  }
}
