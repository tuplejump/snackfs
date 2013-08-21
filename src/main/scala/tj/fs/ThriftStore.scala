package tj.fs

import tj.util.AsyncUtil

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
import tj.exceptions.KeyspaceAlreadyExistsException
import tj.model._
import org.apache.cassandra.locator.SimpleStrategy
import org.apache.hadoop.fs.Path
import java.math.BigInteger
import java.util.UUID
import tj.model.SubBlockMeta
import tj.model.GenericOpSuccess
import java.io.InputStream

class ThriftStore(client: AsyncClient) extends FileSystemStore {


  private val pathCol: ByteBuffer = ByteBufferUtil.bytes("path")
  private val parentPathCol: ByteBuffer = ByteBufferUtil.bytes("parent_path")
  private val sentCol: ByteBuffer = ByteBufferUtil.bytes("sentinel")
  private val sentinelValue: ByteBuffer = ByteBufferUtil.bytes("x")
  private val dataCol: ByteBuffer = ByteBufferUtil.bytes("data")
  private val consistencyLevelWrite = ConsistencyLevel.QUORUM
  private val consistencyLevelRead = ConsistencyLevel.QUORUM

  def createKeyspace(ksDef: KsDef): Future[Keyspace] = {
    val prom = promise[Keyspace]()
    val ksDefFuture = AsyncUtil.executeAsync[describe_keyspace_call](client.describe_keyspace(ksDef.getName, _))

    ksDefFuture onSuccess {
      case p => {

        val mayBeKsDef: Try[KsDef] = Try(p.getResult)

        if (mayBeKsDef.isSuccess) {
          prom failure new KeyspaceAlreadyExistsException("%s keyspace already exists".format(ksDef.getName))
        } else {

          val response = AsyncUtil.executeAsync[system_add_keyspace_call](
            client.system_add_keyspace(ksDef, _))

          response.onSuccess {
            case r => prom success new Keyspace(r.getResult)
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

  private def generateMutationforINode(data: ByteBuffer, path: Path, timestamp: Long): Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]] = {
    val pathColMutation = createMutationForCol(pathCol, ByteBufferUtil.bytes(path.toUri.getPath), timestamp)
    val parentColMutation = createMutationForCol(parentPathCol, ByteBufferUtil.bytes(getParentForIndex(path)), timestamp)
    val sentinelMutation = createMutationForCol(sentCol, sentinelValue, timestamp)
    val dataMutation = createMutationForCol(dataCol, data, timestamp)
    val mutations: java.util.List[Mutation] = List(pathColMutation, parentColMutation, sentinelMutation, dataMutation)

    val pathMutation: java.util.Map[String, java.util.List[Mutation]] = Map("inode" -> mutations)
    val mutationMap: Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]] = Map(getPathKey(path) -> pathMutation)

    mutationMap
  }

  def storeINode(path: Path, iNode: INode): Future[GenericOpSuccess] = {
    val data: ByteBuffer = iNode.serialize
    val timestamp = iNode.timestamp
    val mutationMap: Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]] = generateMutationforINode(data, path, timestamp)
    val iNodeFuture = AsyncUtil.executeAsync[batch_mutate_call](client.batch_mutate(mutationMap, consistencyLevelWrite, _))
    val prom = promise[GenericOpSuccess]()
    iNodeFuture.onSuccess {
      case p => {
        prom success GenericOpSuccess()
      }
    }
    iNodeFuture.onFailure {
      case f => prom failure f
    }
    prom.future
  }

  private def performGet(key: ByteBuffer,
                         columnPath: ColumnPath,
                         consistency: ConsistencyLevel): Future[ColumnOrSuperColumn] = {

    val prom = promise[ColumnOrSuperColumn]()
    val getFuture = AsyncUtil.executeAsync[get_call](client.get(key, columnPath, consistency, _))
    getFuture.onSuccess {
      case p =>
        try {
          val res = p.getResult
          prom success res
        } catch {
          case e =>
            prom failure e
        }
    }
    getFuture.onFailure {
      case f => prom failure f
    }
    prom.future
  }

  def retrieveINode(path: Path): Future[INode] = {
    val pathKey: ByteBuffer = getPathKey(path)
    val inodeDataPath = new ColumnPath("inode").setColumn(dataCol)

    val inodePromise = promise[INode]()
    val pathInfo = performGet(pathKey, inodeDataPath, consistencyLevelRead)
    pathInfo.onSuccess {
      case p =>
        inodePromise success INode.deserialize(ByteBufferUtil.inputStream(p.column.value), p.column.getTimestamp)
    }
    pathInfo.onFailure {
      case f =>
        inodePromise failure f
    }
    inodePromise.future
  }

  def storeSubBlock(blockId: UUID, subBlockMeta: SubBlockMeta, data: ByteBuffer): Future[GenericOpSuccess] = {
    val parentBlockId: ByteBuffer = ByteBufferUtil.bytes(blockId)

    val sblockParent = new ColumnParent("sblock")

    val column = new Column()
      .setName(ByteBufferUtil.bytes(subBlockMeta.id))
      .setValue(data)
      .setTimestamp(System.currentTimeMillis)

    val prom = promise[GenericOpSuccess]()
    val subBlockFuture = AsyncUtil.executeAsync[insert_call](
      client.insert(parentBlockId, sblockParent, column, consistencyLevelWrite, _))

    subBlockFuture.onSuccess {
      case p => prom success GenericOpSuccess()
    }
    subBlockFuture.onFailure {
      case f => prom failure f
    }
    prom.future
  }

  //TODO change this to accept just the blockId and subBlockId
  def retrieveSubBlock(blockMeta: BlockMeta, subBlockMeta: SubBlockMeta, byteRangeStart: Long): Future[InputStream] = {
    val blockId: ByteBuffer = ByteBufferUtil.bytes(blockMeta.id)
    val subBlockId = ByteBufferUtil.bytes(subBlockMeta.id)
    val subBlockFuture = performGet(blockId, new ColumnPath("sblock").setColumn(subBlockId), consistencyLevelRead)
    val prom = promise[InputStream]()
    subBlockFuture.onSuccess {
      case p => prom success ByteBufferUtil.inputStream(p.column.value)
    }
    subBlockFuture.onFailure {
      case f => prom failure f
    }
    prom.future
  }

  def retrieveBlock(blockMeta: BlockMeta): InputStream = {
    BlockInputStream(this, blockMeta)
  }

  def deleteINode(path: Path): Future[GenericOpSuccess] = {
    val pathKey = getPathKey(path)
    val iNodeColumnPath = new ColumnPath("inode")
    val timestamp = System.currentTimeMillis

    val result = promise[GenericOpSuccess]()

    val deleteInodeFuture = AsyncUtil.executeAsync[remove_call](
      client.remove(pathKey, iNodeColumnPath, timestamp, consistencyLevelWrite, _))

    deleteInodeFuture.onSuccess {
      case p => result success GenericOpSuccess()
    }
    deleteInodeFuture.onFailure {
      case f => result failure f
    }
    result.future
  }

  def deleteBlocks(inode: INode): Future[GenericOpSuccess] = {
    val timestamp = System.currentTimeMillis()
    val deletion = new Deletion()
    deletion.setTimestamp(timestamp)

    val mmap: Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]] = inode.blocks.map {
      block =>
        (ByteBufferUtil.bytes(block.id), Map("sblock" -> List(new Mutation().setDeletion(deletion)).asJava).asJava)
    }.toMap

    val result = promise[GenericOpSuccess]()

    val deleteFuture = AsyncUtil.executeAsync[batch_mutate_call](client.batch_mutate(mmap, consistencyLevelWrite, _))
    deleteFuture.onSuccess {
      case p => {
        result success GenericOpSuccess()
      }
    }
    deleteFuture.onFailure {
      case f => {
        result failure f
      }
    }
    result.future
  }

}
