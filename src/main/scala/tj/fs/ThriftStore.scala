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


  private val PATH_COLUMN: ByteBuffer = ByteBufferUtil.bytes("path")
  private val PARENT_PATH_COLUMN: ByteBuffer = ByteBufferUtil.bytes("parent_path")
  private val SENTINEL_COLUMN: ByteBuffer = ByteBufferUtil.bytes("sentinel")
  private val SENTINEL_VALUE: ByteBuffer = ByteBufferUtil.bytes("x")
  private val DATA_COLUMN: ByteBuffer = ByteBufferUtil.bytes("data")
  private val CONSISTENCY_LEVEL_WRITE = ConsistencyLevel.QUORUM
  private val CONSISTENCY_LEVEL_READ = ConsistencyLevel.QUORUM

  private val INODE_COLUMN_FAMILY_NAME = "inode"
  private val BLOCK_COLUMN_FAMILY_NAME = "sblock"

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

    val PATH_INDEX_LABEL = "path"
    val SENTINEL_INDEX_LABEL = "sentinel"
    val PARENT_PATH_INDEX_LABEL = "parent_path"

    val DATA_TYPE = "BytesType"

    val columnFamily = new CfDef()
    columnFamily.setName(cfName)
    columnFamily.setComparator_type(DATA_TYPE)
    columnFamily.setGc_grace_seconds(60)
    columnFamily.setComment("Stores file meta data")
    columnFamily.setKeyspace(keyspace)

    val path = new ColumnDef(PATH_COLUMN, DATA_TYPE).
      setIndex_type(IndexType.KEYS).
      setIndex_name(PATH_INDEX_LABEL)

    val sentinel = new ColumnDef(SENTINEL_COLUMN, DATA_TYPE).
      setIndex_type(IndexType.KEYS).
      setIndex_name(SENTINEL_INDEX_LABEL)

    val parentPath = new ColumnDef(PARENT_PATH_COLUMN, DATA_TYPE).
      setIndex_type(IndexType.KEYS).
      setIndex_name(PARENT_PATH_INDEX_LABEL)

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
    val inode = createINodeCF(INODE_COLUMN_FAMILY_NAME, keyspace)
    val sblock = createSBlockCF(BLOCK_COLUMN_FAMILY_NAME, keyspace, 16, 64)

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
    val pathColMutation = createMutationForCol(PATH_COLUMN, ByteBufferUtil.bytes(path.toUri.getPath), timestamp)
    val parentColMutation = createMutationForCol(PARENT_PATH_COLUMN, ByteBufferUtil.bytes(getParentForIndex(path)), timestamp)
    val sentinelMutation = createMutationForCol(SENTINEL_COLUMN, SENTINEL_VALUE, timestamp)
    val dataMutation = createMutationForCol(DATA_COLUMN, data, timestamp)
    val mutations: java.util.List[Mutation] = List(pathColMutation, parentColMutation, sentinelMutation, dataMutation)

    val pathMutation: java.util.Map[String, java.util.List[Mutation]] = Map(INODE_COLUMN_FAMILY_NAME -> mutations)
    val mutationMap: Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]] = Map(getPathKey(path) -> pathMutation)

    mutationMap
  }

  def storeINode(path: Path, iNode: INode): Future[GenericOpSuccess] = {
    val data: ByteBuffer = iNode.serialize
    val timestamp = iNode.timestamp
    val mutationMap: Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]] = generateMutationforINode(data, path, timestamp)
    val iNodeFuture = AsyncUtil.executeAsync[batch_mutate_call](client.batch_mutate(mutationMap, CONSISTENCY_LEVEL_WRITE, _))
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
    val inodeDataPath = new ColumnPath(INODE_COLUMN_FAMILY_NAME).setColumn(DATA_COLUMN)

    val inodePromise = promise[INode]()
    val pathInfo = performGet(pathKey, inodeDataPath, CONSISTENCY_LEVEL_READ)
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

    val sblockParent = new ColumnParent(BLOCK_COLUMN_FAMILY_NAME)

    val column = new Column()
      .setName(ByteBufferUtil.bytes(subBlockMeta.id))
      .setValue(data)
      .setTimestamp(System.currentTimeMillis)

    val prom = promise[GenericOpSuccess]()
    val subBlockFuture = AsyncUtil.executeAsync[insert_call](
      client.insert(parentBlockId, sblockParent, column, CONSISTENCY_LEVEL_WRITE, _))

    subBlockFuture.onSuccess {
      case p => prom success GenericOpSuccess()
    }
    subBlockFuture.onFailure {
      case f => prom failure f
    }
    prom.future
  }

  def retrieveSubBlock(blockId: UUID, subBlockId: UUID, byteRangeStart: Long): Future[InputStream] = {
    val blockIdBuffer: ByteBuffer = ByteBufferUtil.bytes(blockId)
    val subBlockIdBuffer = ByteBufferUtil.bytes(subBlockId)
    val subBlockFuture = performGet(blockIdBuffer, new ColumnPath(BLOCK_COLUMN_FAMILY_NAME).setColumn(subBlockIdBuffer), CONSISTENCY_LEVEL_READ)
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
    val iNodeColumnPath = new ColumnPath(INODE_COLUMN_FAMILY_NAME)
    val timestamp = System.currentTimeMillis

    val result = promise[GenericOpSuccess]()

    val deleteInodeFuture = AsyncUtil.executeAsync[remove_call](
      client.remove(pathKey, iNodeColumnPath, timestamp, CONSISTENCY_LEVEL_WRITE, _))

    deleteInodeFuture.onSuccess {
      case p => result success GenericOpSuccess()
    }
    deleteInodeFuture.onFailure {
      case f => result failure f
    }
    result.future
  }

  def deleteBlocks(iNode: INode): Future[GenericOpSuccess] = {
    val timestamp = System.currentTimeMillis()
    val deletion = new Deletion()
    deletion.setTimestamp(timestamp)

    val mutationMap: Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]] = iNode.blocks.map {
      block =>
        (ByteBufferUtil.bytes(block.id), Map(BLOCK_COLUMN_FAMILY_NAME ->
          List(new Mutation().setDeletion(deletion)).asJava).asJava)
    }.toMap

    val result = promise[GenericOpSuccess]()

    val deleteFuture = AsyncUtil.executeAsync[batch_mutate_call](client.batch_mutate(mutationMap, CONSISTENCY_LEVEL_WRITE, _))
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

  def fetchSubPaths(path: Path): Future[Set[Path]] = {
    val startPath = path.toUri.getPath
    val startPathBuffer = ByteBufferUtil.bytes(startPath)

    val sentinelIndexExpr = new IndexExpression(SENTINEL_COLUMN, IndexOperator.EQ, SENTINEL_VALUE)
    val startPathIndexExpr = new IndexExpression(PATH_COLUMN, IndexOperator.GT, startPathBuffer)

    var indexExpr = List(sentinelIndexExpr, startPathIndexExpr)

    if (startPath.length > 1) {
      val lastChar = (startPath(startPath.length - 1) + 1).asInstanceOf[Char]
      val endPath = startPath.substring(0, startPath.length - 1) + lastChar
      val endPathBuffer = ByteBufferUtil.bytes(endPath)
      val endPathIndexExpr = new IndexExpression(PATH_COLUMN, IndexOperator.LT, endPathBuffer)
      indexExpr = indexExpr :+ endPathIndexExpr
    }

    val pathPredicate = new SlicePredicate().setColumn_names(List(PATH_COLUMN))
    val iNodeParent = new ColumnParent(INODE_COLUMN_FAMILY_NAME)

    val indexClause = new IndexClause(indexExpr, ByteBufferUtil.EMPTY_BYTE_BUFFER, 100000)
    val rowFuture = AsyncUtil.executeAsync[get_indexed_slices_call](
      client.get_indexed_slices(iNodeParent, indexClause, pathPredicate, CONSISTENCY_LEVEL_READ, _))

    val result = promise[Set[Path]]()
    rowFuture.onSuccess {
      case p => {
        val paths = p.getResult.flatMap(keySlice =>
          keySlice.getColumns.map(columnOrSuperColumn =>
            new Path(ByteBufferUtil.string(columnOrSuperColumn.column.value)))
        ).toSet
        result success paths
      }
    }
    rowFuture.onFailure {
      case f => result failure f
    }

    result.future
  }
}
