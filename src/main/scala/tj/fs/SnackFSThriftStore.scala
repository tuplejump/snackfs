package tj.fs

import tj.util.AsyncUtil

import org.apache.cassandra.thrift.Cassandra.AsyncClient
import org.apache.cassandra.thrift.Cassandra.AsyncClient.{system_add_keyspace_call, describe_keyspace_call}

import org.apache.cassandra.utils.ByteBufferUtil
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.cassandra.thrift.{IndexType, ColumnDef, CfDef, KsDef}
import scala.collection.JavaConversions._
import java.nio.ByteBuffer
import scala.util. Try
import tj.exceptions.KeyspaceAlreadyExistsException
import tj.model.Keyspace
import org.apache.cassandra.locator.SimpleStrategy

class SnackFSThriftStore(client:AsyncClient) extends SnackFSStore{

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

    val pathCol: ByteBuffer = ByteBufferUtil.bytes("path")
    val parentPathCol: ByteBuffer = ByteBufferUtil.bytes("parent_path")
    val sentCol: ByteBuffer = ByteBufferUtil.bytes("sentinel")

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

  def buildSchema(keyspace:String,replicationFactor:Int):KsDef={
    val inode = createINodeCF("inode", keyspace)
    val sblock = createSBlockCF("sblock", keyspace, 16, 64)

    val ksDef: KsDef = new KsDef(keyspace, classOf[SimpleStrategy].getCanonicalName,
      List(inode, sblock))
    ksDef.setStrategy_options(Map("replication_factor" -> replicationFactor.toString))

    ksDef
  }

}
