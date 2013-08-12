package tj.fs

import org.apache.thrift.async.TAsyncClientManager
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TNonblockingSocket
import scala.concurrent.duration._
import scala.concurrent.Await

import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import tj.exceptions.KeyspaceAlreadyExistsException
import tj.model._
import org.apache.cassandra.thrift.Cassandra.AsyncClient
import tj.util.AsyncUtil
import org.apache.cassandra.thrift.Cassandra.AsyncClient.{set_keyspace_call, system_drop_keyspace_call}
import org.apache.hadoop.fs.permission.FsPermission
import java.util.UUID
import tj.model.SubBlockMeta
import java.net.URI
import org.apache.hadoop.fs.Path
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.cassandra.utils.ByteBufferUtil
import java.io.{ByteArrayOutputStream, File}
import java.nio.{MappedByteBuffer, ByteBuffer}

class SnackFSThriftStoreSpec extends FlatSpec with BeforeAndAfterAll {

  val clientManager = new TAsyncClientManager()
  val protocolFactory = new TBinaryProtocol.Factory()
  val transport = new TNonblockingSocket("127.0.0.1", 9160)

  def client = new AsyncClient(protocolFactory, clientManager, transport)

  val store = new SnackFSThriftStore(client)

  val timestamp = System.currentTimeMillis()
  val subBlocks = List(SubBlockMeta(UUID.randomUUID, 0, 128), SubBlockMeta(UUID.randomUUID, 128, 128))
  val block1 = BlockMeta(UUID.randomUUID, 0, 256, subBlocks)
  val block2 = BlockMeta(UUID.randomUUID, 0, 256, subBlocks)
  val blocks = List(block1, block2)
  val pathURI= URI.create("/home/shiti/Downloads/jquery.fixedheadertable.min.js")
  val path = new Path(pathURI)
  val iNode = INode("user", "group", FsPermission.getDefault, FileType.FILE, blocks, timestamp)

  it should "create a keyspace with name RANDOM" in {
    val ks = store.createKeyspace(store.buildSchema("RANDOM", 1))
    val status = Await.result(ks, 5 seconds)
    assert(status.isInstanceOf[Keyspace])
  }

  it should "throw KeyspaceAlreadyExistsException to create another keyspace RANDOM" in {
    val ks = store.createKeyspace(store.buildSchema("RANDOM", 1))
    val exception = intercept[KeyspaceAlreadyExistsException] {
      val status = Await.result(ks, 5 seconds)
    }
    assert(exception.getMessage === "RANDOM keyspace already exists")
  }

  it should "set keyspace to RANDOM" in {
    val setKeyspaceFuture = AsyncUtil.executeAsync[set_keyspace_call](client.set_keyspace("RANDOM", _))
    val result = Await.result(setKeyspaceFuture, 5 seconds)
    assert(result != "")
  }

  it should "create a INode" in {
    val response = store.storeINode(path, iNode)
    val responseValue: GenericOpSuccess = Await.result(response, 10 seconds)
    assert(responseValue === GenericOpSuccess())
  }

  it should "fetch created INode" in {
    val response = store.retrieveINode(path)
    assert(Await.result(response, 10 seconds).user === "user")
    assert(Await.result(response, 10 seconds).group === "group")
    assert(Await.result(response, 10 seconds).blocks === blocks)
  }

  it should "update INode on storing subBlock" in {
    val newSubBlockMeta = SubBlockMeta(UUID.randomUUID, 0, 128)
    val data = ByteBufferUtil.bytes("Test to store subBLock")
    val storeResponse = store.storeSubBlockAndUpdateINode(path, iNode, block1, newSubBlockMeta,data)
    val response = Await.result(storeResponse,10 seconds)
    assert(response === GenericOpSuccess())
  }

  override def afterAll = {
    Await.ready(AsyncUtil.executeAsync[system_drop_keyspace_call](client.system_drop_keyspace("RANDOM", _)), 50 seconds)
    clientManager.stop()
  }

}
