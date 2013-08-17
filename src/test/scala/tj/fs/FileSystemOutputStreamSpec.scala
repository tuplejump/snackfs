package tj.fs

import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.apache.thrift.async.TAsyncClientManager
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TNonblockingSocket
import org.apache.cassandra.thrift.Cassandra.AsyncClient
import tj.model._
import java.util.UUID
import java.net.URI
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.cassandra.utils.ByteBufferUtil
import scala.concurrent.Await
import scala.concurrent.duration._
import tj.model.BlockMeta
import tj.model.SubBlockMeta
import tj.util.AsyncUtil
import org.apache.cassandra.thrift.Cassandra.AsyncClient.{system_drop_keyspace_call, set_keyspace_call}

class FileSystemOutputStreamSpec extends FlatSpec with BeforeAndAfterAll {
  val clientManager = new TAsyncClientManager()
  val protocolFactory = new TBinaryProtocol.Factory()
  val transport = new TNonblockingSocket("127.0.0.1", 9160)

  def client = new AsyncClient(protocolFactory, clientManager, transport)

  val store = new ThriftStore(client)

  val timestamp = System.currentTimeMillis()
  val subBlocks = List(SubBlockMeta(UUID.randomUUID, 0, 128), SubBlockMeta(UUID.randomUUID, 128, 128))
  val block1 = BlockMeta(UUID.randomUUID, 0, 256, subBlocks)
  val block2 = BlockMeta(UUID.randomUUID, 0, 256, subBlocks)
  val blocks = List(block1, block2)
  val pathURI = URI.create("outputStream.txt")
  val path = new Path(pathURI)
  val iNode = INode("user", "group", FsPermission.getDefault, FileType.FILE, blocks, timestamp)

  val subBlockMeta1 = SubBlockMeta(UUID.randomUUID, 0, 128)
  val data = ByteBufferUtil.bytes("Test Subblock insertion")

  val outputStream = FileSystemOutputStream(store, path, 30, 10, 10)

  Await.result(store.createKeyspace(store.buildSchema("RANDOM", 1)), 5 seconds)
  Await.result(AsyncUtil.executeAsync[set_keyspace_call](client.set_keyspace("RANDOM", _)), 5 seconds)

  def convertStreamToString(inputStream: java.io.InputStream): String = {
    val scanner = new java.util.Scanner(inputStream).useDelimiter("\\A")
    if (scanner.hasNext()) {
      scanner.next()
    }
    else ""
  }

  it should "fetch data which is equal to actual data" in {

    println(data.array().length)
    outputStream.write(data.array(), 0, data.array().length)
    outputStream.close

    val inode = Await.result(store.retrieveINode(path), 10 seconds)
    assert(inode.blocks.length === 1)
    inode.blocks.foreach(b=>println(b.toString))

    val blockData = store.retrieveBlock(iNode.blocks(0), 0)
    println(convertStreamToString(blockData))
    assert(blockData != null)
    assert(convertStreamToString(blockData) === data.array())
  }

  override def afterAll = {
    Await.ready(AsyncUtil.executeAsync[system_drop_keyspace_call](client.system_drop_keyspace("RANDOM", _)), 10 seconds)
    clientManager.stop()
  }

}
