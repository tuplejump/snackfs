package tj.fs

import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.apache.thrift.async.TAsyncClientManager
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TNonblockingSocket
import org.apache.cassandra.thrift.Cassandra.AsyncClient
import java.net.URI
import org.apache.hadoop.fs.Path
import org.apache.cassandra.utils.ByteBufferUtil
import scala.concurrent.Await
import scala.concurrent.duration._
import tj.util.AsyncUtil
import org.apache.cassandra.thrift.Cassandra.AsyncClient.{system_drop_keyspace_call, set_keyspace_call}
import java.nio.file.{FileSystems, Files}

class FileSystemOutputStreamSpec extends FlatSpec with BeforeAndAfterAll {
  val clientManager = new TAsyncClientManager()
  val protocolFactory = new TBinaryProtocol.Factory()
  val transport = new TNonblockingSocket("127.0.0.1", 9160)

  def client = new AsyncClient(protocolFactory, clientManager, transport)

  val store = new ThriftStore(client)

  Await.result(store.createKeyspace(store.buildSchema("RANDOM", 1)), 5 seconds)
  Await.result(AsyncUtil.executeAsync[set_keyspace_call](client.set_keyspace("RANDOM", _)), 5 seconds)

  it should "fetch data which is equal to actual data" in {
    val pathURI = URI.create("outputStream.txt")
    val path = new Path(pathURI)
    val data = ByteBufferUtil.bytes("Test Subblock insertion")

    val outputStream = FileSystemOutputStream(store, path, 30, 10, 10)

    outputStream.write(data.array(), 0, data.array().length)
    outputStream.close

    val inode = Await.result(store.retrieveINode(path), 10 seconds)
    assert(inode.blocks.length === 1)

    val blockData = store.retrieveBlock(inode.blocks(0), 0)
    var outBuf: Array[Byte] = new Array[Byte](23)
    blockData.read(outBuf, 0, 23)
    assert(outBuf != null)
    assert(outBuf === data.array())
  }

  it should "fetch data loaded from a file" in {
    val nioPath = FileSystems.getDefault().getPath("/home/shiti/books/mapreduce-osdi04.pdf")
    val data = Files.readAllBytes(nioPath)

    println("file size=" + data.length)
    val pathURI = URI.create("fileRead.pdf")
    val path = new Path(pathURI)
    val outputStream = FileSystemOutputStream(store, path, 30000, 3000, data.length)
    outputStream.write(data, 0, data.length)
    outputStream.close

    val inode = Await.result(store.retrieveINode(path), 10 seconds)
    println("blocks=" + inode.blocks.length)
    val minSize: Int = data.length / 30000
    println(minSize)
    assert(inode.blocks.length >= minSize)
    val fetchedData: Array[Byte] = new Array[Byte](data.length)
    var offset = 0
    inode.blocks.foreach(block => {
      val blockData = store.retrieveBlock(block, 0)
      blockData.read(fetchedData, offset, block.length.asInstanceOf[Int])
      println(block.toString)
      offset += (block.length - 1).asInstanceOf[Int]
    })
    println("completed copy")
//    assert(fetchedData(1) === data(1))
  }

  override def afterAll = {
    Await.ready(AsyncUtil.executeAsync[system_drop_keyspace_call](client.system_drop_keyspace("RANDOM", _)), 10 seconds)
    clientManager.stop()
  }

}
