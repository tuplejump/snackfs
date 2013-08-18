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
import org.apache.commons.io.IOUtils
import org.scalatest.matchers.MustMatchers

class FileSystemOutputStreamSpec extends FlatSpec with BeforeAndAfterAll with MustMatchers {
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

  it should "fetch data loaded from smaller(<2KB) file" in {
    val nioPath = FileSystems.getDefault().getPath("src/test/resources/vsmall.txt")
    val data = Files.readAllBytes(nioPath)

    println("file size=" + data.length)
    val pathURI = URI.create("vsmall.txt")
    val path = new Path(pathURI)
    val maxBlockSize = 500
    val maxSubBlockSize = 50
    val outputStream = FileSystemOutputStream(store, path, maxBlockSize, maxSubBlockSize, data.length)
    outputStream.write(data, 0, data.length)
    outputStream.close

    val inode = Await.result(store.retrieveINode(path), 10 seconds)
    println("blocks=" + inode.blocks.length)
    val minSize: Int = data.length / maxBlockSize
    println(minSize)
    assert(inode.blocks.length >= minSize)
    var fetchedData: Array[Byte] = new Array[Byte](data.length)
    var offset = 0
    inode.blocks.foreach(block => {
      val blockData = store.retrieveBlock(block, 0)
      val source = IOUtils.toByteArray(blockData)
      System.arraycopy(source, 0, fetchedData, offset, source.length)
      blockData.close()
      offset += (block.length).asInstanceOf[Int]
    })
    println("completed copy")
    assert(fetchedData === data)
  }

  it should "fetch data loaded from medium(~600KB) file" in {
    val nioPath = FileSystems.getDefault().getPath("src/test/resources/small.txt")
    val data = Files.readAllBytes(nioPath)

    val dataString = new java.lang.String(data)

    println("file size=" + data.length)
    val pathURI = URI.create("small.txt")
    val path = new Path(pathURI)
    val maxBlockSize = 30000
    val maxSubBlockSize = 3000
    val outputStream = FileSystemOutputStream(store, path, maxBlockSize, maxSubBlockSize, data.length)
    outputStream.write(data, 0, data.length)
    outputStream.close

    val inode = Await.result(store.retrieveINode(path), 10 seconds)
    println("blocks=" + inode.blocks.length)
    val minSize: Int = data.length / maxBlockSize
    println(minSize)
    assert(inode.blocks.length >= minSize)

    var fetchedData: Array[Byte] = Array()
    var offset = 0
    inode.blocks.foreach(block => {
      val blockData = store.retrieveBlock(block, 0)
      val source = IOUtils.toByteArray(blockData)
      blockData.close()
      fetchedData = fetchedData ++ source
      offset += source.length
    })
    println("completed copy")
    val fetchedDataString = new String(fetchedData)
    fetchedDataString must be(dataString)
  }

  override def afterAll = {
    Await.ready(AsyncUtil.executeAsync[system_drop_keyspace_call](client.system_drop_keyspace("RANDOM", _)), 10 seconds)
    clientManager.stop()
  }

}
