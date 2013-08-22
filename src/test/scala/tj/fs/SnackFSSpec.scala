package tj.fs

import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.scalatest.matchers.MustMatchers
import org.apache.thrift.async.TAsyncClientManager
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TNonblockingSocket
import org.apache.cassandra.thrift.Cassandra.AsyncClient
import scala.concurrent.Await
import scala.concurrent.duration._
import tj.util.AsyncUtil
import org.apache.cassandra.thrift.Cassandra.AsyncClient.{set_keyspace_call, system_drop_keyspace_call}
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import java.io.IOException

class SnackFSSpec extends FlatSpec with BeforeAndAfterAll with MustMatchers {

  val clientManager = new TAsyncClientManager()
  val protocolFactory = new TBinaryProtocol.Factory()
  val transport = new TNonblockingSocket("127.0.0.1", 9160)

  def client = new AsyncClient(protocolFactory, clientManager, transport)

  val store = new ThriftStore(client)

  Await.result(store.createKeyspace(store.buildSchema("FS", 1)), 5 seconds)
  Await.result(AsyncUtil.executeAsync[set_keyspace_call](client.set_keyspace("FS", _)), 5 seconds)

  it should "create a new filesystem with given store" in {
    val fs = SnackFS(store)
    val uri = URI.create("cfs://localhost:9000")
    fs.initialize(uri, new Configuration())
    fs.getUri must be(uri)
    val user = System.getProperty("user.name", "none")
    fs.getWorkingDirectory must be(new Path("cfs://localhost:9000/user/" + user))
  }

  it should "add a directory" in {
    val fs = SnackFS(store)
    val uri = URI.create("cfs://localhost:9000")
    fs.initialize(uri, new Configuration())
    val result = fs.mkdirs(new Path("/mytestdir"))
    assert(result === true)
  }

  it should "create an entry for a file" in {
    val fs = SnackFS(store)
    val uri = URI.create("cfs://localhost:9000")
    fs.initialize(uri, new Configuration())
    val fsData = fs.create(new Path("/home/shiti/Downloads/JSONParser.js"))
    fsData.write("SOME CONTENT".getBytes)
    val position = fsData.getPos
    position must be(12)
  }

  it should "throw an exception when trying to add a file as a directory" in {
    val fs = SnackFS(store)
    val uri = URI.create("cfs://localhost:9000")
    fs.initialize(uri, new Configuration())
    val fsData = fs.create(new Path("/home/shiti/Downloads/SOMEFILE"))
    fsData.write("SOME CONTENT".getBytes)
    fsData.close()
    val path = new Path("/home/shiti/Downloads/SOMEFILE")
    val exception = intercept[IOException] {
      val result = fs.mkdirs(path)
    }
    exception.getMessage must be("Can't make a directory for path %s since its a file".format(path))
  }

  it should "allow to read from a file" in {
    val fs = SnackFS(store)
    val uri = URI.create("cfs://localhost:9000")
    fs.initialize(uri, new Configuration())
    val fsData = fs.create(new Path("/home/shiti/Downloads/SOMEFILE"))
    fsData.write("SOME CONTENT".getBytes)
    fsData.close()

    val is = fs.open(new Path("/home/shiti/Downloads/SOMEFILE"))
    var dataArray = new Array[Byte](12)
    is.readFully(0, dataArray)
    is.close()

    val result = new String(dataArray)
    result must be("SOME CONTENT")
  }

  it should "throw an exception when trying to open a directory" in {
    val fs = SnackFS(store)
    val uri = URI.create("cfs://localhost:9000")
    fs.initialize(uri, new Configuration())
    val path = new Path("/test")
    fs.mkdirs(path)
    val exception = intercept[IOException] {
      fs.open(path)
    }
    exception.getMessage must be("Path %s is a directory.".format(path))
  }

  it should "throw an exception when trying to open a file which doesn't exist" in {
    val fs = SnackFS(store)
    val uri = URI.create("cfs://localhost:9000")
    fs.initialize(uri, new Configuration())
    val path = new Path("/newFile")
    val exception = intercept[IOException] {
      fs.open(path)
    }
    exception.getMessage must be("No such file.")
  }

  it should "get file status" in {
    val fs = SnackFS(store)
    val uri = URI.create("cfs://localhost:9000")
    fs.initialize(uri, new Configuration())
    val path = new Path("/home/shiti/Downloads/SOMEFILE")
    val fsData = fs.create(path)
    fsData.write("SOME CONTENT".getBytes)
    fsData.close()

    val status = fs.getFileStatus(path)
    status.isFile must be(true)
    status.getLen must be(12)
    status.getPath must be(path)
  }

  it should "get file block locations" in {
    val fs = SnackFS(store)
    val uri = URI.create("cfs://localhost:9000")
    fs.initialize(uri, new Configuration())
    val path = new Path("/home/shiti/Downloads/SOMEFILE")
    val fsData = fs.create(path)
    fsData.write("This is a test to check the block location details".getBytes)
    fsData.write("This is a test to check the block location details".getBytes)
    fsData.write("This is a test to check the block location details".getBytes)
    fsData.write("This is a test to check the block location details".getBytes)
    fsData.write("This is a test to check the block location details".getBytes)

    fsData.close()

    val status = fs.getFileStatus(path)
    val locations = fs.getFileBlockLocations(status, 0, 10)
    assert(locations(0).getLength === 250)
  }

  override def afterAll() = {
    Await.ready(AsyncUtil.executeAsync[system_drop_keyspace_call](client.system_drop_keyspace("FS", _)), 10 seconds)
    clientManager.stop()
  }

}
