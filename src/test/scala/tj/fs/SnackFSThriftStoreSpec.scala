package tj.fs

import org.apache.thrift.async.TAsyncClientManager
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TNonblockingSocket
import scala.concurrent.duration._
import scala.concurrent.Await

import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import tj.exceptions.KeyspaceAlreadyExistsException
import tj.model.Keyspace
import org.apache.cassandra.thrift.Cassandra.AsyncClient
import tj.util.AsyncUtil
import org.apache.cassandra.thrift.Cassandra.AsyncClient.system_drop_keyspace_call

class SnackFSThriftStoreSpec extends FlatSpec with BeforeAndAfterAll {

  val clientManager = new TAsyncClientManager()
  val protocolFactory = new TBinaryProtocol.Factory()
  val transport = new TNonblockingSocket("127.0.0.1", 9160)

  def client = new AsyncClient(protocolFactory, clientManager, transport)

  val store = new SnackFSThriftStore(client)

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

  override def afterAll = {
    Await.ready(AsyncUtil.executeAsync[system_drop_keyspace_call](client.system_drop_keyspace("RANDOM", _)), 5 seconds)
    clientManager.stop()
  }

}
