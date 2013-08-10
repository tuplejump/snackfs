package tj.fs

import org.apache.cassandra.thrift.KsDef
import scala.concurrent.Future
import tj.model.Keyspace

trait SnackFSStore {
  def buildSchema(keyspace: String, replicationFactor: Int): KsDef
  def createKeyspace(ksDef: KsDef): Future[Keyspace]
}
