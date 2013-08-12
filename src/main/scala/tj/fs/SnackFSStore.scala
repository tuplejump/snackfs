package tj.fs

import org.apache.cassandra.thrift.KsDef
import scala.concurrent.Future
import tj.model._
import org.apache.hadoop.fs.Path
import java.util.UUID
import java.nio.ByteBuffer
import tj.model.SubBlockMeta
import tj.model.GenericOpSuccess
import java.io.InputStream

trait SnackFSStore {
  def buildSchema(keyspace: String, replicationFactor: Int): KsDef

  def createKeyspace(ksDef: KsDef): Future[Keyspace]

  def saveINode(path: Path, iNode: INode): GenericOpSuccess

  def retrieveINode(path: Path): INode

  def saveSubBlock(blockId: UUID, subBlockMeta: SubBlockMeta, data: ByteBuffer): GenericOpSuccess

  def retrieveSubBlock(blockMeta: BlockMeta, subBlockMeta: SubBlockMeta, byteRangeStart: Long):InputStream
}
