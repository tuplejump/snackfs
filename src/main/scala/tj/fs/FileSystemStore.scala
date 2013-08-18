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

trait FileSystemStore {
  def buildSchema(keyspace: String, replicationFactor: Int): KsDef

  def createKeyspace(ksDef: KsDef): Future[Keyspace]

  def storeINode(path: Path, iNode: INode): Future[GenericOpSuccess]

  def retrieveINode(path: Path): Future[INode]

  def storeSubBlock(blockId: UUID, subBlockMeta: SubBlockMeta, data: ByteBuffer): Future[GenericOpSuccess]

  def retrieveSubBlock(blockMeta: BlockMeta, subBlockMeta: SubBlockMeta, byteRangeStart: Long):Future[InputStream]

  def storeSubBlockAndUpdateINode(path: Path, iNode: INode,block:BlockMeta,subBlockMeta: SubBlockMeta, data: ByteBuffer): Future[GenericOpSuccess]

  def retrieveBlock (blockMeta:BlockMeta):InputStream

}
