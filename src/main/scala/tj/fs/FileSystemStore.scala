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
  def buildSchema(keyspace: String, replicationFactor: Int, replicationStrategy: String): KsDef

  def createKeyspace(ksDef: KsDef): Future[Keyspace]

  def storeINode(path: Path, iNode: INode): Future[GenericOpSuccess]

  def retrieveINode(path: Path): Future[INode]

  def storeSubBlock(blockId: UUID, subBlockMeta: SubBlockMeta, data: ByteBuffer): Future[GenericOpSuccess]

  def retrieveSubBlock(blockId: UUID, subBlockId: UUID, byteRangeStart: Long): Future[InputStream]

  def retrieveBlock(blockMeta: BlockMeta): InputStream

  def deleteINode(path: Path): Future[GenericOpSuccess]

  def deleteBlocks(iNode: INode): Future[GenericOpSuccess]

  def fetchSubPaths(path: Path, isDeepFetch: Boolean): Future[Set[Path]]
}
