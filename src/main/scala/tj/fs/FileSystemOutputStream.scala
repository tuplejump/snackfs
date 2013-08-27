package tj.fs

import java.io.{IOException, OutputStream}
import org.apache.hadoop.fs.Path
import org.apache.cassandra.utils.UUIDGen
import java.util.UUID
import java.nio.ByteBuffer
import tj.model.{SubBlockMeta, BlockMeta, FileType, INode}
import org.apache.hadoop.fs.permission.FsPermission
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.collection.mutable

case class FileSystemOutputStream(store: FileSystemStore, path: Path,
                                  blockSize: Long, subBlockSize: Long,
                                  bufferSize: Long) extends OutputStream {

  private val AT_MOST: FiniteDuration = 10 seconds
  private var isClosed: Boolean = false

  private var blockId: UUID = UUIDGen.getTimeUUID

  private var subBlockOffset = 0
  private var blockOffset = 0
  private var position = 0
  private var outBuffer = Array.empty[Byte]

  private var subBlocksMeta = List[SubBlockMeta]()
  private var blocksMeta = List[BlockMeta]()

  private var isClosing = false

  private var bytesWrittenToBlock = 0

  def write(p1: Int) = {
    if (isClosed) {
      throw new IOException("Stream closed")
    }

  }

  override def write(buf: Array[Byte], offset: Int, length: Int) = {
    if (isClosed) {
      throw new IOException("Stream closed")
    }
    var lengthTemp = length
    var offsetTemp = offset
    while (lengthTemp > 0) {
      //      println("offset:%d,length:%d".format(offset,length))
      val lengthToWrite = math.min(subBlockSize - position, lengthTemp).asInstanceOf[Int]
      val slice: Array[Byte] = buf.slice(offsetTemp, offsetTemp + lengthToWrite)
      outBuffer = outBuffer ++ slice
      lengthTemp -= lengthToWrite
      offsetTemp += lengthToWrite
      position += lengthToWrite
      if (position == subBlockSize) {
        println("flushing into subblock")
        flush()
      }
    }
  }

  private def endSubBlock() = {
    if (position != 0) {
      val subBlockMeta = SubBlockMeta(UUIDGen.getTimeUUID, subBlockOffset, position)
      Await.ready(store.storeSubBlock(blockId, subBlockMeta, ByteBuffer.wrap(outBuffer)), AT_MOST)
      subBlockOffset += position
      bytesWrittenToBlock += position
      subBlocksMeta = subBlocksMeta :+ subBlockMeta
      position = 0
      outBuffer = Array.empty[Byte]
    }
  }

  private def endBlock() = {
    val subBlockLengths = subBlocksMeta.map(_.length).sum
    val block = BlockMeta(blockId, blockOffset, subBlockLengths, subBlocksMeta)
    println(block)
    blocksMeta = blocksMeta :+ block
    val user = System.getProperty("user.name")
    val permissions = FsPermission.getDefault
    val timestamp = System.currentTimeMillis()
    val iNode = INode(user, user, permissions, FileType.FILE, blocksMeta, timestamp)
    Await.ready(store.storeINode(path, iNode), AT_MOST)
    blockOffset += subBlockLengths.asInstanceOf[Int]
    subBlocksMeta = List()
    subBlockOffset = 0
    blockId = UUIDGen.getTimeUUID
    bytesWrittenToBlock = 0
  }

  override def flush() = {
    if (isClosed) {
      throw new IOException("Stream closed")
    }
    endSubBlock()
    if (bytesWrittenToBlock >= blockSize || isClosing) {
      endBlock()
    }
  }

  override def close() = {
    if (!isClosed) {
      isClosing = true
      flush()
      super.close()
      isClosed = true
    }
  }
}
