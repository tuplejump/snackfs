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

class FileSystemOutputStream(store: FileSystemStore, path: Path,
                             blockSize: Long, subBlockSize: Long,
                             bufferSize: Long) extends OutputStream {

  private var isClosed: Boolean = false

  private var blockId: UUID = UUIDGen.getTimeUUID

  private val backupBuffer = ByteBuffer.allocateDirect(subBlockSize.asInstanceOf[Int])

  private var bytesWrittenToBlock: Long = 0
  private var bytesWrittenToSubBlock: Long = 0
  private var position: Int = 0
  private var filePosition: Long = 0

  private val user = System.getProperty("user.name", "none")
  private val timestamp = System.currentTimeMillis()
  private val permission = FsPermission.getDefault

  private var blocksMeta = List[BlockMeta]()
  private var subBlocksMeta = List[SubBlockMeta]()

  private val outBuffer: Array[Byte] = Array()

  def write(p1: Int) = {
    if (isClosed) {
      throw new IOException("Stream closed")
    }
    if ((bytesWrittenToBlock + position == blockSize)
      || (bytesWrittenToSubBlock + position == subBlockSize)
      || (position >= bufferSize)) {
      flush
    }
    outBuffer(position) = p1.asInstanceOf[Byte]
    position += 1
    filePosition += 1
  }

  override def write(buf: Array[Byte], offset: Int, length: Int) = {
    if (isClosed) {
      throw new IOException("Stream closed")
    }
    while (length > 0) {
      val remaining: Int = (bufferSize - position).asInstanceOf[Int]
      var toWrite: Int = math.min(remaining, length)
      toWrite = math.min(toWrite, subBlockSize.asInstanceOf[Int])
      System.arraycopy(buf, offset, outBuffer, position, toWrite)
      position += toWrite
      filePosition += toWrite
      val reachedLocalBuffer = position == bufferSize
      if (overFlowsBlockSize || overFlowsSubBlockSize || reachedLocalBuffer) {
        flush
      }
    }
  }

  private def endSubBlock = {
    val offset = bytesWrittenToBlock - bytesWrittenToSubBlock - position
    val subBlockMeta = SubBlockMeta(UUIDGen.getTimeUUID, offset, bytesWrittenToSubBlock)

    backupBuffer.limit(backupBuffer.position())
    backupBuffer.rewind()

    val subBlockList = subBlocksMeta ++ List(subBlockMeta)
    val blockMeta = BlockMeta(blockId, filePosition - bytesWrittenToBlock - position, bytesWrittenToBlock, subBlockList)
    val iNode = INode(user, user, permission, FileType.FILE, blocksMeta, timestamp)
    Await.result(store.storeSubBlockAndUpdateINode(path, iNode, blockMeta, subBlockMeta, backupBuffer), 10 seconds)
    subBlocksMeta = subBlockList
  }

  private def endBlock = {
    val blockMeta = BlockMeta(blockId, filePosition - bytesWrittenToBlock - position, bytesWrittenToBlock, subBlocksMeta)
    blocksMeta = blocksMeta ++ List(blockMeta)
    bytesWrittenToBlock = 0
    subBlocksMeta = List[SubBlockMeta]()
    blockId = UUIDGen.getTimeUUID
  }

  private def flushData(maxPosition: Int) = {
    val workingPosition: Int = math.min(position, maxPosition)
    if (workingPosition > 0) {
      backupBuffer.put(outBuffer, 0, workingPosition)

      bytesWrittenToBlock += workingPosition
      bytesWrittenToSubBlock += workingPosition

      System.arraycopy(outBuffer, workingPosition, outBuffer, 0, position - workingPosition)
      position -= 1
    }
  }

  private def overFlowsBlockSize: Boolean = (bytesWrittenToBlock + position) >= blockSize

  private def overFlowsSubBlockSize: Boolean = (bytesWrittenToSubBlock + position) >= subBlockSize

  override def flush = {
    if (isClosed) {
      throw new IOException("Stream closed")
    }
    if (overFlowsBlockSize || overFlowsSubBlockSize) {
      val difference = subBlockSize - bytesWrittenToSubBlock
      flushData(difference.asInstanceOf[Int])
    }
    if (bytesWrittenToSubBlock == subBlockSize) {
      endSubBlock
    }
    if (bytesWrittenToBlock == blockSize) {
      if (bytesWrittenToSubBlock != 0) {
        endSubBlock
      }
      endBlock
    }
    flushData(position)
  }

  override def close = {
    if (!isClosed) {
      super.close
      isClosed = true
    }
  }
}
