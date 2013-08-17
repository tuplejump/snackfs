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
import tj.util.AsyncUtil

case class FileSystemOutputStream(store: FileSystemStore, path: Path,
                                  blockSize: Long, subBlockSize: Long,
                                  bufferSize: Long) extends OutputStream {

  private var isClosed: Boolean = false

  private var blockId: UUID = UUIDGen.getTimeUUID

  private var backupBuffer: ByteBuffer = ByteBuffer.allocate(subBlockSize.asInstanceOf[Int])

  private var bytesWrittenToBlock: Long = 0
  private var bytesWrittenToSubBlock: Long = 0
  private var position: Int = 0
  private var filePosition: Long = 0

  private val user = System.getProperty("user.name", "none")
  private val timestamp = System.currentTimeMillis()
  private val permission = FsPermission.getDefault

  private var blocksMeta = List[BlockMeta]()
  private var subBlocksMeta = List[SubBlockMeta]()

  private val outBuffer: Array[Byte] = new Array[Byte](bufferSize.asInstanceOf[Int])

  def write(p1: Int) = {
    if (isClosed) {
      throw new IOException("Stream closed")
    }
    if ((bytesWrittenToBlock + position == blockSize)
      || (bytesWrittenToSubBlock + position == subBlockSize)
      || (position >= bufferSize)) {
      flush
    }
    outBuffer.update(position, p1.asInstanceOf[Byte])
    position += 1
    filePosition += 1
  }

  override def write(buf: Array[Byte], offset: Int, length: Int) = {
    if (isClosed) {
      throw new IOException("Stream closed")
    }
    var offsetTemp = offset
    var lengthTemp = length

    while (lengthTemp > 0) {
      val remaining: Int = (bufferSize - position).asInstanceOf[Int]
      var toWrite: Int = math.min(remaining, lengthTemp)
      toWrite = math.min(toWrite, subBlockSize.asInstanceOf[Int])
      System.arraycopy(buf, offsetTemp, outBuffer, position, toWrite)
      offsetTemp += toWrite
      lengthTemp -= toWrite
      position += toWrite
      filePosition += toWrite
      val reachedLocalBuffer = position == bufferSize
      if (overFlowsBlockSize || overFlowsSubBlockSize || reachedLocalBuffer) {
        flush
      }
    }
  }

  private def endSubBlock = {
    println("%d:%d:%d".format(bytesWrittenToBlock, bytesWrittenToSubBlock, position))
    val offset = bytesWrittenToBlock - bytesWrittenToSubBlock - position
    val subBlockMeta = SubBlockMeta(UUIDGen.getTimeUUID, offset, bytesWrittenToSubBlock)

    backupBuffer.limit(backupBuffer.position())
    backupBuffer.rewind()

    val subBlockList = subBlocksMeta ++ List(subBlockMeta)
    println("Writing subblock... BLOCK SIZE %d".format(bytesWrittenToBlock))
    Await.ready(store.storeSubBlock(blockId, subBlockMeta, backupBuffer), 10 seconds)
    //bytesWrittenToSubBlock = 0
    subBlocksMeta = subBlockList
  }

  private def endBlock = {
    println("current block size %d".format(bytesWrittenToBlock))
    val blockMeta = BlockMeta(blockId, filePosition - bytesWrittenToBlock - position, bytesWrittenToBlock, subBlocksMeta)
    blocksMeta = blocksMeta ++ List(blockMeta)
    val iNode = INode(user, user, permission, FileType.FILE, blocksMeta, timestamp)
    Await.ready(store.storeINode(path, iNode), 10 seconds)
    bytesWrittenToBlock = 0
    subBlocksMeta = List[SubBlockMeta]()
    blockId = UUIDGen.getTimeUUID
  }

  private def flushData(maxPosition: Int) = {
    val workingPosition: Int = math.min(position, maxPosition)
    if (workingPosition > 0) {
      backupBuffer = ByteBuffer.wrap(outBuffer, 0, workingPosition)

      bytesWrittenToBlock += workingPosition
      bytesWrittenToSubBlock += workingPosition

      System.arraycopy(outBuffer, workingPosition, outBuffer, 0, position - workingPosition)
      position = 0
    }
  }

  private def overFlowsBlockSize: Boolean = (bytesWrittenToBlock + position) >= blockSize

  private def overFlowsSubBlockSize: Boolean = (bytesWrittenToSubBlock + position) >= subBlockSize

  override def flush = {
    println("Flushing!!! " + isClosing)
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

    if (bytesWrittenToBlock == blockSize || isClosing) {
      println("Closing subblock...")
      if (bytesWrittenToSubBlock != 0 || position > 0) {
        endSubBlock
      }
      endBlock
    }
    flushData(position)
  }

  var isClosing: Boolean = false

  override def close = {
    if (!isClosed) {
      println("CLOSING --- ")
      isClosing = true
      flush
      super.close
      isClosed = true
    }
  }
}
