package tj.fs

import java.io.{IOException, InputStream}
import tj.model.BlockMeta
import scala.concurrent.Await
import scala.concurrent.duration._

case class SubBlockInputStream(store: FileSystemStore, blockMeta: BlockMeta, start: Long) extends InputStream {
  private var isClosed: Boolean = false
  private var inputStream: InputStream = null
  private var currentPosition: Long = start
  private val length = blockMeta.length
  private var targetSubBLockSize = 0L

  private def findSubBlock(targetPosition: Long): InputStream = {
    val subBlockLengthTotals = blockMeta.subBlocks.scanLeft(0L)(_ + _.length).tail
    val subBlockIndex = subBlockLengthTotals.indexWhere(p => targetPosition < p)
    if (subBlockIndex == -1) {
      throw new IOException("Impossible situation: could not find targetPosition position " + targetPosition)
    }
    var offset = targetPosition
    if (subBlockIndex != 0) {
      offset -= subBlockLengthTotals(subBlockIndex - 1)
    }
    val subBlock = blockMeta.subBlocks(subBlockIndex)
    currentPosition = targetPosition
    targetSubBLockSize = subBlock.length
    Await.result(store.retrieveSubBlock(blockMeta, subBlock, offset), 10 seconds)
  }

  def read: Int = {
    if (isClosed) {
      throw new IOException("Stream closed")
    }
    var result = -1
    if (currentPosition <= length - 1) {
      if (currentPosition > targetSubBLockSize - 1) {
        if (inputStream != null) {
          inputStream.close()
        }
        inputStream = findSubBlock(currentPosition)
      }
      result = inputStream.read()
      if (result >= 0) {
        currentPosition += 1
      }
    }
    result
  }

  override def read(buf: Array[Byte], off: Int, len: Int): Int = {
    if (isClosed) {
      throw new IOException("Stream closed")
    }
    if (buf == null) {
      throw new NullPointerException
    }
    if ((off < 0) || (len < 0) || (len > buf.length - off)) {
      throw new IndexOutOfBoundsException
    }
    var result = 0
    if (len > 0) {
      while (result < len && currentPosition <= length - 1) {
        inputStream = findSubBlock(currentPosition)
        val remaining = len - (off + result)
        val size = math.min(remaining, targetSubBLockSize)
        inputStream.read(buf, off + result, size.asInstanceOf[Int])
        result += size.asInstanceOf[Int]
        currentPosition += size
      }
      if (result == 0) {
        result = -1
      }
    }
    result
  }

  override def close() = {
    if (!isClosed) {
      if (inputStream != null) {
        inputStream.close()
      }
      super.close()
      isClosed = true
    }
  }
}
