package tj.fs

import java.io.{IOException, InputStream}
import tj.model.BlockMeta
import scala.concurrent.Await
import scala.concurrent.duration._

case class SubBlockInputStream(store: FileSystemStore, blockMeta: BlockMeta, start: Long) extends InputStream {
  private var isClosed: Boolean = false
  private var inputStream: InputStream = null
  private var position: Long = start
  private var subBlockEndPosition: Long = -1

  private def findSubBlock(target: Long): InputStream = {
    val subBlockLengthTotals = blockMeta.subBlocks.scanLeft(0L)(_ + _.length).tail
    val subBlockIndex = subBlockLengthTotals.indexWhere(p => target < p)
    if (subBlockIndex == -1) {
      throw new IOException("Impossible situation: could not find target position " + target)
    }
    var offset = target
    if (subBlockIndex != 0) {
      offset -= subBlockLengthTotals(subBlockIndex - 1)
    }
    val subBlock = blockMeta.subBlocks(subBlockIndex)
    position = target
    subBlockEndPosition = subBlock.length - 1
    Await.result(store.retrieveSubBlock(blockMeta, subBlock, offset), 10 seconds)
  }

  def read: Int = {
    if (isClosed) {
      throw new IOException("Stream closed")
    }
    var nextByte = -1
    if (position < blockMeta.length) {
      if (position > subBlockEndPosition) {
        if (inputStream != null) {
          inputStream.close
        }
        inputStream = findSubBlock(position)
      }
      nextByte = inputStream.read()
      if (nextByte >= 0) {
        position += 1
      }
    }
    nextByte
  }

  override def read(buf: Array[Byte], off: Int, len: Int): Int = {
    if (isClosed) {
      throw new IOException("Stream closed")
    }
    var result = -1
    if (position < blockMeta.length) {
      if (position > subBlockEndPosition) {
        if (inputStream != null) {
          inputStream.close
        }
        inputStream = findSubBlock(position)
      }
      val realLen: Int = math.min(len, (subBlockEndPosition - position + 1).asInstanceOf[Int])
      result = inputStream.read(buf, off, realLen)
      if (result >= 0) {
        position += result
      }
    }
    result
  }

  override def close = {
    if (!isClosed) {
      if (inputStream != null) {
        inputStream.close
      }
      super.close
      isClosed = true
    }
  }
}
