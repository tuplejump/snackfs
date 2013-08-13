package tj.fs

import java.io.{IOException, InputStream}
import tj.model.BlockMeta
import scala.concurrent.Await
import scala.concurrent.duration._

class SnackFSSubBlockInputStream(store: SnackFSStore, blockMeta: BlockMeta, start: Long) extends InputStream {
  private var isClosed: Boolean = false
  private var stream: InputStream = null
  private var position: Long = start
  private var subBlockEndPosition: Long = -1

  private def findSubBlock(target: Long) = {
    if (stream != null) {
      stream.close
    }
    val subBlockLengthTotals = blockMeta.subBlocks.scanLeft(0L)(_ + _.length).tail
    val subBlockIndex = subBlockLengthTotals.indexWhere(p => target < p)
    if (subBlockIndex == -1) {
      throw new IOException("Impossible situation: could not find target position " + target)
    }
    val offset = target - subBlockLengthTotals(subBlockIndex - 1)
    val subBlock = blockMeta.subBlocks(subBlockIndex)
    position = target
    stream = Await.result(store.retrieveSubBlock(blockMeta, subBlock, offset), 10 seconds)
    subBlockEndPosition = subBlock.length - 1
  }

  def read: Int = {
    if (isClosed) {
      throw new IOException("Stream closed")
    }
    var nextByte = -1
    if (position < blockMeta.length) {
      if (position > subBlockEndPosition) {
        findSubBlock(position)
      }
      nextByte = stream.read()
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
        findSubBlock(position)
      }
      val realLen: Int = List(len, (subBlockEndPosition - position + 1).asInstanceOf[Int]).min
      result = stream.read(buf, off, realLen)
      if (result >= 0) {
        position += result
      }
    }
    result
  }

  override def close = {
    if(!isClosed){
      stream.close
      super.close
      isClosed = true
    }
  }
}
