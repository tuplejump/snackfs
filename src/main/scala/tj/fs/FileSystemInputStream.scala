package tj.fs

import org.apache.hadoop.fs.{Path, FSInputStream}
import java.io.{IOException, InputStream}
import scala.concurrent.Await
import scala.concurrent.duration._

case class FileSystemInputStream(store: FileSystemStore, path: Path) extends FSInputStream {

  private val iNode = Await.result(store.retrieveINode(path), 10 seconds)
  private val fileLength: Long = iNode.blocks.map(_.length).sum

  private var currentPosition: Long = 0L

  private var blockStream: InputStream = null

  private var blockEndPosition: Long = -1

  private var isClosed: Boolean = false

  def seek(target: Long) = {
    if (target > fileLength) {
      throw new IOException("Cannot seek after EOF")
    }
    currentPosition = target
    blockEndPosition = -1
  }

  def getPos: Long = currentPosition

  def seekToNewSource(targetPos: Long): Boolean = false

  private def findBlock(targetPosition: Long): InputStream = {
    val blockLengthTotals = iNode.blocks.scanLeft(0L)(_ + _.length).tail
    val blockIndex = blockLengthTotals.indexWhere(p => targetPosition < p)
    if (blockIndex == -1) {
      throw new IOException("Impossible situation: could not find position " + targetPosition)
    }
    var offset = targetPosition
    if (blockIndex != 0) {
      offset -= blockLengthTotals(blockIndex - 1)
    }
    val block = iNode.blocks(blockIndex)
    currentPosition = targetPosition
    blockEndPosition = block.length - 1
    store.retrieveBlock(block)
  }

  def read(): Int = {
    if (isClosed) {
      throw new IOException("Stream closed")
    }
    var result: Int = -1

    if (currentPosition < fileLength) {
      if (currentPosition > blockEndPosition) {
        if (blockStream != null) {
          blockStream.close()
        }
        blockStream = findBlock(currentPosition)
      }
      result = blockStream.read
      if (result >= 0) {
        currentPosition += 1
      }
    }
    result
  }

  override def available: Int = (fileLength - currentPosition).asInstanceOf[Int]

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

    var result: Int = 0
    if (len > 0) {
      while (result < len && currentPosition <= fileLength - 1) {
        if (currentPosition > blockEndPosition - 1) {
          if (blockStream != null) {
            blockStream.close()
          }
          blockStream = findBlock(currentPosition)
        }
        val realLen: Int = math.min(len - result, blockEndPosition + 1).asInstanceOf[Int]
        var readSize = blockStream.read(buf, off + result, realLen)
        result += readSize
        currentPosition += readSize
      }
      if (result == 0) {
        result = -1
      }
    }
    result
  }

  override def close() = {
    if (!isClosed) {
      if (blockStream != null) {
        blockStream.close()
      }
      super.close()
      isClosed = true
    }
  }
}
