package tj.fs

import org.apache.hadoop.fs.{Path, FSInputStream}
import java.io.{IOException, InputStream}
import scala.concurrent.Await
import scala.concurrent.duration._

case class FileSystemInputStream(store: FileSystemStore, path: Path) extends FSInputStream {

  private val iNode = Await.result(store.retrieveINode(path), 10 seconds)
  private val fileLength: Long = iNode.blocks.map(_.length).sum

  private var position: Long = 0L

  private var inputStream: InputStream = null

  private var blockEndPosition: Long = -1

  private var isClosed: Boolean = false

  def seek(target: Long) = {
    if (target > fileLength) {
      throw new IOException("Cannot seek after EOF")
    }
    position = target
    blockEndPosition = -1
  }

  def getPos: Long = position

  def seekToNewSource(targetPos: Long): Boolean = false

  private def findBlock(target: Long): InputStream = {
    val blockLengthTotals = iNode.blocks.scanLeft(0L)(_ + _.length).tail
    val blockIndex = blockLengthTotals.indexWhere(p => target < p)
    if (blockIndex == -1) {
      throw new IOException("Impossible situation: could not find target position " + target)
    }
    var offset = target
    if (blockIndex != 0) {
      offset -= blockLengthTotals(blockIndex - 1)
    }
    val block = iNode.blocks(blockIndex)
    position = target
    blockEndPosition = block.length - 1
    val bis = store.retrieveBlock(block)
    bis.skip(offset)
    bis

  }

  def read(): Int = {
    if (isClosed) {
      throw new IOException("Stream closed")
    }
    var result: Int = -1

    if (position < fileLength) {
      if (position > blockEndPosition) {
        if (inputStream != null) {
          inputStream.close
        }
        inputStream = findBlock(position)
      }
      result = inputStream.read
      if (result >= 0) {
        position += 1
      }
    }
    result
  }

  override def available: Int = (fileLength - position).asInstanceOf[Int]

  override def read(buf: Array[Byte], off: Int, len: Int): Int = {
    if (isClosed) {
      throw new IOException("Stream closed")
    }

    var result: Int = -1
    if (position < fileLength) {
      if (position > blockEndPosition) {
        if (inputStream != null) {
          inputStream.close
        }
        inputStream = findBlock(position)
      }
      val realLen: Int = math.min(len, (blockEndPosition - position + 1).asInstanceOf[Int])
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
