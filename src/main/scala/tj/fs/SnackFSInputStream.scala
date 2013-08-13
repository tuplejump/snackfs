package tj.fs

import org.apache.hadoop.fs.{Path, FSInputStream}
import java.io.InputStream
import tj.model.BlockMeta

case class SnackFSInputStream(store: SnackFSStore, path: Path) extends FSInputStream {

  private val stream: InputStream = null

  def seek(pos: Long) {}

  def getPos: Long = 0L

  def seekToNewSource(targetPos: Long): Boolean = false

  def read(): Int = 0

  override def available: Int = 0

  override def read(b: Array[Byte], off: Int, len: Int): Int = 0

  private val fileSize: Long = 0L

  override def close = {
    stream.close
    super.close
  }

  //  blocksMeta.foreach(blockMeta => fileSize += blockMeta.length)
}
