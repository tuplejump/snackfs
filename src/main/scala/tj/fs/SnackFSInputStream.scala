package tj.fs

import org.apache.hadoop.fs.FSInputStream

class SnackFSInputStream extends FSInputStream {
  def seek(pos: Long) {}

  def getPos: Long = 0L

  def seekToNewSource(targetPos: Long): Boolean = false

  def read(): Int = 0
}
