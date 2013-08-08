package tj.fs

import org.apache.hadoop.fs._
import java.net.URI
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

class SnackFileSystem extends FileSystem {
  def getUri: URI = null

  def open(f: Path, bufferSize: Int): FSDataInputStream = null

  def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int, replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream = null

  def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = null

  def rename(src: Path, dst: Path): Boolean = false

  def delete(f: Path, recursive: Boolean): Boolean = false

  def listStatus(f: Path): Array[FileStatus] = null

  def setWorkingDirectory(new_dir: Path) {}

  def getWorkingDirectory: Path = null

  def mkdirs(f: Path, permission: FsPermission): Boolean = false

  def getFileStatus(f: Path): FileStatus = null
}
