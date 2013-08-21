package tj.fs

import org.apache.hadoop.fs._
import java.net.URI
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.hadoop.conf.Configuration
import java.io.{FileNotFoundException, IOException}
import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._
import tj.model.{FileType, INode}

import ExecutionContext.Implicits.global
import org.apache.cassandra.thrift.NotFoundException
import scala.util.{Failure, Success, Try}

case class SnackFS(store: FileSystemStore) extends FileSystem {

  private var systemURI: URI = null
  private var currentDirectory: Path = null
  private var subBlockSize: Long = 0L

  override def initialize(uri: URI, configuration: Configuration) = {
    super.initialize(uri, configuration)
    setConf(configuration)

    systemURI = URI.create(uri.getScheme + "://" + uri.getAuthority)

    val directory = new Path("/user", System.getProperty("user.name"))
    currentDirectory = makeQualified(directory)

    //store initialize skipped

    val defaultSubBLockSize = 256L * 1024L
    subBlockSize = configuration.getLong("fs.local.subblock.size", defaultSubBLockSize)
  }

  private def makeAbsolute(path: Path): Path = {
    if (path.isAbsolute) path else new Path(currentDirectory, path)
  }

  def getUri: URI = systemURI

  def setWorkingDirectory(newDir: Path) = {
    currentDirectory = makeAbsolute(newDir)
  }

  def getWorkingDirectory: Path = currentDirectory

  def open(path: Path, bufferSize: Int): FSDataInputStream = {
    val mayBeiNode = Try(Await.result(store.retrieveINode(path), 10 seconds))

    mayBeiNode match {
      case Success(inode) => {
        if (inode.isDirectory) {
          throw new IOException("Path %s is a directory.".format(path))
        }
        else {
          val fileStream = new FSDataInputStream(FileSystemInputStream(store, path))
          fileStream
        }
      }
      case Failure(e) => throw new IOException("No such file.")
    }
  }

  private def mkdir(path: Path, permission: FsPermission): Any = {
    val mayBeiNode = Try(Await.result(store.retrieveINode(path), 10 seconds))

    mayBeiNode match {
      case Success(inode) =>
        if (inode.isFile) {
          throw new IOException("Can't make a directory for path %s since its a file".format(path))
        }

      case Failure(e: NotFoundException) =>
        val user = System.getProperty("user.name", "none")
        val timestamp = System.currentTimeMillis()
        val iNode = INode(user, user, permission, FileType.DIRECTORY, null, timestamp)
        Await.ready(store.storeINode(path, iNode), 10 seconds)
    }
  }

  def mkdirs(path: Path, permission: FsPermission): Boolean = {
    var absolutePath = makeAbsolute(path)
    var paths = List[Path]()
    while (absolutePath != null) {
      paths = paths :+ absolutePath
      absolutePath = absolutePath.getParent
    }
    paths.foreach(p => mkdir(p, permission))
    true
  }

  def create(filePath: Path, permission: FsPermission, overwrite: Boolean,
             bufferSize: Int, replication: Short, blockSize: Long,
             progress: Progressable): FSDataOutputStream = {

    val mayBeiNode = Try(Await.result(store.retrieveINode(filePath), 10 seconds))
    mayBeiNode match {
      case Success(p) => {
        if (p.isFile && !overwrite) {
          throw new IOException("File exists and cannot be overwritten")
        }
      }
      case Failure(e) =>
        val parentPath = filePath.getParent
        if (parentPath != null) {
          mkdirs(parentPath)
        }
    }
    val fileStream = new FileSystemOutputStream(store, filePath, blockSize, subBlockSize, bufferSize)
    val fileDataStream = new FSDataOutputStream(fileStream, statistics)
    fileDataStream
  }

  def append(path: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = {
    throw new IOException("Appending to existing file is not supported.")
  }

  private def length(iNode: INode): Long = {
    var result = 0L
    if (iNode.isFile) {
      result = iNode.blocks.map(_.length).sum
    }
    result
  }

  private def blockSize(iNode: INode): Long = {
    var result = 0L
    if (iNode.blocks != null && iNode.blocks.length > 0) {
      result = iNode.blocks(0).length
    }
    result
  }

  private case class SnackFileStatus(iNode: INode, path: Path) extends FileStatus(length(iNode), iNode.isDirectory, 0,
    blockSize(iNode), iNode.timestamp, 0, iNode.permission, iNode.user, iNode.group, path: Path) {
  }

  def getFileStatus(path: Path): FileStatus = {
    val maybeInode = Try(Await.result(store.retrieveINode(path), 10 seconds))
    maybeInode match {
      case Success(iNode: INode) => SnackFileStatus(iNode, path)
      case Failure(e) => throw new FileNotFoundException("No such file exists")
    }
  }

  override def getFileBlockLocations(fileStatus: FileStatus, start: Long, length: Long) = {
    var result: Array[BlockLocation] = null
    if (fileStatus != null) {

      if (!fileStatus.isInstanceOf[SnackFileStatus]) {
        super.getFileBlockLocations(fileStatus, start, length)
      }
      else {
        if (start < 0 || length < 0) {
          throw new IllegalArgumentException("Invalid start or length parameter")
        }
        else if (fileStatus.getLen > 0) {
          val iNode = fileStatus.asInstanceOf[SnackFileStatus].iNode
          val end = start + length

          val usedBlocks = iNode.blocks.filter(block =>
            ((start >= block.offset && start < (block.offset + block.length)) ||
              (end >= block.offset && end < (block.offset + block.length))) ||

              ((block.offset >= start && block.offset < end) ||
                ((block.offset + block.length) >= start && (block.offset + block.length) < end)))

          if (!usedBlocks.isEmpty) {
            result = usedBlocks.map(block => {
              val offset = if (usedBlocks.indexOf(block) == 0) start else block.offset
              new BlockLocation(null, null, offset, block.length)
            }).toArray
          }
        }
      }
    }
    result
  }

  def rename(src: Path, dst: Path): Boolean = false

  def delete(path: Path, recursive: Boolean): Boolean = false

  def listStatus(path: Path): Array[FileStatus] = null
}
