package tj.fs

import org.apache.hadoop.fs._
import java.net.URI
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.hadoop.conf.Configuration
import java.io.IOException
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
      case Success(p) => if (p.isFile) {
        if (overwrite) {
          //OVERWRTIE THE FILE
        } else {
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

  def rename(src: Path, dst: Path): Boolean = false

  def delete(path: Path, recursive: Boolean): Boolean = false

  def listStatus(path: Path): Array[FileStatus] = null

  def setWorkingDirectory(newDir: Path) = {
    currentDirectory = makeAbsolute(newDir)
  }

  def getWorkingDirectory: Path = currentDirectory

  def getFileStatus(path: Path): FileStatus = null
}
