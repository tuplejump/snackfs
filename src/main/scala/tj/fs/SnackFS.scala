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
import scala.util.{Failure, Success, Try}

case class SnackFS(store: FileSystemStore) extends FileSystem {

  private var systemURI: URI = null
  private var currentDirectory: Path = null
  private var subBlockSize: Long = 0L

  private val AT_MOST: FiniteDuration = 10 seconds

  override def initialize(uri: URI, configuration: Configuration) = {
    super.initialize(uri, configuration)
    setConf(configuration)

    systemURI = URI.create(uri.getScheme + "://" + uri.getAuthority)

    val directory = new Path("/user", System.getProperty("user.name"))
    currentDirectory = makeQualified(directory)

    //store initialize skipped--passing it as an argument

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
    val mayBeiNode = Try(Await.result(store.retrieveINode(path), AT_MOST))

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

  private def mkdir(path: Path, permission: FsPermission): Boolean = {
    val mayBeiNode = Try(Await.result(store.retrieveINode(path), AT_MOST))

    var result = true
    mayBeiNode match {
      case Success(inode) =>
        if (inode.isFile) {
          result = false //Can't make a directory for path since its a file
        }
      case Failure(e) =>
        val user = System.getProperty("user.name", "none")
        val timestamp = System.currentTimeMillis()
        val iNode = INode(user, user, permission, FileType.DIRECTORY, null, timestamp)
        Await.ready(store.storeINode(path, iNode), AT_MOST)
    }
    result
  }

  def mkdirs(path: Path, permission: FsPermission): Boolean = {
    var absolutePath = makeAbsolute(path)
    var paths = List[Path]()
    var result = true
    while (absolutePath != null) {
      paths = paths :+ absolutePath
      absolutePath = absolutePath.getParent
    }
    result = paths.map(p => mkdir(p, permission)).reduce(_ && _)
    result
  }

  def create(filePath: Path, permission: FsPermission, overwrite: Boolean,
             bufferSize: Int, replication: Short, blockSize: Long,
             progress: Progressable): FSDataOutputStream = {

    val mayBeiNode = Try(Await.result(store.retrieveINode(filePath), AT_MOST))
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
    val maybeInode = Try(Await.result(store.retrieveINode(path), AT_MOST))
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
        if (fileStatus.getLen < start) {
          result = Array(new BlockLocation())
        }
        else {
          val iNode = fileStatus.asInstanceOf[SnackFileStatus].iNode
          val end = start + length

          val usedBlocks = iNode.blocks.filter(block =>
            ((start >= block.offset && start < (block.offset + block.length)) ||
              (end >= block.offset && end < (block.offset + block.length))) ||

              ((block.offset >= start && block.offset < end) ||
                ((block.offset + block.length) >= start && (block.offset + block.length) < end)))

          if (!usedBlocks.isEmpty) {
            /* check name and host */
            val name: Array[String] = Array("localhost:50010")
            val host: Array[String] = Array("localhost")

            result = usedBlocks.map(block => {
              val offset = if (usedBlocks.indexOf(block) == 0) start else block.offset
              new BlockLocation(name, host, offset, block.length)
            }).toArray
          }
        }
      }
    }
    result
  }

  def delete(path: Path, recursive: Boolean): Boolean = {
    val absolutePath = makeAbsolute(path)
    val mayBeiNode = Try(Await.result(store.retrieveINode(absolutePath), AT_MOST))
    var result = true
    mayBeiNode match {
      case Success(iNode: INode) =>
        if (iNode.isFile) {
          Await.ready(store.deleteINode(absolutePath), AT_MOST)
          Await.ready(store.deleteBlocks(iNode), AT_MOST)
        }
        else {
          val contents = listStatus(path)
          if (contents.length == 0) Await.ready(store.deleteINode(absolutePath), AT_MOST)
          else if (!recursive) throw new IOException("Directory is not empty")
          else result = contents.map(p => delete(p.getPath, recursive)).reduce(_ && _)
        }
      case Failure(e) => result = false //No such file
    }
    result
  }

  def rename(src: Path, dst: Path): Boolean = {
    val srcPath = makeAbsolute(src)
    val mayBeSrcINode = Try(Await.result(store.retrieveINode(srcPath), AT_MOST))
    mayBeSrcINode match {
      case Failure(e1) => throw new IOException("No such file or directory.%s".format(srcPath))
      case Success(iNode: INode) =>
        val dstPath = makeAbsolute(dst)
        val mayBeDstINode = Try(Await.result(store.retrieveINode(dstPath), AT_MOST))
        mayBeDstINode match {
          case Failure(e) => {
            val maybeDstParent = Try(Await.result(store.retrieveINode(dst.getParent), AT_MOST))
            maybeDstParent match {
              case Failure(e2) => throw new IOException("Destination %s directory does not exist.".format(dst.getParent))
              case Success(dstParentINode: INode) => {
                if (dstParentINode.isFile) {
                  throw new IOException("A file exists with parent of destination.")
                }
                if (iNode.isFile) {
                  Await.ready(store.deleteINode(srcPath), AT_MOST)
                  Await.ready(store.storeINode(dstPath, iNode), AT_MOST)
                }
                else {
                  mkdirs(dst)
                  val contents = Await.result(store.fetchSubPaths(srcPath, true), AT_MOST)
                  if (contents.size > 0) {
                    val srcPathString = src.toUri.getPath
                    val dstPathString = dst.toUri.getPath
                    val result: Boolean = contents.map(path => {
                      val oldPathString = path.toUri.getPath
                      val changedPathString = oldPathString.replace(srcPathString, dstPathString)
                      val changedPath = new Path(changedPathString)
                      mkdirs(changedPath.getParent)
                      rename(makeQualified(path), makeQualified(changedPath))
                    }).reduce(_ && _)
                    if (result) {
                      Await.ready(store.deleteINode(srcPath), AT_MOST)
                      Await.ready(store.storeINode(dstPath, iNode), AT_MOST)
                    }
                  }
                }
              }
            }
          }
          case Success(dstINode: INode) =>
            throw new IOException("A file or directory exists at %s, cannot overwrite".format(dst))
        }
    }
    true
  }

  def listStatus(path: Path): Array[FileStatus] = {
    var result: Array[FileStatus] = Array()
    val absolutePath = makeAbsolute(path)
    val mayBeiNode = Try(Await.result(store.retrieveINode(absolutePath), AT_MOST))
    mayBeiNode match {
      case Success(iNode: INode) =>
        if (iNode.isFile) {
          val fileStatus = SnackFileStatus(iNode, absolutePath)
          result = Array(fileStatus)
        } else {
          val subPaths = Await.result(store.fetchSubPaths(absolutePath, false), AT_MOST)
          result = subPaths.map(p => getFileStatus(makeQualified(p))).toArray
        }
      case Failure(e) => throw new FileNotFoundException("No such file exists")
    }
    result
  }
}
