/*
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Tuplejump Software Pvt. Ltd. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.tuplejump.fs

import java.net.URI
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.hadoop.conf.Configuration
import java.io.{FileNotFoundException, IOException}
import scala.concurrent.Await
import scala.concurrent.duration._

import scala.util.{Failure, Success, Try}
import org.apache.thrift.async.TAsyncClientManager
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TNonblockingSocket
import org.apache.cassandra.thrift.Cassandra.AsyncClient
import org.apache.cassandra.thrift.Cassandra.AsyncClient.set_keyspace_call
import org.apache.cassandra.thrift.ConsistencyLevel
import org.apache.cassandra.locator.SimpleStrategy
import com.tuplejump.util.AsyncUtil
import com.tuplejump.model.{FileType, INode}
import org.apache.hadoop.fs._

case class SnackFS() extends FileSystem {

  private var systemURI: URI = null
  private var currentDirectory: Path = null
  private var subBlockSize: Long = 0L

  private val AT_MOST: FiniteDuration = 10 seconds
  private var store: FileSystemStore = null

  private def createStore(client: AsyncClient, writeLevel: String, readLevel: String): FileSystemStore = {
    var fileSystemStore: FileSystemStore = null
    if (writeLevel == null && readLevel == null) {
      fileSystemStore = new ThriftStore(client)
    } else if (writeLevel != null && readLevel == null) {
      val writeConsistency = ConsistencyLevel.valueOf(readLevel)
      fileSystemStore = new ThriftStore(client, writeConsistency)
    } else if (writeLevel == null && readLevel != null) {
      val readConsistency = ConsistencyLevel.valueOf(readLevel)
      fileSystemStore = new ThriftStore(client, consistencyLevelRead = readConsistency)
    } else {
      val writeConsistency = ConsistencyLevel.valueOf(writeLevel)
      val readConsistency = ConsistencyLevel.valueOf(readLevel)
      fileSystemStore = new ThriftStore(client, writeConsistency, readConsistency)
    }
    fileSystemStore
  }

  override def initialize(uri: URI, configuration: Configuration) = {
    super.initialize(uri, configuration)
    setConf(configuration)

    systemURI = URI.create(uri.getScheme + "://" + uri.getAuthority)

    val directory = new Path("/user", System.getProperty("user.name"))
    currentDirectory = makeQualified(directory)

    var host = configuration.get("fs.cassandra.host")
    if (host == null) {
      host = "127.0.0.1"
    }
    val port = configuration.getInt("fs.cassandra.port", 9160)

    val clientManager = new TAsyncClientManager()
    val protocolFactory = new TBinaryProtocol.Factory()
    val transport = new TNonblockingSocket(host, port)

    def client = new AsyncClient(protocolFactory, clientManager, transport)

    val consistencyLevelWrite = configuration.get("fs.consistencyLevel.write")
    val consistencyLevelRead = configuration.get("fs.consistencyLevel.read")

    store = createStore(client, consistencyLevelWrite, consistencyLevelRead)

    var keyspaceName = configuration.get("fs.keyspace")
    if (keyspaceName == null) {
      keyspaceName = "snackfs"
    }
    val replicationFactor = configuration.getInt("fs.replicationFactor", 3)
    var replicationStrategy = configuration.get("fs.replicationStrategy")
    if (replicationStrategy == null) {
      replicationStrategy = classOf[SimpleStrategy].getCanonicalName
    }

    Await.ready(store.createKeyspace(store.buildSchema(keyspaceName, replicationFactor, replicationStrategy)), AT_MOST)
    Await.ready(AsyncUtil.executeAsync[set_keyspace_call](client.set_keyspace(keyspaceName, _)), AT_MOST)

    val defaultSubBLockSize = 256 * 1024L
    subBlockSize = configuration.getLong("fs.subblock.size", defaultSubBLockSize)
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
        val user = System.getProperty("user.name")
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
          else {
            result = contents.map(p => delete(p.getPath, recursive)).reduce(_ && _)
            Await.ready(store.deleteINode(absolutePath), AT_MOST)
          }
        }
      case Failure(e) => result = false //No such file
    }
    result
  }

  def rename(src: Path, dst: Path): Boolean = {
    if (src != dst) {
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
                case Failure(e2) =>
                  throw new IOException("Destination %s directory does not exist.".format(dst.getParent))
                case Success(dstParentINode: INode) => {
                  if (dstParentINode.isFile) {
                    throw new IOException("A file exists with parent of destination.")
                  }
                  if (iNode.isDirectory) {
                    renameDir(src, dst)
                  }
                  renameINode(srcPath, dstPath, iNode)
                }
              }
            }
            case Success(dstINode: INode) =>
              if (dstINode.isFile) {
                throw new IOException("A file %s already exists".format(dstPath))
              }
              else {
                var dstPathString = dstPath.toUri.getPath
                if (!dstPathString.endsWith("/")) {
                  dstPathString = dstPathString + "/"
                }
                val fileName = src.getName
                val updatedPath = new Path(dstPathString + fileName)

                val mayBeExistingINode = Try(Await.result(store.retrieveINode(updatedPath), AT_MOST))

                mayBeExistingINode match {
                  case Failure(e) =>
                    if (iNode.isFile) {
                      renameINode(srcPath, updatedPath, iNode)
                    } else {
                      renameDir(srcPath, updatedPath)
                    }
                  case Success(existingINode: INode) =>
                    if (existingINode.isFile) {
                      if (iNode.isFile) {
                        renameINode(srcPath, updatedPath, iNode)
                      } else
                        throw new IOException("cannot overwrite non-directory with a directory")
                    } else {
                      if (iNode.isFile)
                        throw new IOException("cannot overwrite directory with a non-directory")
                      else {
                        val contents = Await.result(store.fetchSubPaths(updatedPath, false), AT_MOST)
                        if (contents.size > 0)
                          throw new IOException("cannot move %s to %s - directory not empty".format(src, dst))
                        else
                          renameDir(srcPath, updatedPath)
                      }
                    }
                }
              }
          }
      }
    }
    true
  }

  def renameINode(originalPath: Path, updatedPath: Path, iNode: INode) = {
    Await.ready(store.deleteINode(originalPath), AT_MOST)
    Await.ready(store.storeINode(updatedPath, iNode), AT_MOST)
  }

  def renameDir(src: Path, dst: Path) = {
    val srcPath = makeAbsolute(src)
    mkdirs(dst)
    val contents = Await.result(store.fetchSubPaths(srcPath, true), AT_MOST)
    if (contents.size > 0) {
      val srcPathString = src.toUri.getPath
      val dstPathString = dst.toUri.getPath
      contents.map(path => {
        val actualINode = Await.result(store.retrieveINode(makeQualified(path)), AT_MOST)
        val oldPathString = path.toUri.getPath
        val changedPathString = oldPathString.replaceFirst(srcPathString, dstPathString)
        val changedPath = new Path(changedPathString)
        mkdirs(changedPath.getParent)
        renameINode(makeQualified(path), changedPath, actualINode)
      })
    }
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

  def delete(p1: Path): Boolean = delete(p1, false)

  /* override def getFileBlockLocations(file: FileStatus, start: Long, len: Long): Array[BlockLocation] = {
    val blocks = store.getBlockLocations(keyspace)
  }  */
}
