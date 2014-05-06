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
package com.tuplejump.snackfs

import java.net.{InetAddress, Inet4Address, URI}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.hadoop.conf.Configuration
import scala.concurrent.duration._

import org.apache.hadoop.fs._
import com.twitter.logging.Logger
import java.util.UUID
import com.tuplejump.snackfs.api.model._
import com.tuplejump.snackfs.fs.model.BlockMeta
import com.tuplejump.snackfs.cassandra.store.ThriftStore
import com.tuplejump.snackfs.cassandra.partial.FileSystemStore
import com.tuplejump.snackfs.cassandra.model.SnackFSConfiguration
import com.tuplejump.snackfs.util.TryHelper
import scala.util.{Try, Failure, Success}
import sun.net.util.IPAddressUtil

case class SnackFS() extends FileSystem {

  private lazy val log = Logger.get(getClass)

  private var systemURI: URI = null
  private var currentDirectory: Path = null
  private var subBlockSize: Long = 0L

  private var atMost: FiniteDuration = null
  private var store: FileSystemStore = null
  private var customConfiguration: SnackFSConfiguration = _

  val processId = UUID.randomUUID()

  override def initialize(uri: URI, configuration: Configuration) = {
    log.debug("Initializing SnackFs")
    super.initialize(uri, configuration)
    setConf(configuration)

    systemURI = if (uri.getAuthority != null) URI.create(uri.getScheme + "://" + uri.getAuthority) else URI.create(uri.getScheme + ":///")

    val directory = new Path("/user", System.getProperty("user.name"))
    currentDirectory = makeQualified(directory)

    log.debug("generating required configuration")
    customConfiguration = SnackFSConfiguration.get(configuration)

    store = new ThriftStore(customConfiguration)
    atMost = customConfiguration.atMost
    val initialized: Try[Unit] = Try {
      store.createKeyspace.get
      store.init
      log.debug("creating base directory")
      mkdirs(new Path("/"))
      subBlockSize = customConfiguration.subBlockSize
    }

    initialized match {
      case Success(_) =>
        log.debug("Filesystem initialized")
      case Failure(ex) =>
        log.error( s"""Tried creating filesystem with Host: %s,
             |Port: %s on
             |System: %s""".stripMargin, customConfiguration.CassandraHost, customConfiguration.CassandraPort, InetAddress.getLocalHost)

        log.error(ex, "Failed to initialize File system store")

    }


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
    //bufferSize can be ignored since we are providing configurable blockSize and subBlockSize
    OpenFileCommand(store, makeAbsolute(path), atMost)
  }

  def mkdirs(path: Path, permission: FsPermission): Boolean = {
    val absolutePath = makeAbsolute(path)
    MakeDirectoryCommand(store, absolutePath, permission, atMost)
  }

  def create(filePath: Path, permission: FsPermission, overwrite: Boolean,
             bufferSize: Int, replication: Short, blockSize: Long,
             progress: Progressable): FSDataOutputStream = {

    //bufferSize can be ignored since we are providing configurable blockSize and subBlockSize
    CreateFileCommand(store, makeAbsolute(filePath), permission, overwrite, replication,
      blockSize, progress, processId, statistics, subBlockSize, atMost)
  }

  override def getDefaultBlockSize: Long = {
    customConfiguration.blockSize
  }

  def append(path: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = {
    AppendFileCommand(store, makeAbsolute(path), bufferSize, progress, atMost)
  }

  def getFileStatus(path: Path): FileStatus = {
    FileStatusCommand(store, makeAbsolute(path), atMost, this)
  }

  def delete(path: Path, isRecursive: Boolean): Boolean = {
    val absolutePath = makeAbsolute(path)
    DeleteCommand(store, absolutePath, isRecursive, atMost, this)
  }

  def rename(src: Path, dst: Path): Boolean = {
    val srcPath = makeAbsolute(src)
    val dstPath = makeAbsolute(dst)
    RenameCommand(store, srcPath, dstPath, atMost)
  }


  def listStatus(path: Path): Array[FileStatus] = {
    val absolutePath = makeAbsolute(path)
    ListCommand(store, absolutePath, atMost, this)
  }

  def delete(p1: Path): Boolean = delete(makeAbsolute(p1), isRecursive = false)

  def getFileBlockLocations(path: Path, start: Long, len: Long): Array[BlockLocation] = {
    val absolute = makeAbsolute(path)
    log.debug("fetching block locations for %s", absolute)
    val mayBeBlocks = TryHelper.handleFailure[(Path), Map[BlockMeta, List[String]]](store.getBlockLocations, absolute)
    val blocks: Map[BlockMeta, List[String]] = mayBeBlocks.get
    val locs = blocks.filterNot(x => x._1.offset + x._1.length < start)
    val locsMap = locs.map {
      case (b, ips) =>
        val bl = new BlockLocation()
        bl.setHosts(ips.toArray)
        bl.setNames(ips.map(i => "%s:%s".format(i, customConfiguration.CassandraPort)).toArray)
        bl.setOffset(b.offset)
        bl.setLength(b.length)
        bl
    }
    log.debug("got block location %s", locsMap)
    locsMap.toArray
  }

  override def getFileBlockLocations(file: FileStatus, start: Long, len: Long): Array[BlockLocation] = {
    getFileBlockLocations(file.getPath, start, len)
  }

  /**
   * Very bad method!!!
   * Checks if provided path structure conforms this FileSytem's path and throws exception if it doesn't.
   * @param path
   */
  override def checkPath(path: Path) {
    val thisUri = getUri
    val pathUri = path.toUri
    val thatScheme = pathUri.getScheme
    val thatAuthority = pathUri.getAuthority

    if (thatScheme == null) {
      if (thatAuthority == null) {
        if (!path.isAbsolute) {
          throw new IllegalArgumentException("relative paths not allowed:" + path)
        }
      } else {
        throw new IllegalArgumentException("Path without scheme with non-null authority:" + path)
      }
    } else {
      val thisScheme: String = thisUri.getScheme
      if (thisScheme.equalsIgnoreCase(thatScheme)) {
        if (thisUri.getAuthority != null && pathUri.getAuthority == null) {
          throw new IllegalArgumentException(s"FS Path has authority [${thisUri.getAuthority}] and provided path doesnt!")
        }
      } else {
        throw new IllegalArgumentException(s"FS Path Schemes do not match! Default: [$thisScheme]  Requested: [$thatScheme]!")
      }
    }
  }

}