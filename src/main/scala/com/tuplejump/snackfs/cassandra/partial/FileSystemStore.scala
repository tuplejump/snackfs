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
package com.tuplejump.snackfs.cassandra.partial

import org.apache.hadoop.fs.Path
import java.util.UUID
import java.nio.ByteBuffer
import java.io.InputStream
import com.tuplejump.snackfs.fs.model._
import com.tuplejump.snackfs.cassandra.model.{GenericOpSuccess, Keyspace}
import scala.util.Try

trait FileSystemStore {

  def createKeyspace: Try[Keyspace]

  def init: Unit

  def storeINode(path: Path, iNode: INode): Try[GenericOpSuccess]

  def retrieveINode(path: Path): Try[INode]

  def storeSubBlock(blockId: UUID, subBlockMeta: SubBlockMeta, data: ByteBuffer): Try[GenericOpSuccess]

  def retrieveSubBlock(blockId: UUID, subBlockId: UUID, byteRangeStart: Long): Try[InputStream]

  def retrieveBlock(blockMeta: BlockMeta): InputStream

  def deleteINode(path: Path): Try[GenericOpSuccess]

  def deleteBlocks(iNode: INode): Try[GenericOpSuccess]

  def fetchSubPaths(path: Path, isDeepFetch: Boolean): Try[Set[Path]]

  def getBlockLocations(path: Path): Try[Map[BlockMeta, List[String]]]

  def acquireFileLock(path: Path, processId: UUID): Boolean

  def releaseFileLock(path: Path): Boolean
}
