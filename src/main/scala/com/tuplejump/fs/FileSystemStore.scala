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

import scala.concurrent.Future
import org.apache.hadoop.fs.Path
import java.util.UUID
import java.nio.ByteBuffer
import java.io.InputStream
import com.tuplejump.model._
import com.tuplejump.model.SubBlockMeta
import com.tuplejump.model.BlockMeta
import com.tuplejump.model.GenericOpSuccess

trait FileSystemStore {

  def createKeyspace: Future[Keyspace]

  def init: Unit

  def storeINode(path: Path, iNode: INode): Future[GenericOpSuccess]

  def retrieveINode(path: Path): Future[INode]

  def storeSubBlock(blockId: UUID, subBlockMeta: SubBlockMeta, data: ByteBuffer): Future[GenericOpSuccess]

  def retrieveSubBlock(blockId: UUID, subBlockId: UUID, byteRangeStart: Long): Future[InputStream]

  def retrieveBlock(blockMeta: BlockMeta): InputStream

  def deleteINode(path: Path): Future[GenericOpSuccess]

  def deleteBlocks(iNode: INode): Future[GenericOpSuccess]

  def fetchSubPaths(path: Path, isDeepFetch: Boolean): Future[Set[Path]]

  def getBlockLocations(path: Path): Future[Map[BlockMeta, List[String]]]
}
