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
package com.tuplejump.model

import java.util.UUID
import org.apache.hadoop.fs.Path
import java.net.URI
import org.apache.hadoop.fs.permission.FsPermission
import org.scalatest.FlatSpec
import java.io.ByteArrayInputStream

class INodeSpec extends FlatSpec {

  val timestamp = System.currentTimeMillis()
  val subBlocks = List(SubBlockMeta(UUID.randomUUID, 0, 128), SubBlockMeta(UUID.randomUUID, 128, 128))
  val blocks = List(BlockMeta(UUID.randomUUID, 0, 256, subBlocks), BlockMeta(UUID.randomUUID, 0, 256, subBlocks))
  val path = new Path(URI.create("jquery.fixedheadertable.min.js"))
  val iNode = INode("user", "group", FsPermission.getDefault, FileType.FILE, blocks, timestamp)

  it should "result in correct serialization for a file" in {
    val input = new ByteArrayInputStream(iNode.serialize.array)
    assert(iNode === INode.deserialize(input, timestamp))
  }
}
