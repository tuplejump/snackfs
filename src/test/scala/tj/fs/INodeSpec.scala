package tj.fs

import tj.model.{FileType, INode, BlockMeta, SubBlockMeta}
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
