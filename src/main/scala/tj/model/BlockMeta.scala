package tj.model

import java.util.UUID

case class BlockMeta(id: UUID, offset: Long, length: Long, subBlocks: Seq[SubBlockMeta]) {
  override def toString = {
    val result = "Block[" + (id,offset,length).toString() + "]"
    result
  }
}
