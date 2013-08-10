package tj.model

import java.util.UUID

case class SubBlockMeta(id:UUID,offset:Long,length:Long) {
  override def toString= {
    val result = "SubBlock["+(id,offset,length).toString()+"]"
    result
  }
}
