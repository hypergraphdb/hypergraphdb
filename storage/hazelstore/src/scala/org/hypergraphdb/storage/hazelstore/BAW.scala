package org.hypergraphdb.storage.hazelstore

import java.util


class BAW(ba:Array[Byte]) extends  Serializable {
  def data: Array[Byte] = if (ba == null) Array.empty[Byte] else ba
  override def equals(obj:Any):Boolean = if (obj.isInstanceOf[BAW]) util.Arrays.equals(data, obj.asInstanceOf[BAW].data) else false
  override def hashCode:Int = util.Arrays.hashCode(data)
}

object BAW{
  def apply(ba:Array[Byte]):BAW =
    if(ba == null || ba.length == 0) EmptyBAW
    else new BAW(ba)
}

object EmptyBAW extends BAW(Array.empty[Byte])