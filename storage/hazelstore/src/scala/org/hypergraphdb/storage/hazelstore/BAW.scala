package org.hypergraphdb.storage.hazelstore

import java.util
import java.util.Comparator


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

object BAWComparator extends Comparator[BAW]{
  def compare(o1: BAW, o2: BAW): Int = BAComp.compare(o1.data,o2.data)
}