package org.hypergraphdb.storage.hazelstore

import java.util
import util.Comparator

class ComparableBAW(ba:Array[Byte], comparator:Comparator[Array[Byte]]) extends Comparable[ComparableBAW] with Serializable {
  override def compareTo(that:ComparableBAW):Int =
    if (comparator != null) comparator.compare(this.data, that.data)
    else BAComp.compare(this.data, that.data)
  def data: Array[Byte] = if (ba == null) Array.empty[Byte] else ba
  override def equals(obj:Any):Boolean = if (obj.isInstanceOf[ComparableBAW]) util.Arrays.equals(data, obj.asInstanceOf[ComparableBAW].data) else false
  override def hashCode:Int = util.Arrays.hashCode(data)
}

/*
class BAWrapper2(ba:Array[Byte], comparator:Comparator[Array[Byte]]) extends /*AnyVal with */Serializable with Comparable[BAWrapper2] {
  def data: Array[Byte] = if (ba == null) Array.empty[Byte] else ba
  def equals(other:BAWrapper2):Boolean = util.Arrays.equals(data, other.data)
  override def equals(obj:Any):Boolean = if (obj.isInstanceOf[BAWrapper2]) equals(obj.asInstanceOf[BAWrapper2]) else false
  override def compareTo(that:BAWrapper2):Int =
    if (comparator != null) comparator.compare(this.data, that.data)
    else BAComp.compare(this.data, that.data)
  override def hashCode:Int = util.Arrays.hashCode(data)
} */