package org.hypergraphdb.storage.hazelstore

import org.hypergraphdb.HGRandomAccessResult
import org.hypergraphdb.util.CountMe
import java.util.Comparator
import org.hypergraphdb.storage.ByteArrayConverter
import com.sun.org.apache.bcel.internal.generic.GOTO
import org.hypergraphdb.HGRandomAccessResult.GotoResult

//import Common.binarySearch
import Searching._


class HazelRS3[T](col: IndexedSeq[Array[Byte]], sorted:Boolean = true)(implicit comparator: Comparator[Array[Byte]],converter:ByteArrayConverter[T]) extends HGRandomAccessResult[T] with CountMe {
  // BeforeFirst  => cur = -1
  // AfterLast    => cur = -2
  // this gets initialized only if goTo is actually used

  // Pseudo-Cursor
  var cur: Int = -1
  val lenght = col.length

  def hasPrev: Boolean =
    if (col == null || col.size == 0) false
    else if (cur == -2) true
    else if (cur > 0) true
    else false


  def prev(): T = {
    if (cur == -2)
      cur = col.size - 1
    else
      cur = cur - 1
    current()
  }

  def hasNext: Boolean =
    if (col == null || !col.headOption.isDefined) false
    else if (cur == -1) true
    else if (cur < col.size - 1 && cur >= 0) true
    else false


  def next(): T = {
    cur = cur + 1
    current()
  }

  def remove() {    ???   }

  def goTo(value: T, exactMatch: Boolean) =
  {
    val valBA = converter.toByteArray(value)
    val res = col.search[Array[Byte]](valBA)(Ordering.comparatorToOrdering(comparator))
    res match {
      case Found(i) => {
        cur = i
        GotoResult.found
      }
      case InsertionPoint(i) =>
        if(exactMatch)
          GotoResult.nothing
        else {
          println("i ist: " + i)
          cur = i
          GotoResult.close
        }
    }
  }

  def goAfterLast() {
    cur = -2
  }

  def goBeforeFirst() {
    cur = -1
  }

  def close() {}

  override def toString = col.mkString

  def count() = col.size

  def current() = {
    val a  =  col(cur)
    converter.fromByteArray(a,0,a.length)
  }

  def isOrdered = sorted
}



object HazelRS3{
  def apply[T](col: IndexedSeq[Array[Byte]], sorted:Boolean = true)(implicit comparator: Comparator[Array[Byte]],converter:ByteArrayConverter[T]):HGRandomAccessResult[T] =
  if (col.size == 0)
    EmptySR.asInstanceOf[HGRandomAccessResult[T]]
  else
    new HazelRS3[T](col, sorted)(comparator, converter)

}
