package org.hypergraphdb.storage.hazelstore

import org.hypergraphdb.HGRandomAccessResult
import org.hypergraphdb.util.CountMe
import org.hypergraphdb.HGRandomAccessResult.GotoResult
import java.util.Comparator
import org.hypergraphdb.storage.ByteArrayConverter
import Common.binarySearch
import java.util


class HazelRS2[T](col: IndexedSeq[Array[Byte]], sorted:Boolean = true)(implicit comparator: Comparator[Array[Byte]],converter:ByteArrayConverter[T]) extends HGRandomAccessResult[T] with CountMe {
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
      cur = cur - 1;
    current()
  }

  def hasNext: Boolean =
    if (col == null || !col.headOption.isDefined) false
    else if (cur == -1) true
    else if (cur < col.size - 1 && cur >= 0) true
    else false


  def next(): T = {
    cur = cur + 1;
    current()
  }

  def remove() {    ???   }

  def goTo(value: T, exactMatch: Boolean) =
  if (sorted)
  {
    val valBA = converter.toByteArray(value)
    binarySearch(col, valBA, 0, lenght)(Ordering.comparatorToOrdering(comparator)) match {
        case Right(i)                       =>  { cur = i;GotoResult.found}
        case Left(i) if exactMatch == true  =>  GotoResult.nothing
        case Left(i) if exactMatch == false =>  { cur = i;GotoResult.close}
      }
  }
  else
  {
    println("in HazelRS2 goTo not sorted")
    val ba = converter.toByteArray(value)
    val idx = col.indexWhere(a => util.Arrays.equals(a,ba))
    if (idx >= 0) {
      cur = idx;
      GotoResult.found
    }
    else
    {
      val idx2 = col.lastIndexWhere(a => comparator.compare(a,ba)>0,lenght-1)
      if (idx2 < 0 || idx2 >= col.size - 1) // if "lastIndexWhere" element is last
        GotoResult.nothing

      else {
        cur = idx2 + 1
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

  def isOrdered = true

}


