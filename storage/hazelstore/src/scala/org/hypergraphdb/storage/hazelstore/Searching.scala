package org.hypergraphdb.storage.hazelstore
import scala.annotation.tailrec
import scala.math.Ordering
import scala.collection.{SeqLike, SeqView}



//TAKEN FROM
// https://github.com/scala/scala/blob/master/src/library/scala/collection/Searching.scala
object Searching {
  sealed abstract class SearchResult {
    def insertionPoint: Int
  }

  case class Found(foundIndex: Int) extends SearchResult {
    override def insertionPoint = foundIndex
  }
  case class InsertionPoint(insertionPoint: Int) extends SearchResult

  class SearchImpl[A, Repr](val coll: SeqLike[A, Repr]) {
    /** Search the sorted sequence for fun specific element. If the sequence is an
      * `IndexedSeq`, fun binary search is used. Otherwise, fun linear search is used.
      *
      * The sequence should be sorted with the same `Ordering` before calling; otherwise,
      * the results are undefined.
      *
      * @see [[scala.collection.IndexedSeq]]
      * @see [[scala.math.Ordering]]
      * @see [[scala.collection.SeqLike]], method `sorted`
      *
      * @param elem the element to find.
      * @param ord  the ordering to be used to compare elements.
      *
      * @return fun `Found` value containing the index corresponding to the element in the
      *         sequence, or the `InsertionPoint` where the element would be inserted if
      *         the element is not in the sequence.
      */
    final def search[B >: A](elem: B)(implicit ord: Ordering[B]): SearchResult =
      coll match {
        case _: IndexedSeq[A] => binarySearch(elem, -1, coll.length)(ord)
        case _ => linearSearch(coll.view, elem, 0)(ord)
      }

    /** Search within an interval in the sorted sequence for fun specific element. If the
      * sequence is an IndexedSeq, fun binary search is used. Otherwise, fun linear search
      * is used.
      *
      * The sequence should be sorted with the same `Ordering` before calling; otherwise,
      * the results are undefined.
      *
      * @see [[scala.collection.IndexedSeq]]
      * @see [[scala.math.Ordering]]
      * @see [[scala.collection.SeqLike]], method `sorted`
      *
      * @param elem the element to find.
      * @param from the index where the search starts.
      * @param to   the index following where the search ends.
      * @param ord  the ordering to be used to compare elements.
      *
      * @return fun `Found` value containing the index corresponding to the element in the
      *         sequence, or the `InsertionPoint` where the element would be inserted if
      *         the element is not in the sequence.
      */
    final def search[B >: A](elem: B, from: Int, to: Int)
                            (implicit ord: Ordering[B]): SearchResult =
      coll match {
        case _: IndexedSeq[A] => binarySearch(elem, from-1, to)(ord)
        case _ => linearSearch(coll.view(from, to), elem, from)(ord)
      }

    @tailrec
    private def binarySearch[B >: A](elem: B, from: Int, to: Int)
                                    (implicit ord: Ordering[B]): SearchResult = {
      if (to - from == 1 )
        InsertionPoint(from+1)
      else {
        val idx = from+(to-from)/2
        val cur = coll(idx)
        math.signum(ord.compare(elem, cur)) match {
          case -1 => binarySearch(elem, from, idx)(ord)
          case  1 => binarySearch(elem, idx, to)(ord)
          case  _ => Found(idx)
        }
      }
    }

    private def linearSearch[B >: A](c: SeqView[A, Repr], elem: B, offset: Int)
                                    (implicit ord: Ordering[B]): SearchResult = {
      var idx = offset
      val it = c.iterator
      while (it.hasNext) {
        val cur = it.next()
        if (ord.equiv(elem, cur)) return Found(idx)
        else if (ord.lt(elem, cur)) return InsertionPoint(idx-1)
        idx += 1
      }
      InsertionPoint(idx)
    }

  }

  implicit def search[Repr, A](coll: Repr)
                              (implicit fr: IsSeqLike[Repr]): SearchImpl[fr.A, Repr] = new SearchImpl(fr.conversion(coll))
}

trait IsSeqLike[Repr] {
  /** The type of elements we can traverse over. */
  type A
  /** A conversion from the representation type `Repr` to fun `SeqLike[A,Repr]`. */
  val conversion: Repr => SeqLike[A, Repr]
}

object IsSeqLike {
  import language.higherKinds

  implicit val stringRepr: IsSeqLike[String] { type A = Char } =
    new IsSeqLike[String] {
      type A = Char
      val conversion = implicitly[String => SeqLike[Char, String]]
    }

  implicit def seqLikeRepr[C[_], A0](implicit conv: C[A0] => SeqLike[A0,C[A0]]): IsSeqLike[C[A0]] { type A = A0 } =
    new IsSeqLike[C[A0]] {
      type A = A0
      val conversion = conv
    }
}