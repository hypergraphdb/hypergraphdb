package storage

import java.util.Comparator
import org.hypergraphdb.storage.BAUtils
import scala.concurrent.Future
import scala.util.Try
import scala.util.Success
import scala.concurrent.ExecutionContext
import scala.util.Failure
import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuffer

object help {

  val ByteArrayComparator = new Comparator[Array[Byte]] {
      def compare(left: Array[Byte], right: Array[Byte]): Int = {
        val differentByte = (left, right).zipped
          .map( (x,y) => (x.toInt & 0xff, y.toInt & 0xff))
          .find( (x, y) =>  x != y )
        differentByte.map( p => p._1 - p._2).getOrElse(left.length - right.length)
      }
  }

  def futureToFutureTry[T](future: Future[T])(implicit ec: ExecutionContext): Future[Try[T]] = 
    future map (Success(_)) recover { case x => Failure(x)}

  def allOf[T](futures: IterableOnce[Future[T]])(implicit ec: ExecutionContext) : Future[Seq[Try[T]]] = {
    val buf: List[Future[T]] = futures.iterator.foldLeft(List[Future[T]]())( (l, x) => x :: l)
    Future.sequence(buf map futureToFutureTry)
  }
    
  // def allOf[T](futures: Future[T]*)(implicit ec: ExecutionContext): Future[Seq[Try[T]]] = 
  //   Future.sequence(futures map futureToFutureTry)

}