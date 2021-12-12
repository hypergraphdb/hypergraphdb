package storage

import java.util.Comparator
import org.hypergraphdb.storage.BAUtils

object help {

    val ByteArrayComparator = new Comparator[Array[Byte]] {
        def compare(left: Array[Byte], right: Array[Byte]): Int = {
          val differentByte = (left, right).zipped
            .map( (x,y) => (x.toInt & 0xff, y.toInt & 0xff))
            .find( (x, y) =>  x != y )
          differentByte.map( p => p._1 - p._2).getOrElse(left.length - right.length)
        }
    }
}