package storage

import java.util.Comparator

object help {

    val ByteArrayComparator = new Comparator[Array[Byte]] {
        def compare(left: Array[Byte], right: Array[Byte]): Int = {
          val differentByte = (left, right).zipped.find( (x, y) => (x & 0xff) != (y & 0xff) )
          differentByte.map( p => p._1 - p._2).getOrElse(left.length - right.length)
        }
    }
}
