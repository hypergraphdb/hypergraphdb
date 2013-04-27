package org.hypergraphdb.storage.hazelstore

import org.hypergraphdb.storage._
import java.util
import scala.util.hashing.MurmurHash3.bytesHash
import util.Comparator
import com.hazelcast.core.{PartitionAware, AtomicNumber, ISet, IMap}
import scala.Tuple5
import scala.Some
import org.hypergraphdb.{HGRandomAccessResult, HGPersistentHandle}
import scala.util.hashing.MurmurHash3
import scala.annotation.tailrec
import java.io.{DataOutput, DataInput}

object Common {
  type O[T] = Option[T]

  def sort(bas: Seq[Array[Byte]], comparator:Comparator[BA]): Seq[Array[Byte]] = {
    val localComp = if (comparator == null) BAComparator else comparator
    val sortedBas = bas.sortWith {
      case (first, next) => localComp.compare(first, next) > 0
    }
    sortedBas
  }

  def streamCons[T,R](st:Seq[T], fun: T => R, accu: Stream[R] = Stream.empty[R]):Stream[R]= st match {
    case a: Seq[T] if(a.isEmpty) => accu
    case a :: b  => streamCons(b, fun, fun(a) #:: accu)
  }


  def ifNotNull[T,R](input:T, f: T => R):R = if(input == null) null.asInstanceOf[R] else f(input)
  def ifNotNullTwice[T,R](input:T)(f: T => R):R =
    if(input == null) null.asInstanceOf[R]
    else {
      val res = f(input)
      if (res == null)
        null.asInstanceOf[R]
      else
        res
    }

  object BAComparator extends ByteArrayComparator

  //  case class KeyAndValueHash(first:Int, second:Int, third:Int, fourth:Int, fifth:Int, sixth:Int, seventh:Int, eighth:Int, nineth:Int,  tenth:Int)
  //type FiveInt = Tuple5[Int, Int, Int, Int, Int]
  //  type TenInt = Tuple10[Int, Int, Int, Int, Int,Int, Int, Int, Int, Int]


  class FiveInt(val i1:Int,val i2:Int,val i3:Int,val i4:Int,val i5:Int) extends Serializable {
    override def equals(o:Any) =
      if (!o.isInstanceOf[FiveInt]) false
      else {
        val other5 = o.asInstanceOf[FiveInt]
        if      (!this.i1.equals(other5.i1)) false
        else if (!this.i2.equals(other5.i2)) false
        else if (!this.i3.equals(other5.i3)) false
        else if (!this.i4.equals(other5.i4)) false
        else if (!this.i5.equals(other5.i5)) false
        else true
      }
    override def hashCode = MurmurHash3.seqHash(List(i1,i2,i3,i4,i5))
  }


  /*
  class FiveInt(private var i1:Int,private var i2:Int, private var i3:Int,private var i4:Int,private var i5:Int) extends com.hazelcast.nio.DataSerializable{
    override def equals(o:Any) =
      if (!o.isInstanceOf[FiveInt]) false
      else {
        val other5 = o.asInstanceOf[FiveInt]
        if      (!this.i1.equals(other5.i1)) false
        else if (!this.i2.equals(other5.i2)) false
        else if (!this.i3.equals(other5.i3)) false
        else if (!this.i4.equals(other5.i4)) false
        else if (!this.i5.equals(other5.i5)) false
        else true
      }
    override def hashCode = MurmurHash3.seqHash(List(i1,i2,i3,i4,i5))

    def writeData(out: DataOutput) {
      out.writeInt(i1)
      out.writeInt(i2)
      out.writeInt(i3)
      out.writeInt(i4)
      out.writeInt(i5)
    }

    def readData(in: DataInput) {
      i1 = in.readInt()
      i2 = in.readInt()
      i3 = in.readInt()
      i4 = in.readInt()
      i5 = in.readInt()
    }
  }
  */

  object FiveInt{
    def apply(i1:Int,i2:Int,i3:Int,i4:Int,i5:Int) = new FiveInt(i1,i2,i3,i4,i5)
  }

/*  class TenInt(val keyHash: FiveInt, val valHash: FiveInt) extends PartitionAware[FiveInt] with Serializable{    // possible optimizations: simple class maybe more lightweight? => implement equal / hashCode by Hand
    override def getPartitionKey():FiveInt = keyHash
    override def equals(o:Any) =
      if (! o.isInstanceOf[TenInt]) false
      else {
        val other = o.asInstanceOf[TenInt]
        if (!this.keyHash.equals(other.keyHash)) false
        else if(!this.valHash.equals(other.valHash)) false
        else true
      }
    override def hashCode = MurmurHash3.seqHash(List(keyHash.i1,keyHash.i2,keyHash.i3,keyHash.i4,keyHash.i5,valHash.i1,valHash.i2,valHash.i3,valHash.i4,valHash.i5))
  }

  object TenInt {def apply(keyHash:FiveInt, valHash:FiveInt) = new TenInt(keyHash,valHash)}

  def twoBa2tenIntHash(keyHash: FiveInt, valBA: BA): TenInt =
    TenInt(keyHash, hashBaTo5Int(valBA))

  */
  type BA = Array[Byte]

  def hashBaTo5Int(ba: BA): FiveInt =
    if (ba == null)
    {
      println("returning (0,0,0,0,0) as Hash for null byte array")
      //(0,0,0,0,0)     // TODO -- check if valid default value
      FiveInt(0,0,0,0,0)
    }
    else {
      val length = ba.length
      val chnk = if (length % 5 != 0) (length / 5) + 1 else length / 5
      /*(bytesHash(ba.take(chnk)),
        bytesHash(ba.drop(chnk).take(chnk)),
        bytesHash(ba.drop(2*chnk).take(chnk)),
        bytesHash(ba.drop(3*chnk).take(chnk)),
        bytesHash(ba.drop(4*chnk)))*/
      FiveInt(bytesHash(ba.take(chnk)),
        bytesHash(ba.drop(chnk).take(chnk)),
        bytesHash(ba.drop(2*chnk).take(chnk)),
        bytesHash(ba.drop(3*chnk).take(chnk)),
        bytesHash(ba.drop(4*chnk)))
  }


  def persistentH2BAWrapper(ph: HGPersistentHandle, comparator:Comparator[Array[Byte]]): ComparableBAW = new ComparableBAW(ph.toByteArray, comparator)


  /*
  // Collection[T] => HGRandomAccessResult[T]
  def hgRARS[T](t: util.Collection[T], comp:Comparator[T] = null): HGRandomAccessResult[T] =
    if (t == null || t.size == 0) {
      EmptySR.asInstanceOf[HGRandomAccessResult[T]]
    }
    else {
      //val ars = new ArrayBasedSet[T with Object](t.toSeq.toArray.asInstanceOf[Array[T with Object]])
      //  creates "error: No ClassTag available for ValueType"... all over teh place
      //val tarr:Array[T @uncheckedVariance] = mkArray(t.toSeq:_*)
      // where def mkArray[T:ClassTag](a:T*):Array[T] = Array[T](a:_*)
      // also produces weird failures

      val arrayBased = true

      if (arrayBased){
        // val tarr2 = java.lang.reflect.Array.newInstance(t.head.getClass, t.size()).asInstanceOf[Array[T @uncheckedVariance]]; val ars = new ArrayBasedSet[_ >: T](tarr2.asInstanceOf[Array[T @uncheckedVariance]]); ars.getSearchResult.asInstanceOf[HGRandomAccessResult[T]]

        val a = JUtils.hgRARS(t) //if (comp == null) JUtils.hgRARS(t) else JUtils.hgRARS(t, comp)
        a
      }

      else
      {
        val tree: LLRBTreeCountMe[T] = new LLRBTreeCountMe[T]() // if (comp == null) new LLRBTreeCountMe[T]() else new LLRBTreeCountMe[T](comp)
        tree.addAll(t)
        //t.foreach(k => tree.add(k))
        tree.getSearchResult
      }
    }

*/

  def getOrElseUpdated[K, V](m: Map[K, V], key: K, op: => V): (Map[K, V], V) =
  {
    val getInExistingMap = m.get(key)
    getInExistingMap match
    {
      case Some(value) => (m, value)
      case None => {
        val newval = op;
        val updated = m.updated(key, newval)
        (updated, newval)
      }
    }
  }

      //def getClazzz[T](atc:util.Collection[T]):Class[T] = atc.head.getClass

  // T <-> Byte Array
  def toBA[T](t: T)(implicit converter: ByteArrayConverter[T]): BA = if (t == null) null.asInstanceOf[BA] else converter.toByteArray(t)
  def baToT[T](ba: BA)(implicit converter: ByteArrayConverter[T]): T =
    if (ba == null) null.asInstanceOf[T]
    else converter.fromByteArray(ba, 0, ba.length)



  // ByteArray <-> ByteArrayWrapper
  implicit def ba2BAW (ba:BA, comparator:Comparator[Array[Byte]]):ComparableBAW = new  ComparableBAW(ba, comparator)
  implicit def baw2Ba (baw:ComparableBAW ):BA = if(baw == null) null else baw.data

  // T <-> BAWrapper
  def pack[T](t: T)(implicit tc: ByteArrayConverter[T], comparator:Comparator[Array[Byte]]): ComparableBAW = new ComparableBAW(toBA[T](t)(tc), comparator)

  def unpack[T](t: ComparableBAW)(implicit tc: ByteArrayConverter[T]): T = if(t == null) null.asInstanceOf[T] else baToT[T](t.data)(tc)

  object SideEffects {
    implicit def anyWithSideEffects[T](any: T) = new {
      def ~(fn: T => Unit) = {
        fn(any)
        any
      }
    }
  }

}

