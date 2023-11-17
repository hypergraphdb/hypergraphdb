/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.UUID;

/**
 * A logical DB backed by a specific column family
 * The logical DB supports multiple values per key
 * Both the keys and values are variable sized
 *
 * The rocks db keys in this column family have the following structure:
 * byte[0] unsigned
 * byte[1] a flag which stores the size of the logical key - key_size
 * byte[2 - key_size+2] the logical key
 * byte[key_size+2 - ...] the value for this record
 *
 * TODO only one byte for a size means the serialization of the key
 *    is
 *
 */
public class VarKeyVarValueColumnFamilyMultivaluedDB
{


   /**
    * The rocks DB key of the first possible value for a given logical key
    * i.e. the rocksdb key returned by this key is less than or equal to any
    * other rocks db key for the same logical key
    *
    * @param logicalKey
    * @return
    */
   public static byte[] firstRocksDBKey(byte[] logicalKey)
   {
      return makeVirtualRocksDBKey(logicalKey, RangeEdge.START);
   }

   public static byte[] lastRocksDBKey(byte[] logicalKey)
   {
      return makeVirtualRocksDBKey(logicalKey, RangeEdge.END);
   }
   public static byte[] globallyFirstRocksDBKey()
   {
      return makeVirtualRocksDBKey(new byte[0], RangeEdge.GLOBAL_END);
   }
   public static byte[] globallyLastRocksDBKey()
   {
      return makeVirtualRocksDBKey(new byte[0], RangeEdge.GLOBAL_START);
   }

   private enum RangeEdge
   {
      START((byte)0b0000_0001), END((byte)0b0000_0010),
      GLOBAL_START((byte)0b0000_0011), GLOBAL_END((byte)0b0000_0100);

      private final byte flag;
      RangeEdge(byte flag)
      {
         this.flag = flag;
      }

   }

   /**
    * The system key is a key which does not actually
    * @param logicalKey
    * @param edge
    * @return
    */
   private static byte[] makeVirtualRocksDBKey(byte[] logicalKey, RangeEdge edge)
   {
      byte[] result = new byte[logicalKey.length + 2];

      result[0] = (byte) logicalKey.length;
      result[1] = edge.flag;

      System.arraycopy(logicalKey, 0, result, 2, logicalKey.length);

      return result;

   }

   public static byte[] makeRocksDBKey(byte[] logicalKey, byte[] value)
   {

      byte[] result = new byte[logicalKey.length + value.length + 2];
      /*
      TODO
         casting int to byte. consider range and overflow!

       */
      if (logicalKey.length > 255)
      {
         /*
         TODO This obviously is not ok.
         Determine whether we can use this scheme (just with more bytes)
         or we need something else
          */
         throw new IllegalArgumentException("Keys larger than 255 bytes are not supported.");
      }
      result[0] =  (byte) logicalKey.length;
      System.arraycopy(logicalKey, 0, result, 2, logicalKey.length);
      System.arraycopy(value, 0, result, 2 + logicalKey.length, value.length);

      return result;
   }

   /**
    * extract the part of the rocksdb key which represents the logical key
    * @param keyvalue
    * @return
    */
   public static byte[] extractKey(byte[] keyvalue)
   {
      var keySize = Byte.toUnsignedInt(keyvalue[0]);

      byte[] res = new byte[keySize];
      System.arraycopy(keyvalue, 2, res, 0, keySize);

      return res;
   }

   public static byte[] extractValue(byte[] keyvalue)
   {
      var keySize = Byte.toUnsignedInt(keyvalue[0]);

      int valueSize = keyvalue.length - keySize - 2;
      byte[] res = new byte[valueSize];
      System.arraycopy(keyvalue, keySize + 2, res, 0, valueSize);

      return res;
   }

   /**
    * Compare two rocks db keys.
    * HGDB API tells the user to supply comparators for byte[]
    * but RocksDB expects ByteBuffer (which are actually DirectBuffer) i.e. are
    * outside the VM's heap and are not backed by a byte[]
    * @param keyComparator
    * @param valueComparator
    * @return
    */
   public static int compareRocksDBKeys(
           ByteBuffer buffer1,
           ByteBuffer buffer2,
           Comparator<byte[]> keyComparator,
           Comparator<byte[]> valueComparator)
   {
//      buffer1.rewind();
//      buffer2.rewind();

      int keysize1 = Byte.toUnsignedInt(buffer1.get());
      int keysize2 = Byte.toUnsignedInt(buffer2.get());

      byte isSystem1 = buffer1.get();
      byte isSystem2 = buffer2.get();

      if (isGloballyFirst(isSystem1) || isGloballyLast(isSystem2))
      {
         return -1;
      }

      if (isGloballyLast(isSystem1) || isGloballyFirst(isSystem2))
      {
         return 1;
      }

      /*
      TODO this is not efficient, we would like to compare the logical
         key/values byte by byte or ideally with memcmp
       */
      byte[] keyA = new byte[keysize1];
      byte[] keyB = new byte[keysize2];

      buffer1.get(keyA);
      buffer2.get(keyB);

      int keyComp;
      if (keyComparator != null)
      {
         keyComp = keyComparator.compare(keyA, keyB);
      }
      else
      {
         keyComp = arrayCmp(keyA, keyB);
      }

      if (keyComp != 0) return keyComp;
      else
      {
         if (isFirst(isSystem1))
         {
            if (isFirst(isSystem2))
               return 0;
            else
               return -1;
         }
         else if (isLast(isSystem1))
         {
            if (isLast(isSystem2))
               return 0;
            else
               return 1;
         }
         else if (isLast(isSystem2))
         {
            return -1;
         }
         else if (isFirst(isSystem2))
         {
            return 1;
         }
         else
         {
            ByteArrayOutputStream valueA = new ByteArrayOutputStream();
            ByteArrayOutputStream valueB = new ByteArrayOutputStream();

            while (buffer1.hasRemaining())
               valueA.write(buffer1.get());

            while (buffer2.hasRemaining())
               valueB.write(buffer2.get());

            if (valueComparator != null)
               return valueComparator.compare(valueA.toByteArray(), valueB.toByteArray());
            else
               return arrayCmp(valueA.toByteArray(), valueB.toByteArray());

         }

      }
   }


   private static final boolean isGloballyFirst(byte systemFlag)
   {
      return systemFlag == RangeEdge.GLOBAL_START.flag;
   }

   public static final boolean isGloballyLast(byte systemFlag)
   {
      return systemFlag == RangeEdge.GLOBAL_END.flag;
   }

   private static final boolean isSystem(byte systemFlag)
   {
      return systemFlag != 0b0000_0000;
   }
   private static final boolean isFirst(byte systemFlag)
   {
      return systemFlag == RangeEdge.START.flag;
   }
   private static final boolean isLast(byte systemFlag)
   {
      return systemFlag == RangeEdge.END.flag;
   }


   public static void main(String[] args)
   {
      /*
      test comparisons
       */
      byte[] k1bytes = asBytes(UUID.randomUUID());
      byte[] k2bytes = asBytes(UUID.randomUUID());
      byte[] k3bytes = asBytes(UUID.randomUUID());
      byte[] k4bytes = asBytes(UUID.randomUUID());

      var stringBytes = "Hello world".getBytes(StandardCharsets.UTF_8);

      var first = firstRocksDBKey(stringBytes);
      var last = lastRocksDBKey(stringBytes);


      var kv1 = makeRocksDBKey(stringBytes, k1bytes);
      var kv2 = makeRocksDBKey(stringBytes, k2bytes);
      var kv3 = makeRocksDBKey(stringBytes, k3bytes);
      var kv4 = makeRocksDBKey(stringBytes, k4bytes);

//      System.out.printf("first - last: %s %n", compareRocksDBKeys(first, last, Arrays::compare, Arrays::compare));
//      System.out.printf("last - first: %s %n", compareRocksDBKeys(last, first, Arrays::compare, Arrays::compare));
//      System.out.printf(" kv1 - first: %s %n", compareRocksDBKeys(kv1, first, Arrays::compare, Arrays::compare));
//      System.out.printf(" kv1 - last : %s %n", compareRocksDBKeys(kv1, last, Arrays::compare, Arrays::compare));
//      System.out.printf(" last - kv1 : %s %n", compareRocksDBKeys(last, kv1, Arrays::compare, Arrays::compare));
//      System.out.printf(" first - kv1 : %s %n", compareRocksDBKeys(first, kv1, Arrays::compare, Arrays::compare));
//      System.out.printf(" kv2 - kv1 : %s %n", compareRocksDBKeys(kv2, kv1, Arrays::compare, Arrays::compare));
//      System.out.printf(" kv1 - kv2 : %s %n", compareRocksDBKeys(kv1, kv2, Arrays::compare, Arrays::compare));
//      System.out.printf(" kv1 - kv3 : %s %n", compareRocksDBKeys(kv1, kv3, Arrays::compare, Arrays::compare));
//      System.out.printf(" kv1 - kv4 : %s %n", compareRocksDBKeys(kv1, kv4, Arrays::compare, Arrays::compare));
//      System.out.printf(" kv2 - kv4 : %s %n", compareRocksDBKeys(kv2, kv4, Arrays::compare, Arrays::compare));
//      System.out.printf(" kv2 - kv3 : %s %n", compareRocksDBKeys(kv2, kv3, Arrays::compare, Arrays::compare));

   }



   private static byte[] asBytes(UUID uuid) {
      ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());
      return bb.array();
   }

   /*
   TODO this is slow but according to the tests we need to compare the
      bytes unsigned. we need to use something native and fast to compare
      not compare byte by byte
    */
   private static int arrayCmp(byte[] left, byte[] right)
   {
      int len = Math.min(left.length, right.length);
      for (int i = 0; i < len; i++)
      {
         if (left[i] != right[i])
         {
            return Byte.toUnsignedInt(left[i]) - Byte.toUnsignedInt(right[i]);
         }
      }
      return left.length - right.length;

   }



}
