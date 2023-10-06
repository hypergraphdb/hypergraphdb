/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb;

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
      return makeVirtualRocksDBKey(logicalKey, RangeEdge.Start);
   }

   public static byte[] lastRocksDBKey(byte[] logicalKey)
   {
      return makeVirtualRocksDBKey(logicalKey, RangeEdge.End);
   }

   private enum RangeEdge
   {
      Start((byte)1), End((byte)2);

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

   private static byte[] makeRocksDBKey(byte[] logicalKey, byte[] value)
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

   public static int compareRocksDBKeys(
           byte[] a,
           byte[] b,
           Comparator<byte[]> keyComparator,
           Comparator<byte[]> valueComparator)
   {
      /*
      keyComparator and valueComparator are
      possibly efficient i.e. they could be calling directly memcmp()
      what can we do in order to ensure the comparison of the
      complete rocks db key is also efficient?

      copying the parts into separate byte arrays is not efficient.
      we need to perform the comparison in place
       */
      byte[] keyA = extractKey(a);
      byte[] keyB = extractKey(b);

      /*
      what is the first value for a given key
      the first
       */
      int keyComparison = keyComparator.compare(keyA, keyB);

      if (keyComparison != 0) return keyComparison;
      else
      {
         /*
         Handle the case when one of the keys is the 'first' or 'last'
         key

         TODO this is inefficient
          */
         if (isFirst(a))
         {
            if (isFirst(b))
               return 0;
            else
               return -1;

         }
         else if (isLast(a))
         {
            if (isLast(b))
               return 0;
            else
               return 1;
         }
         else if (isLast(b))
         {
               return -1;
         }
         else if (isFirst(b))
         {
            return 1;
         }
         return valueComparator.compare(extractValue(a), extractValue(b));
      }

   }

   private static final boolean isSystem(byte[] key)
   {
      return key[SYSTEM_FLAG_INDEX] != (byte)0;
   }
   private static final boolean isFirst(byte[] key)
   {
      return key[SYSTEM_FLAG_INDEX] == (byte)1;
   }
   private static final boolean isLast(byte[] key)
   {
      return key[SYSTEM_FLAG_INDEX] == (byte)2;
   }


   private static final int SYSTEM_FLAG_INDEX = 1;

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

      System.out.printf("first - last: %s %n", compareRocksDBKeys(first, last, Arrays::compare, Arrays::compare));
      System.out.printf("last - first: %s %n", compareRocksDBKeys(last, first, Arrays::compare, Arrays::compare));
      System.out.printf(" kv1 - first: %s %n", compareRocksDBKeys(kv1, first, Arrays::compare, Arrays::compare));
      System.out.printf(" kv1 - last : %s %n", compareRocksDBKeys(kv1, last, Arrays::compare, Arrays::compare));
      System.out.printf(" last - kv1 : %s %n", compareRocksDBKeys(last, kv1, Arrays::compare, Arrays::compare));
      System.out.printf(" first - kv1 : %s %n", compareRocksDBKeys(first, kv1, Arrays::compare, Arrays::compare));
      System.out.printf(" kv2 - kv1 : %s %n", compareRocksDBKeys(kv2, kv1, Arrays::compare, Arrays::compare));
      System.out.printf(" kv1 - kv2 : %s %n", compareRocksDBKeys(kv1, kv2, Arrays::compare, Arrays::compare));
      System.out.printf(" kv1 - kv3 : %s %n", compareRocksDBKeys(kv1, kv3, Arrays::compare, Arrays::compare));
      System.out.printf(" kv1 - kv4 : %s %n", compareRocksDBKeys(kv1, kv4, Arrays::compare, Arrays::compare));
      System.out.printf(" kv2 - kv4 : %s %n", compareRocksDBKeys(kv2, kv4, Arrays::compare, Arrays::compare));
      System.out.printf(" kv2 - kv3 : %s %n", compareRocksDBKeys(kv2, kv3, Arrays::compare, Arrays::compare));

   }



   private static byte[] asBytes(UUID uuid) {
      ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());
      return bb.array();
   }


}
