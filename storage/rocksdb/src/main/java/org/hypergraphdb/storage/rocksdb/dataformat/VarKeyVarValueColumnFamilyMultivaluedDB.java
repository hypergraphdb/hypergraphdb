/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb.dataformat;


import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.UUID;

/**
 * Utilities for the data format for a logical database which supports variable
 * sized keys, variable sized values and multiple values per key.
 * <br/>
 * The data format is as follows:
 * <br/>
 * All the records are in a single RocksDB (RDB) column family (CF).
 * The column family does not contain data other than the records in th
 * logical database.
 * All the information in this column family is stored in the record's
 * key. (the value of the record is undefined)
 * <br/>
 * The format of the key is a byte array -- key[]
 * key[0] -- KEY_LENGTH -- 8 bit unsigned int which represents the length of the logical
 * key.
 * key[1] -- system flag with the following semantics:
 * Only the keys with this flag set to 0 are present in the CF. All other
 * cases represent range edge keys which are used to define
 *    0 -- a regular 'data' key which can be stored in the CF. All the
 *       keys which are actually stored in the column family have this
 *       set to 0
 *    1 -- START a RDB key which is LT every key with the same value for
 *       the logical key
 *    2 -- END a RDB key which is GT every key with the same value for
 *       the logical key
 *    3 -- GLOBALLY_FIRST a RDB key which is LT every key in the CF
 *    4 -- GLOBALLY_LAST a RDB key which is GT every key in the CF
 *
 * TODO why not just use the first value for the given key? is it defined?
 *    We will need the actual comparator to supply the first value
 * TODO describe why.
 * key[2, 2+KEY_LENGTH] -- the logical key
 *    KEY_LENGTH <= 255 . the logical key has a max size of 255 bytes
 * TODO make the KEY_LENGTH at least short
 * key[2+KEY_LENGTH, ...] -- the logical value
 */
public class VarKeyVarValueColumnFamilyMultivaluedDB
{

   /*
   This is a utility class (at least for now), so block the constructor
	*/
   private VarKeyVarValueColumnFamilyMultivaluedDB(){};


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
	  return rangeEdgeRocksDBKey(logicalKey, RangeEdge.START);
   }

   public static byte[] lastRocksDBKey(byte[] logicalKey)
   {
	  return rangeEdgeRocksDBKey(logicalKey, RangeEdge.END);
   }
   public static byte[] globallyFirstRocksDBKey()
   {
	  return rangeEdgeRocksDBKey(new byte[0], RangeEdge.GLOBAL_END);
   }
   public static byte[] globallyLastRocksDBKey()
   {
	  return rangeEdgeRocksDBKey(new byte[0], RangeEdge.GLOBAL_START);
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

   private static final int FLAG_SIZE = 1;
   private static final int KEY_LENGTH_SIZE = 4;
   /**
	* Construct a range edge RocksDB key. They are not inserted
	* in the column family, but are rather used to define ranges.
	* The semantics of the constructed key is documented in the {@link RangeEdge}
	* enum
	* @param logicalKey
	* @param edge
	* @return
	*/
   private static byte[] rangeEdgeRocksDBKey(byte[] logicalKey, RangeEdge edge)
   {
	  byte[] result = new byte[logicalKey.length + FLAG_SIZE + KEY_LENGTH_SIZE];
	  var lengthBytes = ByteBuffer.wrap(result,0, KEY_LENGTH_SIZE);
	  lengthBytes.putInt(logicalKey.length);
	  result[KEY_LENGTH_SIZE] = edge.flag;
	  System.arraycopy(logicalKey, 0, result, KEY_LENGTH_SIZE + 1, logicalKey.length);

	  return result;

   }

   public static byte[] makeRocksDBKey(byte[] logicalKey, byte[] value)
   {
	  byte[] result = new byte[logicalKey.length + value.length + FLAG_SIZE + KEY_LENGTH_SIZE];
	  var lengthBytes = ByteBuffer.wrap(result,0, KEY_LENGTH_SIZE);
	  lengthBytes.putInt(logicalKey.length);
	  System.arraycopy(logicalKey, 0, result, KEY_LENGTH_SIZE + FLAG_SIZE, logicalKey.length);
	  System.arraycopy(value, 0, result, KEY_LENGTH_SIZE + FLAG_SIZE + logicalKey.length, value.length);

	  return result;
   }

   /**
	* extract the part of the rocksdb key which represents the logical key
	* @param keyvalue
	* @return
	*/
   public static byte[] extractKey(byte[] keyvalue)
   {
	  var lengthBytes = ByteBuffer.wrap(keyvalue,0, KEY_LENGTH_SIZE);
	  var keySize = lengthBytes.getInt();

	  byte[] res = new byte[keySize];
	  System.arraycopy(keyvalue, KEY_LENGTH_SIZE + FLAG_SIZE, res, 0, keySize);

	  return res;
   }

   public static byte[] extractValue(byte[] keyvalue)
   {
	  var lengthBytes = ByteBuffer.wrap(keyvalue,0, KEY_LENGTH_SIZE);
	  var keySize = lengthBytes.getInt();

	  int valueSize = keyvalue.length - keySize - KEY_LENGTH_SIZE - FLAG_SIZE;
	  byte[] res = new byte[valueSize];
	  System.arraycopy(keyvalue, keySize + KEY_LENGTH_SIZE + FLAG_SIZE, res, 0, valueSize);

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

	  byte[] keyLengthBytes1 = new byte[KEY_LENGTH_SIZE];
	  byte[] keyLengthBytes2 = new byte[KEY_LENGTH_SIZE];
	  buffer1.get(keyLengthBytes1);
	  buffer2.get(keyLengthBytes2);
	  int keysize1 = ByteBuffer.wrap(keyLengthBytes1).getInt();
	  int keysize2 = ByteBuffer.wrap(keyLengthBytes2).getInt();

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


   private static boolean isGloballyFirst(byte systemFlag)
   {
	  return systemFlag == RangeEdge.GLOBAL_START.flag;
   }

   public static boolean isGloballyLast(byte systemFlag)
   {
	  return systemFlag == RangeEdge.GLOBAL_END.flag;
   }

   private static boolean isSystem(byte systemFlag)
   {
	  return systemFlag != 0b0000_0000;
   }
   private static boolean isFirst(byte systemFlag)
   {
	  return systemFlag == RangeEdge.START.flag;
   }
   private static boolean isLast(byte systemFlag)
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
