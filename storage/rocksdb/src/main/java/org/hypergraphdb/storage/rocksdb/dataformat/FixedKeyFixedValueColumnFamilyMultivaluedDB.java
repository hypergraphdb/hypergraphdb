/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb.dataformat;

import java.util.Arrays;

/**
 * A logical DB backed by a specific column family
 * The logical DB supports multiple values per key
 * Both the keys and values are fixed in size
 */
public class FixedKeyFixedValueColumnFamilyMultivaluedDB
{
   /*
   TODO ensure that hardcoding this is ok.
    */
   public static final int LOGICAL_KEY_SIZE = 16;

   /*
   If we are storing multiple values for the same key, this is the
    */
   public static final int VALUE_KEY_SIZE = 16;

//   private final boolean multivalued;

//   private final StorageImplementationRocksDB.ColumnFamily cf;
//
//   public ColumnFamilyLogicalDB(StorageImplementationRocksDB.ColumnFamily cf, boolean multivalued)
//   {
//      this.multivalued = multivalued;
//      this.cf = cf;
//   }
   private static byte[] FIRST_VALUE = new byte[VALUE_KEY_SIZE];
   private static byte[] LAST_VALUE = new byte[VALUE_KEY_SIZE];
   static
   {
      Arrays.fill(LAST_VALUE, (byte)0x00); //this is the default but better be explicit
      Arrays.fill(LAST_VALUE, (byte)0xff);
   }



   /**
    * The rocks DB key of the first possible value for a given logical key
    *
    * @param logicalKey
    * @return
    */
   public static byte[] firstRocksDBKey(byte[] logicalKey)
   {
      return makeRocksDBKey(logicalKey, FIRST_VALUE);
   }

   /**
    * The rocks DB key of the last possible value for a given logical key
    *
    * @param logicalKey
    * @return
    */
   public static byte[] lastRocksDBKey(byte[] logicalKey)
   {
      return makeRocksDBKey(logicalKey, LAST_VALUE);
   }

   public static byte[] makeRocksDBKey(byte[] logicalKey, byte[] value)
   {
      if (logicalKey.length != LOGICAL_KEY_SIZE && value.length != VALUE_KEY_SIZE)
      {
         throw new RuntimeException("variable sized values not yet supported in" +
                 "multivalued databases.");
      }
      byte[] res = new byte[LOGICAL_KEY_SIZE + value.length];
      System.arraycopy(logicalKey, 0, res, 0, LOGICAL_KEY_SIZE);
      /*
      if the database allows multivalued keys, the value is part of the key
       */
      System.arraycopy(value, 0, res, LOGICAL_KEY_SIZE, VALUE_KEY_SIZE);
      return res;

   }

   /**
    * extract the
    * @param keyvalue
    * @return
    */
   public static byte[] extractKey(byte[] keyvalue)
   {
      byte[] res = new byte[LOGICAL_KEY_SIZE];
      System.arraycopy(keyvalue, 0, res, 0, LOGICAL_KEY_SIZE);

      return res;
   }

   public static byte[] extractValue(byte[] keyvalue)
   {
      byte[] res = new byte[VALUE_KEY_SIZE];
      System.arraycopy(keyvalue, LOGICAL_KEY_SIZE, res, 0, VALUE_KEY_SIZE);

      return res;
   }
}
