/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb;

/**
 * A logical DB backed by a specific column family
 */
public class ColumnFamilyLogicalDB
{
   /*
   TODO ensure that hardcoding this is ok.
    */
   public static final int LOGICAL_KEY_SIZE = 16;

   /*
   If we are storing multiple values for the same key, this is the
    */
   public static final int VALUE_KEY_SIZE = 16;

   private final boolean multivalued;

   private final StorageImplementationRocksDB.ColumnFamily cf;

   public ColumnFamilyLogicalDB(StorageImplementationRocksDB.ColumnFamily cf, boolean multivalued)
   {
      this.multivalued = multivalued;
      this.cf = cf;
   }

   public byte[] makeRocksDBKey(byte[] logicalKey, byte[] value)
   {
      if (this.multivalued)
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
      else
      {
         /*
         TODO maybe return a copy?
          */
         return logicalKey;
      }

   }
}
