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
 * A class which represents a logical database within a single RocksDB 'physical'
 * database. Conceptually this is an enum.
 *
 * TODO
 *  Describe why do we need this
 * Enum with the different key types in the database.
 * The database is logically split into several parts by having each key in
 * a certain part be prefixed by a certain KeyType.
 *
 * TODO
 *  we want to have the database keys to be properly lexicographically ordered
 *  We have a prefix which we prepend to each logical/raw key. If we have variable
 *  sized raw keys, we want to make sure that smaller keys are properly padded
 *  so that shorter keys come before longer keys
 *
 *
 * TODO
 *  this should be an abstraction over the physical rocksdb and a user should
 *  not need access to both in the same time
 *
 *
 */
public class LogicalDatabase
{
    /*
    What types of databases are possible:
    1. multivalued keys / single valued keys
    2. fixed value size / variable value size
    We cannot have multivalued keys and variable value size because when we
    want multiple values for the same key, we store the value in the key
    so we will need to use a fixed length serialization
     */
    private static final int PREFIX_LENGTH = 1;

    /*
    TODO ensure that hardcoding this is ok.
     */
    public static final int LOGICAL_KEY_SIZE = 16;
    /*
    If we are storing multiple values for the same key, this is the
     */
    public static final int VALUE_KEY_SIZE = 16;

    /**
     * The size of the physical rocks DB key which is to be stored in the
     * RocksDB database
     * @return the size in bytes of the actual RocksDB key
     */
    private static int rocksDBKeySize()
    {
        return PREFIX_LENGTH + LOGICAL_KEY_SIZE + VALUE_KEY_SIZE;
    }


    /**
     * The primitive logical database
     * The records are in the format:
     * {
     *     key: 2<atom-handle-bytes>
     *     value: <atom-serialization>
     * }
     */
    static LogicalDatabase primitive()
    {
        return new LogicalDatabase("primitive", (byte)0, false);
    }



    /**
     * The data logical database
     * The records are in the format:
     * {
     *     key: 1<link-handle-bytes>
     *     value:<target-handles-bytes>
     * }
     */
    static LogicalDatabase data()
    {
        return new LogicalDatabase("data", (byte)1,false);
    }


    /**
     * The incidence logical database
     * The records are in the format:
     * {
     *     key: 0<target-handle-bytes><link-handle-bytes>
     *     value: <no-bytes>
     * }
     */
    static LogicalDatabase incidence()
    {
        return new LogicalDatabase("incidence", (byte)2, true);
    }


    static LogicalDatabase index(String indexID)
    {
        throw new RuntimeException("not implemented");
    }


    private final byte prefix;
    private final String databaseId;
    private final boolean multivalued;

    private LogicalDatabase(String databaseId, byte prefix, boolean multivalued)
    {
        this.databaseId = databaseId;
        this.prefix = prefix;
        this.multivalued = multivalued;
    }

    public boolean isMultivalued()
    {
        return this.multivalued;
    }

    /**
     *
     * Get the first physical in a given logical database
     *
     * @return the first physical key which can be stored in this logical database
     * Note, that this is not the first key actually stored, but the first key
     * that could be stored.
     */
    public byte[] firstGlobalKeyInDB()
    {
        byte[] res = new byte[rocksDBKeySize()];
        res[0] = prefix;
        return res;
    }

    /**
     * The last physical key in a given logical database
     *
     * @return the last physical key which can be stored in this local database
     */
    public byte[] lastGlobalKeyInDB()
    {
        byte[] res = new byte[rocksDBKeySize()];

        res[0] = prefix;
        for (int i = 0; i < LOGICAL_KEY_SIZE + VALUE_KEY_SIZE; i++)
        {
            res[PREFIX_LENGTH + i] = (byte)0xff;
        }
        return res;
    }

    /**
     * The first possible global key for a given logical key in this
     * database
     * @param logicalKey the logical key whose range we are interested in
     * @return
     */
    public byte[] firstGlobalKeyForLogicalKey(byte[] logicalKey)
    {
        byte[] res = firstGlobalKeyInDB();
        System.arraycopy(logicalKey, 0, res, PREFIX_LENGTH, LOGICAL_KEY_SIZE);

        for (int i = 0; i < VALUE_KEY_SIZE; i++)
        {
            res[PREFIX_LENGTH + LOGICAL_KEY_SIZE + i] = (byte)0x00;
        }
        return res;
    }

    /**
     *
     * The first possible record for a given key
     * @param logicalKey
     * @return
     */
    public byte[] lastGlobalKeyForLogicalKey(byte[] logicalKey)
    {
        byte[] res = firstGlobalKeyInDB();
        System.arraycopy(logicalKey, 0, res, PREFIX_LENGTH, LOGICAL_KEY_SIZE);

        for (int i = 0; i < VALUE_KEY_SIZE; i++)
        {
            res[PREFIX_LENGTH + LOGICAL_KEY_SIZE + i] = (byte)0xff;
        }
        return res;
    }

    /**
     * Convert a local key to a global key in this database
     * @param logicalKey the local key, unique to this database
     * @param value the value which, if supplied, will be used to construct the
     *              global key for a key-value combination in a multivalued
     *              logical database
     * @return the global key corresponding to the given local key,
     *  which is the local key, prefixed with this database id
     */
    byte[] scopeKey(byte[] logicalKey, byte[] value)
    {
        if (this.multivalued && value == null)
        {
            throw new RuntimeException(
                    "Multivalued databases need both the logical key and the value " +
                            "to create the physical key.");
        }
        if (!this.multivalued && value != null)
        {
            /*
            fail fast in case we received a set value to make sure there
            is no erroneous calls to the method instead of silently ignoring
            the value
             */
            throw new RuntimeException(
                    String.format(
                    "Single valued database received an unexpected value %s", value));
        }
        byte[] res = new byte[rocksDBKeySize()];
        res[0] = prefix;
        System.arraycopy(logicalKey, 0, res, PREFIX_LENGTH, LOGICAL_KEY_SIZE);
        if (this.multivalued)
        {
            /*
            if the database allows multivalued keys, the value is part of the key
             */
            System.arraycopy(value, 0, res, PREFIX_LENGTH + LOGICAL_KEY_SIZE, VALUE_KEY_SIZE);
        }
        return res;
    }

/*
TODO
    this should be an abstraction above the physical database
 */

//TODO
//    public byte[] get(byte[] key)
//    {
//        /*
//        MV
//         */
//
//    }
//
//TODO
//    public byte[][] getAll(byte[] key)
//    {
//
//     }
//TODO
//    public byte[] add(byte[] key, byte[] value)
//    {
//
//    }
//
//TODO
//    public void remove(byte[] key)
//    {
//
//    }

}
