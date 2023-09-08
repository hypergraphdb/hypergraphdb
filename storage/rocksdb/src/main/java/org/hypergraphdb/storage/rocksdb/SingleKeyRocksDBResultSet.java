/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.util.function.Function;

/**
 *
 * A result set which represents all the values for a specific key
 * @param <T> the type of the values within the result set
 */
public class SingleKeyRocksDBResultSet<T> extends IndexResultSet<T>
{
    public SingleKeyRocksDBResultSet(
            RocksDB rocksDB,
            LogicalDatabase logicalDB,
            RocksIterator iterator,
            byte[] logicalKey,
            Function<T, byte[]> toByteConverter,
            Function<byte[], T> fromByteConverter)
    {
        super(rocksDB, logicalDB, iterator, logicalKey, toByteConverter, fromByteConverter);

        if (!logicalDB.isMultivalued())
        {
            /*
            The single key result set is expected to iterate over the multiple
            values of a single key
             */
            throw new RuntimeException("The supplied logical database is not multivalued");
        }
    }

    protected T advance()
    {
        return this.next();
    }

    protected  T back()
    {
        return this.prev();
    }


    public boolean isOrdered()
    {
        return true;
    }




}
