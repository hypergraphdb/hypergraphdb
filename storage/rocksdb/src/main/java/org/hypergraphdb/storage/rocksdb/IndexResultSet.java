/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb;

import org.rocksdb.RocksIterator;

import java.util.function.Function;

public class IndexResultSet<Value> //extends IteratorResultSet<Value>
{
    /**
     * The result set is associated with a transaction.
     * TODO what happens with the iterator when the transaction is committed/
     *  rolled back?
     *
     * @param iterator
     *         The iterator which backs the result set. All the values in the
     *         iterator are the serializations of the  values in the result set.
     */
//    public IndexResultSet(RocksIterator iterator,
//            Function<Value, byte[]> toByteConverter,
//            Function<byte[], Value> fromByteConverter)
//    {
////        super(iterator, toByteConverter, fromByteConverter);
//    }

//    @Override
    public byte[] getRocksDBKey(Value value)
    {
        return null;
//        return VarKeyVarValueColumnFamilyMultivaluedDB.makeRocksDBKey(this.toByteConverter.apply(value);
    }
}
