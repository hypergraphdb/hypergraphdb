/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2024 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;

import java.util.concurrent.ConcurrentHashMap;


public class ColumnFamilyRegistry implements AutoCloseable
{
    /*
    Registries for the resources related to the indices.
    Conceptually, the CF is a dependency of an index however its lifecycle does not allow the index to
    cleanly manage it. -- the CF can be created (in the case of preexisting index) before the index.
    So, the CF are stored in a registry in the index manager and are retrieved from it when an index requests
    them.
     */
    private final ConcurrentHashMap<String, Tuple.Pair<ColumnFamilyHandle, ColumnFamilyOptions>> store = new ConcurrentHashMap<>();

    public void registerColumnFamily(String cfName, ColumnFamilyHandle handle, ColumnFamilyOptions columnFamilyOptions)
    {
        this.store.put(cfName, Tuple.pair(handle, columnFamilyOptions ));
    }

    public ColumnFamilyHandle handle(String cfName)
    {
        return this.store.get(cfName).a;
    }

    public ColumnFamilyOptions options(String cfName)
    {
        return this.store.get(cfName).b;
    }

    public Tuple.Pair<ColumnFamilyHandle, ColumnFamilyOptions> remove(String cfName)
    {
        return this.store.remove(cfName);
    }

    @Override
    public void close()
    {
        for (var o : store.values())
        {
            o.b.close();
        }
        this.store.clear();
    }

    public boolean containsKey(String cfName)
    {
        return this.store.containsKey(cfName);
    }
}
