/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2024 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb;

import org.hypergraphdb.util.Pair;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A registry for the column families in the RocksDB database.
 * This is needed because we need to initialize the column families when the
 * database is first started.
 * At that point there may be column families which are not yet requested by the
 * user (e.g. the index lifecycle allows indices to be requested at some later
 * point)
 * So, the column families are first put in this registry and then when they are
 * required, they can be retrieved from here.
 * Another function of this registry is to allow us to keep track of the 'orphaned'
 * column families which have not yet been requested by anyone.
 * Thus, such column families (and more importantly their column family options)
 * can be closed when the database is closed.
 */
public class ColumnFamilyRegistry implements AutoCloseable
{
    /*
    Registries for the resources related to the indices.
    Conceptually, the CF is a dependency of an index however its lifecycle does not allow the index to
    cleanly manage it. -- the CF can be created (in the case of preexisting index) before the index.
    So, the CF are stored in a registry in the index manager and are retrieved from it when an index requests
    them.
     */
    private final ConcurrentHashMap<String, Pair<ColumnFamilyHandle, ColumnFamilyOptions>> store = new ConcurrentHashMap<>();

    public void registerColumnFamily(String cfName, ColumnFamilyHandle handle, ColumnFamilyOptions columnFamilyOptions)
    {
        this.store.put(cfName, new Pair<>(handle, columnFamilyOptions ));
    }

    public ColumnFamilyHandle handle(String cfName)
    {
        return this.store.get(cfName).getFirst();
    }

    public ColumnFamilyOptions options(String cfName)
    {
        return this.store.get(cfName).getSecond();
    }

    public Pair<ColumnFamilyHandle, ColumnFamilyOptions> remove(String cfName)
    {
        return this.store.remove(cfName);
    }

    @Override
    public void close()
    {
        for (var o : store.values())
        {
            o.getSecond().close();
        }
        this.store.clear();
    }

    public boolean containsKey(String cfName)
    {
        return this.store.containsKey(cfName);
    }
}
