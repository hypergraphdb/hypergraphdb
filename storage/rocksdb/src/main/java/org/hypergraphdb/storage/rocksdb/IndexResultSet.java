/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb;

import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.util.CountMe;

/**
 *
 * @param <T> TODO
 */
public class IndexResultSet <T> implements HGRandomAccessResult<T>,
        CountMe
{
    @Override
    public GotoResult goTo(T value, boolean exactMatch)
    {
        return null;
    }

    @Override
    public void goAfterLast()
    {

    }

    @Override
    public void goBeforeFirst()
    {

    }

    @Override
    public T current()
    {
        return null;
    }

    @Override
    public void close()
    {

    }

    @Override
    public boolean isOrdered()
    {
        return false;
    }

    @Override
    public boolean hasPrev()
    {
        return false;
    }

    @Override
    public T prev()
    {
        return null;
    }

    @Override
    public boolean hasNext()
    {
        return false;
    }

    @Override
    public T next()
    {
        return null;
    }

    @Override
    public int count()
    {
        return 0;
    }
}
