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
 *
 * @param <BufferType>
 * @param <T>
 */
public class SingleKeyResultSet<BufferType, T> extends IndexResultSet<BufferType, T>
{
    protected T advance()
    {
        throw new RuntimeException("not implemented");
    }

    protected  T back()
    {
        throw new RuntimeException("not implemented");
    }


    public boolean isOrdered()
    {
        return true;
    }




}
