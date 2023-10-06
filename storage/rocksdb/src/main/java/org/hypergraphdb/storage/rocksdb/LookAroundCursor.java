/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb;

public class LookAroundCursor<T>
{
    public static interface Cursor
    {
        boolean isValid();
        byte[] current();
        void prev();
        void next();
    }


    private Cursor cursor;

    public LookAroundCursor(Cursor cursor)
    {

        this.cursor = cursor;
    }

    public boolean hasNext()
    {

        return false;
    }

    public void goBeforeStart()
    {

    }

    public void goAfterEnd()
    {

    }

}
