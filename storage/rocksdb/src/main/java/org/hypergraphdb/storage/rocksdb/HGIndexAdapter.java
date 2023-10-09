/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb;

import org.rocksdb.*;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class HGIndexAdapter
{
    private final String name;
    boolean configured = false;
    private Comparator<byte[]> keyComparator;
    private Comparator<byte[]> valueComparator;

    public HGIndexAdapter(String name)
    {
        this.name = name;
    }

    public void configure(Comparator<byte[]> keyComparator, Comparator<byte[]> valueComparator)
    {
        if (this.configured)
        {
            throw new IllegalStateException("The adapter is already configured.");
        }
        this.keyComparator = keyComparator;
        this.valueComparator = valueComparator;
        this.configured = true;
    }

    public AbstractComparator getComparator()
    {
        /*
        TODO comparator.close? comparatoroptions.close?
         */
        return new AbstractComparator(new ComparatorOptions())
        {
            @Override
            public String name()
            {
                return name;
            }

            @Override
            public int compare(ByteBuffer buffer1, ByteBuffer buffer2)
            {
                if (!configured)
                {
                    throw new IllegalStateException("The adapter is not yet configured and cannot" +
                            "be called");
                }
                return VarKeyVarValueColumnFamilyMultivaluedDB.compareRocksDBKeys(
                        buffer1,
                        buffer2,
                        keyComparator,
                        valueComparator);
            }
        };
    }

}
