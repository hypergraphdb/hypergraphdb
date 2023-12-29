/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb.index;

import org.hypergraphdb.storage.rocksdb.dataformat.VarKeyVarValueColumnFamilyMultivaluedDB;
import org.rocksdb.*;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * The adapter is conceptually a reference to the comparators which the
 * RocksDB engine uses to sort the keys in the column family
 * The index adapter provides a level of indirection between the comparator
 * which RocksDB uses to order the records in the index's column family and the logical
 * key/value comparators which are specified by the user when an index is
 * initialized.
 *
 * This is needed, because RocksDB needs the comparators in order
 * to start up.
 * So we provide the RocksDB runtime with the scaffolding of the comparator
 * i.e. HGIndexAdapter.getRocksDBComparator() which initially would throw an exception
 * if it were actually used because the keyComparator and valueComparator
 * are not yet specified. This is safe, because the column family is not
 * actually used before the index is initialized.
 *
 * When the user initializes and index, they provide the key and value comparator
 * so that they can be used to configure the adapter and the actual RocksDB
 * comparator can be called
 *
 */
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

	public AbstractComparator getRocksDBComparator()
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
