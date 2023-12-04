/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2023 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb.dataformat;

import org.hypergraphdb.storage.rocksdb.IteratorResultSet;

/**
 * TODO
 *  consider creating a common API for the different logical databases
 *  i.e. multivalued  db / fixed key size / var key size etc.
 */
public interface LogicalDB
{
    void add(byte[] key, byte[] value);
    void remove(byte[] key, byte[] value);
    void remove(byte[] key);
}
