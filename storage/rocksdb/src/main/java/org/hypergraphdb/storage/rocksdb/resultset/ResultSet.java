/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2024 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb.resultset;

import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.util.CountMe;

public abstract class ResultSet<T> implements HGRandomAccessResult<T>, CountMe
{
}
