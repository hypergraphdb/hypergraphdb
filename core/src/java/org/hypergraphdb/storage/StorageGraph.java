/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage;

import java.util.Iterator;
import java.util.Set;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.util.Pair;

/**
 * <p>
 * Represents a storage layout graph. Implementations of this interface
 * could be RAM based, disk based or a mix of both. A storage layout graph
 * is represented roughly as the main <code>HGStore</code>  - a 
 * <code>HGPersistentHandle</code> keyed map whose values are either 
 * <code>HGPersistentHandle</code> arrays or byte buffers.
 * </p>
 * <p>
 * In addition, a <code>StorageGraph</code> has a set of designated roots, or
 * starting points to the graph. The intent
 * is for <code>StorageGraph</code> instances to represent a portion of the
 * full disk storage for a given atom or a set of atoms, for example for 
 * purposes of network communication.  
 * </p> 
 * @author Borislav Iordanov
 */
public interface StorageGraph extends Iterable<Pair<HGPersistentHandle, Object>>
{
    /**
     * <p>Return the set of root handles for this storage sub-graph.</p>
     */
    Set<HGPersistentHandle> getRoots();
    HGPersistentHandle [] getLink(HGPersistentHandle handle);
    HGPersistentHandle store(HGPersistentHandle handle, HGPersistentHandle [] link);
    byte [] getData(HGPersistentHandle handle);
    HGPersistentHandle store(HGPersistentHandle handle, byte [] data);
    Iterator<Pair<HGPersistentHandle, Object>> iterator();
}
