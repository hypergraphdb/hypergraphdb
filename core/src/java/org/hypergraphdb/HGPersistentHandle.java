/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

import java.io.Serializable;

/**
 * <p>
 * A <code>HGPersistentHandle</code> is a <code>HGHandle</code> that survives system
 * downtime. That is, a permanent handle will be valid between startup and shutdown of a
 * HyperGraph based application. 
 * </p>
 * 
 * <p>
 * A concrete implementation is guaranteed to be a compact, serialiazable Java object that
 * can be stored by the application using some other means and reused to refer to the same 
 * atom in subsequent runs of the same HyperGraph instance, or within a distributed environment.
 * </p>
 *
 * <p>
 * Generally, plain <code>HGHandle</code> implementation are designed for fast, in-memory 
 * access whereas <code>HGPersistentHandle</code> are designed as a persistent, long-term reference.
 * </p>
 * 
 * @see HGHandleFactory
 * 
 * @author Borislav Iordanov
 */
public interface HGPersistentHandle extends HGHandle, Serializable, Comparable<HGPersistentHandle>
{
    /**
     * <p>Return a <code>byte[]</code> representation of the handle. Note that <code>byte[]</code>
     * representations of all handles will be of the same size. For example 4 for integer based handles,
     * or 16 for UUID based handles.
     */
    byte [] toByteArray();
}