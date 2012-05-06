/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.handle;

import org.hypergraphdb.HGHandle;

/**
 * <p>
 * A <code>LiveHandle</code> represents an in memory handle to a HyperGraph atom.
 * It holds references to both the runtime object instance of the atom and its
 * persistent handle.
 * </p>
 * 
 * <p>
 * An application should never rely on a concrete <code>HGLiveHandle</code> implementation
 * since it will depend on the exact caching/memory management policy adopted. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public interface HGLiveHandle extends HGHandle
{
	byte getFlags(); 
	Object getRef();
}
