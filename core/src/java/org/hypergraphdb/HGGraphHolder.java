/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

import org.hypergraphdb.annotation.HGIgnore;

/**
 * 
 * <p>
 * The interface is for atoms that need to hold a reference to the
 * <code>HyperGraph</code> to which they belong. If an object implements
 * this interface, its <code>setHyperGraph</code> method will be called
 * every time it is read from permanent storage, and also the first it is
 * added to permanent storage.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public interface HGGraphHolder
{
	/**
	 * <p>During load time, set the <code>HyperGraph</code> 
	 * instance to which this atom belongs.</p>
	 * @param hg The <code>HyperGraph</code> that just loaded
	 * the atom.
	 */
	@HGIgnore
	void setHyperGraph(HyperGraph graph);
}
