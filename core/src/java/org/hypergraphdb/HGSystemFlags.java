/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

/**
 * <p>
 * This interface defines a set of system-level atom flags that can be specified at
 * atom addition time in order to control things like atom lifetime. The constants listed
 * here are used as bit flags and can be combined in the usual way by performing a bitwise 
 * <code>or</code> operation on the constant values. 
 * </p> 
 */
public interface HGSystemFlags 
{
	/**
	 * <p>
	 * Represents the default set of flags pertaining to the system-level handling
	 * of a HyperGraph atom.
	 * </p>
	 */
	public static final byte DEFAULT = 0x0;
	
	/**
	 * <p>
	 * Indicates that an atom's value may change during run-time operation. When an atom
	 * is marked as mutable, it will be automatically saved to storage before it is
	 * removed from the HyperGraph cache.
	 * </p>
	 * 
	 * <p>
	 * The default value for this system attribute is <code>false</code>. That is, atom run-time
	 * instances are assumed immutable and applications need to explicitly invoke the
	 * <code>HyperGraph.replace</code> method if need be.
	 * </p>
	 */
	public static final byte MUTABLE = 0x01;
	
	/**
	 * <p>
	 * Indicates that an atom is managed. Managed atom have their lifetime in HyperGraph
	 * be automatically controlled by the system. When an atom is managed, its usage is 
	 * tracked by the system and the atom is removed if it is deemed unused. For the exact
	 * set of conditions that determine when an atom is unused, please consult the reference
	 * guide. Managed atoms are designed to support long term persistence of temporary data,
	 * such as intermediary query results automatically created by the system and certain
	 * application specific atoms. 
	 * </p>
	 * 
	 * <p>
	 * The default value for this system attribute is <code>false</code>. That is, an atom
	 * will by default persist in the database until explicitly removed with the 
	 * <code>HyperGraph.remove</code> method.
	 * </p>
	 */
	public static final byte MANAGED = 0x02;	
}
