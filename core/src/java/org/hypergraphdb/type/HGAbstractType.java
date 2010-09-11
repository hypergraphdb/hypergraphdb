/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGException;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;

/**
 * <p>
 * The implementation of a HyperGraph abstract types. Abstract types serve the purpose
 * of <i>semantic tagging</i>. The main motivation is to model interfaces and abstract classes
 * in Java and other OO languages. From a general perspective, they serve the purpose of 
 * categorizing entities with no concrete realization except in a sub-typing relation with 
 * other types. 
 * </p> 
 * 
 * <p>
 * A possible future application of abstract types in HyperGraph would be to actually
 * create "abstract values" out of them. Those would be semi specified/undefined atoms, with
 * values yet to be refined. Such atoms could be provisionally used for linkage with other atoms 
 * until further (if ever) concretized. 
 * </p>
 * 
 * <p>
 * It is recommended that all HyperGraph "abstract types" be derived from this class.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class HGAbstractType implements HGAtomType 
{
	public void setHyperGraph(HyperGraph hg) 
	{
	}

	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet) 
	{
		throw new HGException("Cannot create a run-time instance of a HGAbstractType.");
	}

	public HGPersistentHandle store(Object instance) 
	{
		throw new HGException("Cannot store and instance of a HGAbstractType in the database.");		
	}

	public void release(HGPersistentHandle handle) 
	{
		throw new HGException("Cannot release an instance of a HGAbstractType.");		
	}

	public boolean subsumes(Object general, Object specific) 
	{
		//
		// Not much thought has been given into this....
		//
		return general.getClass().isAssignableFrom(specific.getClass());
	}
}
