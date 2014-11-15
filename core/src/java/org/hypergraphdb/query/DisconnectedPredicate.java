/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;

/**
 * 
 * <p>
 * A predicate that returns <code>true</code> if the incidence set of a given atom
 * is empty and <code>false</code> otherwise.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class DisconnectedPredicate implements HGAtomPredicate, HGQueryCondition
{
    public boolean satisfies(HyperGraph graph, HGHandle handle)
    {
        return graph.getIncidenceSet(handle).isEmpty();
    }

    public int hashCode()
    {
    	return 0;
    }
    
    public boolean equals(Object x)
    {
    	return x != null && x.getClass().equals(this.getClass());
    }
}
