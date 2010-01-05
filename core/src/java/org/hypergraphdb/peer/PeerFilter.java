/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Cipri Costa
 *
 * Implementors can filter peers that match  a give description. The result of the filter can be 
 * used as a target in any subclass of the <code>PeerRelatedActivity</code> class.
 */
public abstract class PeerFilter 
{
	protected PeerFilterEvaluator evaluator;
	
	private List<Object> targets = new ArrayList<Object>();

	public PeerFilter()
	{
	}
		
	public abstract void filterTargets();

	
	public Iterator<Object> iterator()
	{
		return targets.iterator();
	}

	protected void matchFound(Object target)
	{
		targets.add(target);
	}

	public PeerFilterEvaluator getEvaluator()
	{
		return evaluator;
	}

	public void setEvaluator(PeerFilterEvaluator evaluator)
	{
		this.evaluator = evaluator;
	}

}
