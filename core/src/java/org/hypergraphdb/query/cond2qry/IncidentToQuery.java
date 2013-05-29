/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.cond2qry;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.IncidentCondition;

public class IncidentToQuery implements ConditionToQuery<HGHandle>
{
	public QueryMetaData getMetaData(HyperGraph graph, HGQueryCondition c)
	{
		QueryMetaData x = QueryMetaData.ORACCESS.clone(c);
		x.predicateCost = 1;
		IncidentCondition ic = (IncidentCondition)c;
		if (hg.isVar(ic.getTargetRef()))
		{
			x.sizeExpected = 1000; // incidence sets are usually small...
		}
		else
		{
			final HGPersistentHandle handle = graph.getPersistentHandle(((IncidentCondition)c).getTarget());		
			x.sizeLB = x.sizeExpected = x.sizeUB = graph.getIncidenceSet(handle).size();
		}
		return x;
	}

	public HGQuery<HGHandle> getQuery(HyperGraph graph, HGQueryCondition c)
	{
		final IncidentCondition ic = (IncidentCondition)c;
//		final HGPersistentHandle handle = graph.getPersistentHandle(((IncidentCondition)c).getTarget());
		return new HGQuery<HGHandle>()
		{
			public HGSearchResult<HGHandle> execute()
			{
				return graph.getIncidenceSet(ic.getTarget()).getSearchResult();
			}
		};
	}
}