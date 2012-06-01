/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.cond2qry;


import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.PositionedIncidentCondition;
import org.hypergraphdb.query.impl.PredicateBasedRAFilter;

public class PositionedIncidentToQuery implements ConditionToQuery
{
	public QueryMetaData getMetaData(HyperGraph graph, HGQueryCondition c)
	{
		QueryMetaData x = QueryMetaData.ORACCESS.clone(c);
		x.predicateCost = 1;
		PositionedIncidentCondition cond = (PositionedIncidentCondition) c;		
		x.sizeLB = 0;
		x.sizeUB = hg.isVar(cond.getTargetRef()) ? Integer.MAX_VALUE :
					graph.getIncidenceSet(cond.getTargetRef().get()).size();
		x.sizeExpected = (x.sizeUB - x.sizeLB) / 2;
		return x;
	}

	public HGQuery<?> getQuery(final HyperGraph hg, HGQueryCondition c)
	{
		final PositionedIncidentCondition pic = (PositionedIncidentCondition) c;
		HGQuery<HGHandle> qry = new HGQuery<HGHandle>()
		{
			public HGSearchResult<HGHandle> execute()
			{
				return hg.getIncidenceSet(pic.getTargetRef().get()).getSearchResult();
			}
		};
		return new PredicateBasedRAFilter(hg, qry, pic);
	}
}