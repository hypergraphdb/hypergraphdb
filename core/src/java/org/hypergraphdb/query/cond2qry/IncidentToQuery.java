package org.hypergraphdb.query.cond2qry;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.IncidentCondition;

public class IncidentToQuery implements ConditionToQuery
{

	public QueryMetaData getMetaData(HyperGraph graph, HGQueryCondition c)
	{
		QueryMetaData x = QueryMetaData.ORACCESS.clone(c);
		x.predicateCost = 1;
		final HGPersistentHandle handle = graph.getPersistentHandle(((IncidentCondition)c).getTarget());		
		x.sizeLB = x.sizeExpected = x.sizeUB = graph.getIncidenceSet(handle).size();
		return x;
	}

	public HGQuery<?> getQuery(HyperGraph graph, HGQueryCondition c)
	{
		final HGPersistentHandle handle = graph.getPersistentHandle(((IncidentCondition)c).getTarget());
		return new HGQuery<HGHandle>()
		{
			public HGSearchResult<HGHandle> execute()
			{
				return graph.getIncidenceSet(handle).getSearchResult();
			}
		};
	}
}