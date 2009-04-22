package org.hypergraphdb.query.cond2qry;

import java.util.ArrayList;
import java.util.Iterator;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.IncidentCondition;
import org.hypergraphdb.query.LinkCondition;
import org.hypergraphdb.query.impl.IntersectionQuery;
import org.hypergraphdb.query.impl.SortedIntersectionResult;
import org.hypergraphdb.query.impl.ZigZagIntersectionResult;

@SuppressWarnings("unchecked")
public class LinkToQuery implements ConditionToQuery
{

	public QueryMetaData getMetaData(HyperGraph graph, HGQueryCondition c)
	{
		QueryMetaData qmd;
		if (((LinkCondition)c).targets().size() == 0)
		{
			qmd = QueryMetaData.EMPTY.clone(c);
		}
		else
		{
			qmd = QueryMetaData.ORDERED.clone(c);
		}
		qmd.predicateCost = 0.5;
		return qmd;
	}

	public HGQuery<?> getQuery(HyperGraph graph, HGQueryCondition c)
	{
		LinkCondition lc = (LinkCondition)c;
		ArrayList<HGQuery> L = new ArrayList<HGQuery>();
		for (HGHandle t : lc.targets())
			L.add(ToQueryMap.toQuery(graph, new IncidentCondition(t)));
		if (L.isEmpty())
			return HGQuery.NOP;
		else if (L.size() == 1)
			return L.get(0);
		else
		{
			Iterator<HGQuery> i = L.iterator();
			IntersectionQuery result = new IntersectionQuery(i.next(), 
															 i.next(), 
															 new SortedIntersectionResult());
			while (i.hasNext())
				result = new IntersectionQuery(i.next(), 
											   result,
											   new SortedIntersectionResult());
			return result;
		}
	}

}
