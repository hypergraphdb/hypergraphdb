/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

import java.util.HashMap;
import org.hypergraphdb.query.And;
import org.hypergraphdb.query.AnyAtomCondition;
import org.hypergraphdb.query.AtomPartCondition;
import org.hypergraphdb.query.AtomTypeCondition;
import org.hypergraphdb.query.AtomValueCondition;
import org.hypergraphdb.query.BFSCondition;
import org.hypergraphdb.query.ComparisonOperator;
import org.hypergraphdb.query.DFSCondition;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.IncidentCondition;
import org.hypergraphdb.query.IndexCondition;
import org.hypergraphdb.query.IndexedPartCondition;
import org.hypergraphdb.query.LinkCondition;
import org.hypergraphdb.query.MapCondition;
import org.hypergraphdb.query.Nothing;
import org.hypergraphdb.query.Or;
import org.hypergraphdb.query.OrderedLinkCondition;
import org.hypergraphdb.query.SubsumedCondition;
import org.hypergraphdb.query.SubsumesCondition;
import org.hypergraphdb.query.TargetCondition;
import org.hypergraphdb.query.TypePlusCondition;
import org.hypergraphdb.query.TypedValueCondition;
import org.hypergraphdb.query.cond2qry.ExpressionBasedQuery;
import org.hypergraphdb.type.HGAtomType;

// This is a temporary implementation, to research a bit what's involved in
// estimating result set sizes...
@SuppressWarnings("unchecked")
class ResultSizeEstimation
{
	// count() should return the size of of the result set if the condition
	// is evaluated in cost() should simply estimate how costly it would
	// be the perform the count. cost will return Integer.MAX_VALUE when 
	// the only way to count is the evaluate the query and scan the result set
	
	public interface Counter
	{
		long count(HyperGraph graph, HGQueryCondition cond);
		long cost(HyperGraph graph, HGQueryCondition cond);
	}
	
	static HashMap<Class<?>, Counter> countersMap = new HashMap<Class<?>, Counter>();

	static long countResultSet(HyperGraph graph, HGQueryCondition cond)
	{
		return countResultSet(HGQuery.make(graph, cond));
	}
	
	static long countResultSet(HGQuery<?> q)
	{
    	// need to do full query.
    	HGSearchResult<HGPersistentHandle> rs = (HGSearchResult<HGPersistentHandle>)q.execute();
    	try
    	{
			long result = 0;        			
    		for (; rs.hasNext(); rs.next())
    			result++;
    		return result;            		
    	}
    	finally
    	{
    		try { rs.close(); } catch (Throwable t) { }
    	}		
	}	
	
	// The default version when no counts can be obtained by simple means
	static class FullScanCounter implements Counter
	{
		public long count(HyperGraph graph, HGQueryCondition x)
		{
			return countResultSet(graph, x);
		}
		
		public long cost(HyperGraph graph, HGQueryCondition x)
		{
			return Integer.MAX_VALUE;
		}
	}
	
	static Counter fullScanCounter = new FullScanCounter();
	
	static {
		
	countersMap.put(AnyAtomCondition.class, new Counter()
	{ 
		public long count(HyperGraph graph, HGQueryCondition cond)
		{
			HGSearchResult<HGPersistentHandle> rs = graph.indexByType.scanKeys();
			try
			{
				long result = 0;				
				while (rs.hasNext())
					// TODO: this is actually stupid because that we have to create another cursor
					// and position it on the same key as our rs.current...but can't break
					// information hiding boundaries!
					result += graph.indexByType.count(rs.next()); 
				return result;
			}
			finally
			{
				try { rs.close(); } catch (Throwable t) { }
			}
		}
		
		public long cost(HyperGraph graph, HGQueryCondition cond)
		{
			return graph.indexByType.count();
		}			
	});

	countersMap.put(AtomTypeCondition.class, new Counter()
	{ 
		public long count(HyperGraph graph, HGQueryCondition x)
		{
			AtomTypeCondition cond = (AtomTypeCondition)x;
			HGHandle typeHandle = cond.getTypeHandle(); 
			if (typeHandle == null)
				typeHandle = graph.getTypeSystem().getTypeHandleIfDefined(cond.getJavaClass());
			if (typeHandle != null)
				return graph.indexByType.count(graph.getPersistentHandle(typeHandle));
			else
				return 0;
		}
		
		public long cost(HyperGraph graph, HGQueryCondition x)
		{
			return 1;
		}			
	});

	countersMap.put(TypePlusCondition.class, new Counter()
	{ 
		public long count(HyperGraph graph, HGQueryCondition x)
		{
			TypePlusCondition cond = (TypePlusCondition)x;
			long result = 0;
			for (HGHandle h : cond.getSubTypes(graph))			
				result += graph.indexByType.count(graph.getPersistentHandle(h));
			return result;
		}
		
		public long cost(HyperGraph graph, HGQueryCondition x)
		{
			TypePlusCondition cond = (TypePlusCondition)x;
			return cond.getSubTypes(graph).size();
		}			
	});
	
	countersMap.put(TypedValueCondition.class, new Counter()
	{
		public long count(HyperGraph graph, HGQueryCondition x)
		{
			TypedValueCondition cond = (TypedValueCondition)x;
            HGHandle typeHandle = cond.getTypeHandle();
            if (typeHandle == null)
            	typeHandle = graph.getTypeSystem().getTypeHandleIfDefined(cond.getJavaClass());
            if (typeHandle == null)
            	return 0;
            HGAtomType type = graph.getTypeSystem().getType(typeHandle);
            if (type instanceof HGSearchable && cond.getOperator() == ComparisonOperator.EQ)
            {
            	HGSearchResult<HGPersistentHandle> rs = ((HGSearchable)type).find(cond.getValue());
            	try
            	{
        			long result = 0;        			
            		while (rs.hasNext())
            			result += graph.indexByValue.count(rs.next());
            		return result;            		
            	}
            	finally
            	{
            		try { rs.close(); } catch (Throwable t) { }
            	}
            }
            else
            {
            	return countResultSet(graph, x);
            }
		}
		
		public long cost(HyperGraph graph, HGQueryCondition x)
		{
			TypedValueCondition cond = (TypedValueCondition)x;
            HGHandle typeHandle = cond.getTypeHandle();
            if (typeHandle == null)
            	typeHandle = graph.getTypeSystem().getTypeHandle(cond.getJavaClass());
            if (typeHandle == null)
            	return 0;
            HGAtomType type = graph.getTypeSystem().getType(typeHandle);
            if (type instanceof HGSearchable && cond.getOperator() == ComparisonOperator.EQ)
            	return 2;
            else
            	return Integer.MAX_VALUE;
		}			
	});

	countersMap.put(AtomValueCondition.class, new Counter()
	{ 
		public long count(HyperGraph graph, HGQueryCondition x)
		{
            AtomValueCondition vc = (AtomValueCondition)x;
            Object value = vc.getValue();
            if (value == null)
                throw new HGException("Count by null values is not supported yet.");
            HGHandle type = graph.getTypeSystem().getTypeHandle(value);
			return countersMap.get(TypedValueCondition.class).
				count(graph, new TypedValueCondition(type, 
													 vc.getValue(), 
													 vc.getOperator()));                			
		}
		
		public long cost(HyperGraph graph, HGQueryCondition x)
		{
            AtomValueCondition vc = (AtomValueCondition)x;
            Object value = vc.getValue();
            if (value == null)
                throw new HGException("Count by null values is not supported yet.");
            HGHandle type = graph.getTypeSystem().getTypeHandle(value);
			return countersMap.get(TypedValueCondition.class).
				cost(graph, new TypedValueCondition(type, 
													 vc.getValue(), 
													 vc.getOperator()));
		}			
	});

	countersMap.put(TargetCondition.class, new Counter()
	{ 
		public long count(HyperGraph graph, HGQueryCondition x)
		{
			HGHandle h = ((TargetCondition)x).getLink();
			if (graph.isLoaded(h))
				return ((HGLink)graph.get(h)).getArity();
			else
			{
				HGPersistentHandle [] A = graph.getStore().getLink(graph.getPersistentHandle(h));
				if (A == null)
					throw new NullPointerException("No link data for handle " + h);
				else
					return A.length - 2;
			}
		}
		
		public long cost(HyperGraph graph, HGQueryCondition x)
		{
			return 1;
		}			
	});
	
	countersMap.put(IncidentCondition.class, new Counter()
	{ 
		public long count(HyperGraph graph, HGQueryCondition x)
		{
			IncidentCondition cond = (IncidentCondition)x;
			return graph.getStore().getIncidenceSetCardinality(
					graph.getPersistentHandle(cond.getTarget()));
		}
		
		public long cost(HyperGraph graph, HGQueryCondition x)
		{
			return 1;
		}			
	});

	countersMap.put(MapCondition.class, new Counter()
	{ 
		public long count(HyperGraph graph, HGQueryCondition x)
		{
			x = ((MapCondition)x).getCondition();
			Counter c = countersMap.get(x.getClass());
			return c == null ? countResultSet(graph, x) : c.count(graph, x);				

		}
		
		public long cost(HyperGraph graph, HGQueryCondition x)
		{
			x = ((MapCondition)x).getCondition();
			Counter c = countersMap.get(x.getClass());
			return c == null ? Integer.MAX_VALUE : c.cost(graph, x);					
		}			
	});

	countersMap.put(IndexCondition.class, new Counter()
	{ 
		@SuppressWarnings("rawtypes")
		public long count(HyperGraph graph, HGQueryCondition x)
		{
			IndexCondition ic = (IndexCondition)x;
			if (ic.getOperator() == ComparisonOperator.EQ)
				return ic.getIndex().count(ic.getKey());			
			else
				return countResultSet(graph, ic);
		}
		
		@SuppressWarnings("rawtypes")
		public long cost(HyperGraph graph, HGQueryCondition x)
		{
			IndexCondition ic = (IndexCondition)x;
			return ic.getOperator() == ComparisonOperator.EQ ? 1 : Integer.MAX_VALUE;
		}			
	});
	
	countersMap.put(IndexedPartCondition.class, new Counter()
	{ 
		public long count(HyperGraph graph, HGQueryCondition x)
		{
			IndexedPartCondition ip = (IndexedPartCondition)x;
			if (ip.getOperator() == ComparisonOperator.EQ)
				return ((HGIndex<Object, Object>)ip.getIndex()).count(ip.getPartValue());			
			else
				return countResultSet(graph, ip);
		}
		
		public long cost(HyperGraph graph, HGQueryCondition x)
		{
			IndexedPartCondition ip = (IndexedPartCondition)x;
			return ip.getOperator() == ComparisonOperator.EQ ? 1 : Integer.MAX_VALUE;
		}			
	});
	
	countersMap.put(AtomPartCondition.class, new Counter()
	{ 
		public long count(HyperGraph graph, HGQueryCondition x)
		{
			throw new HGException("Can't count AtomPartCondition results: this condition can't be used alone.");			
		}
		
		public long cost(HyperGraph graph, HGQueryCondition x)
		{
			throw new HGException("Can't estimate cost of counting AtomPartCondition results: this condition can't be used alone.");
		}			
	});
	
	countersMap.put(And.class, new Counter()
	{ 
		public long count(HyperGraph graph, HGQueryCondition x)
		{
			ExpressionBasedQuery q = (ExpressionBasedQuery)HGQuery.make(graph, x);
			x = q.getCondition();
			if (x == Nothing.Instance)
				return 0;
			And cond = (And)q.getCondition();
			if (cond.size() == 1)
			{
				x = cond.get(0);
				Counter c = countersMap.get(cond.get(0).getClass());
				if (c != null)
					return c.count(graph, x);
			}
			return countResultSet(q);
		}
		
		public long cost(HyperGraph graph, HGQueryCondition x)
		{			
			ExpressionBasedQuery q = (ExpressionBasedQuery)HGQuery.make(graph, x);
			x = q.getCondition();
			if (x == Nothing.Instance)
				return 0;
			And cond = (And)q.getCondition();
			if (cond.size() == 1)
			{
				x = cond.get(0);
				Counter c = countersMap.get(cond.get(0).getClass());
				if (c != null)
					return c.cost(graph, x);
			}
			return Integer.MAX_VALUE;
		}			
	});

	countersMap.put(Or.class, new Counter()
	{ 
		public long count(HyperGraph graph, HGQueryCondition x)
		{
			long result = 0;
			for (HGQueryCondition cond : (Or)x)
			{
				Counter c = countersMap.get(cond.getClass());
				result += c == null ? countResultSet(graph, cond) : c.count(graph, cond);
			}
			return result;
		}
		
		public long cost(HyperGraph graph, HGQueryCondition x)
		{
			long cost = 0;
			for (HGQueryCondition cond : (Or)x)
			{
				Counter c = countersMap.get(cond.getClass());
				if (c == null)
					return Integer.MAX_VALUE;
				long cc = c.cost(graph, cond);
				if (cc == Integer.MAX_VALUE)
					return cc;
				else
					cost += cc;
			}
			return cost;
		}			
	});
	
	countersMap.put(SubsumesCondition.class, fullScanCounter);	
	countersMap.put(SubsumedCondition.class, fullScanCounter);
	countersMap.put(LinkCondition.class, fullScanCounter);
	countersMap.put(OrderedLinkCondition.class, fullScanCounter);
	countersMap.put(BFSCondition.class, fullScanCounter);
	countersMap.put(DFSCondition.class, fullScanCounter);
	}
}
