/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.cond2qry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.query.And;
import org.hypergraphdb.query.HGAtomPredicate;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.QueryCompile;
import org.hypergraphdb.query.impl.DelayedSetLoadPredicate;
import org.hypergraphdb.query.impl.IntersectionQuery;
import org.hypergraphdb.query.impl.PredicateBasedFilter;
import org.hypergraphdb.query.impl.RABasedPredicate;
import org.hypergraphdb.query.impl.SortedIntersectionResult;
//import org.hypergraphdb.query.impl.SortedIntersectionResult;
import org.hypergraphdb.query.impl.ZigZagIntersectionResult;

@SuppressWarnings("unchecked")
public class AndToQuery<ResultType> implements ConditionToQuery<ResultType>
{
	/**
	 * 
	 * <p>
	 * Order QueryMetaData instance by the expected size of the result set. The logic
	 * is a bit convoluted because there are 3 numbers in play: lower bound (LB) of
	 * the result, upper bound (UB) and expected size (E). We use the expected size
	 * if available, otherwise we use the upper bound if available or the lower bound
	 * with a "lowest priority". The assumption here is the E when provided should be 
	 * fairly accurate so there's no need to be overly conservative. In most cases, 
	 * the size is actually either known completely (e.g. in an index) or nothing
	 * is know about it. If no size (LB, UB or E) is known for at least one of the 
	 * parameters of the compare method, then the two are deemed equal (i.e. 0 is
	 * returned).
	 * </p>
	 *
	 * @author Borislav Iordanov
	 *
	 */
	private static class BySizeComparator implements Comparator<QueryMetaData>
	{
		public int compare(QueryMetaData o1, QueryMetaData o2)
		{
			long left = o1.sizeExpected > -1 ? o1.sizeExpected : 
						o1.sizeUB > -1 ? o1.sizeUB : o1.sizeLB;
			if (left == -1)
				return 0;
			long right = o2.sizeExpected > -1 ? o2.sizeExpected : 
						 o2.sizeUB > -1 ? o2.sizeUB : o2.sizeLB;
			if (right == -1 || left == right)
				return 0;
			else if (left > right)
				return 1;
			else
				return -1;				
		}		
	}
	
	private static BySizeComparator bySizeComparator = new BySizeComparator();
	
	public QueryMetaData getMetaData(HyperGraph graph, HGQueryCondition condition)
	{
		QueryMetaData x = QueryMetaData.ORACCESS.clone(condition); // assume we have ORACCESS, but check below
		boolean ispredicate = true;
		x.predicateCost = 0;
		for (HGQueryCondition sub : ((And)condition))
		{
			ConditionToQuery<?> transformer = QueryCompile.translator(graph, sub.getClass());
			if (transformer == null)
				if (! (sub instanceof HGAtomPredicate))
					throw new HGException("Condition " + sub + " is not query translatable, nor a predicate.");
				else 
				{
					x.ordered = false;
					x.randomAccess = false;
					continue;
				}
			QueryMetaData subx = transformer.getMetaData(graph, sub);
			ispredicate = ispredicate && subx.predicateCost > -1;
			x.predicateCost += subx.predicateCost;					
			x.ordered = x.ordered && subx.ordered;
			x.randomAccess = x.randomAccess && subx.randomAccess;
		}
		if (!ispredicate)
			x.predicateCost = -1;
		return x;
	}

	@SuppressWarnings("rawtypes")
	public HGQuery<ResultType> getQuery(HyperGraph graph, HGQueryCondition condition)
	{
		And and = (And)condition;
		
		//
		// Trivial limit cases.
		//
		if (and.size() == 0)
			return HGQuery.NOP();
		else if (and.size() == 1)
			return QueryCompile.translate(graph, and.iterator().next());
		
		// query conditions are partitioned into the following categories:
		// - ORA: ordered random access results
		// - RA: random access (but unordered) results
		// - O: ordered results
		// - P: not translatable to one of the above categories, but usable as predicates
		// - W: neither of the above (i.e. unordered, non-random-access, non-predicate yielding conditions
		List<QueryMetaData> ORA = new ArrayList<QueryMetaData>();
		List<QueryMetaData> RA = new ArrayList<QueryMetaData>();
		List<QueryMetaData> O = new ArrayList<QueryMetaData>();
		List<QueryMetaData> P = new ArrayList<QueryMetaData>();
		List<QueryMetaData> W = new ArrayList<QueryMetaData>();
		
		for (HGQueryCondition sub : and)
		{
			ConditionToQuery<ResultType> transformer = QueryCompile.translator(graph, sub.getClass());
			if (transformer == null)
			{
			    QueryMetaData qmd = QueryMetaData.MISTERY.clone(sub);
			    qmd.predicateOnly = true;
				P.add(qmd);
				continue;
			}
			QueryMetaData qmd = transformer.getMetaData(graph, sub);
			if (qmd.predicateOnly)
				P.add(qmd);
			else if (qmd.ordered && qmd.randomAccess)
				ORA.add(qmd);
			else if (qmd.ordered)
				O.add(qmd);
			else if (qmd.randomAccess)
				RA.add(qmd);
			else if (qmd.predicateCost > -1)
				P.add(qmd);
			else
				W.add(qmd);
		}

		//
		// Once the partition is done, the following query is constructed as follows:
	    //	
		// 1. First all ORA result sets are evaluated
		// 2. Then O sets are appended
		// 3. RA sets are used as predicates if there is some other base set that needs to be
		//    scanned anyway
		// 4. Results from the above construction are filter by the predicates in P
		// 5. W sets are scanned and loaded in memory				
		HGQuery result = null;
		HGQueryCondition c1 = null, c2 = null;
		
		// First ORA sets - we just build up nested zig-zag intersections
		if (ORA.size() > 1)
		{
			Collections.sort(ORA, bySizeComparator);
			Iterator<QueryMetaData> i = ORA.iterator();
			c1 = i.next().cond;
			c2 = i.next().cond;
			result = new IntersectionQuery(QueryCompile.translate(graph, c1),// toQueryMap.get(c1.getClass()).getQuery(graph, c1), 
			        QueryCompile.translate(graph, c2), //toQueryMap.get(c2.getClass()).getQuery(graph, c2),
										   new ZigZagIntersectionResult.Combiner());
			while (i.hasNext())
			{
				c1 = i.next().cond;
				result = new IntersectionQuery(result, 
				                               QueryCompile.translate(graph, c1), //toQueryMap.get(c1.getClass()).getQuery(graph, c1),
											   new ZigZagIntersectionResult.Combiner());
			}
		}
		else if (ORA.size() == 1)
		{
			O.addAll(ORA);
			ORA.clear();
		}
		
		// Next O sets - we just build up nested sorted intersections
		if (O.size() > 1)
		{
			Collections.sort(O, bySizeComparator);
			Iterator<QueryMetaData> i = O.iterator();
			if (result == null)
			{
				c1 = i.next().cond;
				c2 = i.next().cond;
				result = new IntersectionQuery(QueryCompile.translate(graph, c1), //toQueryMap.get(c1.getClass()).getQuery(graph, c1), 
				                               QueryCompile.translate(graph, c2), //toQueryMap.get(c2.getClass()).getQuery(graph, c2), 
											   new SortedIntersectionResult.Combiner()); 
			}
			while (i.hasNext())
			{
				c1 = i.next().cond;
				result = new IntersectionQuery(result, 
				                               QueryCompile.translate(graph, c1), // toQueryMap.get(c1.getClass()).getQuery(graph, c1), 
											   new SortedIntersectionResult.Combiner());					
			}						
		}
		else if (O.size() == 1)
		{
			c1 = O.iterator().next().cond;
			if (result == null)
				result = QueryCompile.translate(graph, c1); // toQueryMap.get(c1.getClass()).getQuery(graph, c1);
			else
				result = new IntersectionQuery(result, 
				                               QueryCompile.translate(graph, c1), //toQueryMap.get(c1.getClass()).getQuery(graph, c1),
											   new SortedIntersectionResult.Combiner());
		}
		
		if (result == null)
		{
			if (W.size() > 0)
			{
				Iterator<QueryMetaData> i = W.iterator();
				long n = 0;
				while (i.hasNext())
				{
					QueryMetaData curr = i.next();
					if (n < curr.getSizeExpected())
						c1 = curr.cond;
				}
				result = QueryCompile.translate(graph, c1); //toQueryMap.get(c1.getClass()).getQuery(graph, c1);
				W.remove(c1);
			}
			else if (RA.size() > 0)
			{
				Iterator<QueryMetaData> i = RA.iterator();
				double cost = 0.0;
				while (i.hasNext())
				{
					QueryMetaData curr = i.next();
					if (cost < curr.predicateCost)
						c1 = curr.cond;
				}
				result = QueryCompile.translate(graph, c1); // toQueryMap.get(c1.getClass()).getQuery(graph, c1);
				RA.remove(c1);						
			}
			else if (P.size() > 0) // some predicates can also be used as bases for search...when !qmd.predicateOnly
			{
			    QueryMetaData found = null;
			    for (QueryMetaData qmd : P)
			        if (!qmd.predicateOnly)
			        {
			            found = qmd;
			            break;
			        }
			    if (found != null)
			    {
			        result = QueryCompile.translate(graph, found.cond);
			        P.remove(found);
			    }
			}
			if (result == null)
				throw new HGException("No query condition translatable into a scannable result set.");
		}
		// Here, it remains to convert all remaining RA sets to predicates and all remaining W sets into
		// in memory sets and again into predicates.				
		
		// Transform RAs into predicates
		for (Iterator<QueryMetaData> i = RA.iterator(); i.hasNext(); )
		{
			QueryMetaData curr = i.next();
			c1 = curr.cond;
			QueryMetaData pqmd = QueryMetaData.MISTERY.clone(new RABasedPredicate(QueryCompile.translate(graph, c1)));
			pqmd.predicateCost = curr.predicateCost;
			P.add(pqmd);
		}
		
		// Add predicates in order from the less costly to execute to the most costly...
		while (!P.isEmpty())
		{
			double predicateCost = Double.MAX_VALUE;
			QueryMetaData lessCostly = null;
			for (Iterator<QueryMetaData> i = P.iterator(); i.hasNext(); )
			{
				QueryMetaData curr = i.next();
				if (curr.predicateCost < predicateCost)
				{
					predicateCost = curr.predicateCost;
					lessCostly = curr;
				}
			}
			result = new PredicateBasedFilter(graph, result, lessCostly.pred);
			P.remove(lessCostly);
		}
		
		// add Ws as predicates that lazily load their entire result sets into memory
		// this assumes that all result sets of Ws are UUID handles
		for (Iterator<QueryMetaData> i = W.iterator(); i.hasNext(); )
		{
			QueryMetaData curr = i.next();
			HGQuery q = QueryCompile.translate(graph, curr.cond);
			result = new PredicateBasedFilter(graph, result, new DelayedSetLoadPredicate(q));
		}
		
		return result;
	}
}
