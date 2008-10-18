package org.hypergraphdb.query.cond2qry;

import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.query.And;
import org.hypergraphdb.query.HGAtomPredicate;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.impl.DelayedSetLoadPredicate;
import org.hypergraphdb.query.impl.IntersectionQuery;
import org.hypergraphdb.query.impl.PredicateBasedFilter;
import org.hypergraphdb.query.impl.RABasedPredicate;
import org.hypergraphdb.query.impl.SortedIntersectionResult;
import org.hypergraphdb.query.impl.ZigZagIntersectionResult;

@SuppressWarnings("unchecked")
public class AndToQuery implements ConditionToQuery
{
	public QueryMetaData getMetaData(HyperGraph graph, HGQueryCondition condition)
	{
		QueryMetaData x = QueryMetaData.ORACCESS.clone(); // assume we have ORACCESS, but check below
		boolean ispredicate = true;
		x.predicateCost = 0;
		for (HGQueryCondition sub : ((And)condition))
		{
			ConditionToQuery transformer = ToQueryMap.getInstance().get(sub.getClass());
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

	public HGQuery<?> getQuery(HyperGraph graph, HGQueryCondition condition)
	{
		And and = (And)condition;
		
		//
		// Trivial limit cases.
		//
		if (and.size() == 0)
			return HGQuery.NOP;
		else if (and.size() == 1)
			return ToQueryMap.toQuery(graph, and.get(0));
		
		// query conditions are partitionned into the following categories:
		// - ORA: ordered random access results
		// - RA: random access (but unordered) results
		// - O: ordered results
		// - P: not translatable to one of the above categories, but usable as predicates
		// - W: neither of the above (i.e. unordered, non-random-access, non-predicate yielding conditions
		IdentityHashMap<HGQueryCondition, Double> ORA = new IdentityHashMap<HGQueryCondition, Double>();
		IdentityHashMap<HGQueryCondition, Double> RA = new IdentityHashMap<HGQueryCondition, Double>();
		IdentityHashMap<HGQueryCondition, Double> O = new IdentityHashMap<HGQueryCondition, Double>();
		IdentityHashMap<HGAtomPredicate, Double> P = new IdentityHashMap<HGAtomPredicate, Double>();
		IdentityHashMap<HGQueryCondition, QueryMetaData> W = new IdentityHashMap<HGQueryCondition, QueryMetaData>();
		
		for (HGQueryCondition sub : and)
		{
			ConditionToQuery transformer = ToQueryMap.getInstance().get(sub.getClass());
			if (transformer == null)
			{
				P.put((HGAtomPredicate)sub, 0.0);
				continue;
			}
			QueryMetaData qmd = transformer.getMetaData(graph, sub);
			if (qmd.predicateOnly)
				P.put((HGAtomPredicate)sub, qmd.predicateCost);
			else if (qmd.ordered && qmd.randomAccess)
				ORA.put(sub, qmd.predicateCost);
			else if (qmd.ordered)
				O.put(sub, qmd.predicateCost);
			else if (qmd.randomAccess)
				RA.put(sub, qmd.predicateCost);
			else if (qmd.predicateCost > -1)
				P.put((HGAtomPredicate)sub, qmd.predicateCost);
			else
				W.put(sub, qmd);
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
			Iterator<Map.Entry<HGQueryCondition, Double>> i = ORA.entrySet().iterator();
			c1 = i.next().getKey();
			c2 = i.next().getKey();
			result = new IntersectionQuery(ToQueryMap.toQuery(graph, c1),// toQueryMap.get(c1.getClass()).getQuery(graph, c1), 
										   ToQueryMap.toQuery(graph, c2), //toQueryMap.get(c2.getClass()).getQuery(graph, c2),
										   new ZigZagIntersectionResult());
			while (i.hasNext())
			{
				c1 = i.next().getKey();
				result = new IntersectionQuery(result, 
											   ToQueryMap.toQuery(graph, c1), //toQueryMap.get(c1.getClass()).getQuery(graph, c1),
											   new ZigZagIntersectionResult());
			}
		}
		else if (ORA.size() == 1)
		{
			O.putAll(ORA);
			ORA.clear();
		}
		
		// Next O sets - we just build up nested sorted intersections
		if (O.size() > 1)
		{
			Iterator<Map.Entry<HGQueryCondition, Double>> i = O.entrySet().iterator();
			if (result == null)
			{
				c1 = i.next().getKey();
				c2 = i.next().getKey();
				result = new IntersectionQuery(ToQueryMap.toQuery(graph, c1), //toQueryMap.get(c1.getClass()).getQuery(graph, c1), 
											   ToQueryMap.toQuery(graph, c2), //toQueryMap.get(c2.getClass()).getQuery(graph, c2), 
											   new SortedIntersectionResult()); 
			}
			while (i.hasNext())
			{
				c1 = i.next().getKey();
				result = new IntersectionQuery(result, 
											   ToQueryMap.toQuery(graph, c1), // toQueryMap.get(c1.getClass()).getQuery(graph, c1), 
											   new SortedIntersectionResult());					
			}						
		}
		else if (O.size() == 1)
		{
			c1 = O.keySet().iterator().next();
			if (result == null)
				result = ToQueryMap.toQuery(graph, c1); // toQueryMap.get(c1.getClass()).getQuery(graph, c1);
			else
				result = new IntersectionQuery(result, 
											   ToQueryMap.toQuery(graph, c1), //toQueryMap.get(c1.getClass()).getQuery(graph, c1),
											   new SortedIntersectionResult());
		}
		
		if (result == null)
			if (W.size() > 0)
			{
				Iterator<Map.Entry<HGQueryCondition, QueryMetaData>> i = W.entrySet().iterator();
				long n = 0;
				while (i.hasNext())
				{
					Map.Entry<HGQueryCondition, QueryMetaData> curr = i.next();
					if (n < curr.getValue().getSizeExpected())
						c1 = curr.getKey();
				}
				result = ToQueryMap.toQuery(graph, c1); //toQueryMap.get(c1.getClass()).getQuery(graph, c1);
				W.remove(c1);
			}
			else if (RA.size() > 0)
			{
				Iterator<Map.Entry<HGQueryCondition, Double>> i = RA.entrySet().iterator();
				double cost = 0.0;
				while (i.hasNext())
				{
					Map.Entry<HGQueryCondition, Double> curr = i.next();
					if (cost < curr.getValue())
						c1 = curr.getKey();
				}
				result = ToQueryMap.toQuery(graph, c1); // toQueryMap.get(c1.getClass()).getQuery(graph, c1);
				RA.remove(c1);						
			}
			else
				throw new HGException("No query condition translatable into a scannable result set.");
		
		// Here, it remains to convert all remaining RA sets to predicates and all remaining W sets into
		// in memory sets and again into predicates.				
		
		// Transform RAs into predicates
		for (Iterator<Map.Entry<HGQueryCondition, Double>> i = RA.entrySet().iterator(); i.hasNext(); )
		{
			Map.Entry<HGQueryCondition, Double> curr = i.next();
			c1 = curr.getKey();
			P.put(new RABasedPredicate(ToQueryMap.toQuery(graph, c1)) /* toQueryMap.get(c1.getClass()).getQuery(graph, c1)) */, curr.getValue());
		}
		
		// Add predicates in order from the less costly to execute to the most costly...
		while (!P.isEmpty())
		{
			double predicateCost = Double.MAX_VALUE;
			HGAtomPredicate lessCostly = null;
			for (Iterator<Map.Entry<HGAtomPredicate, Double>> i = P.entrySet().iterator(); i.hasNext(); )
			{
				Map.Entry<HGAtomPredicate, Double> curr = i.next();
				if (curr.getValue() < predicateCost)
				{
					predicateCost = curr.getValue();
					lessCostly = curr.getKey();
				}
			}
			result = new PredicateBasedFilter(graph, result, lessCostly);
			P.remove(lessCostly);
		}
		
		// add Ws as predicates that lazily load their entire result sets into memory
		// this assumes that all result sets of Ws are UUID handles
		for (Iterator<Map.Entry<HGQueryCondition, QueryMetaData>> i = W.entrySet().iterator(); i.hasNext(); )
		{
			Map.Entry<HGQueryCondition, QueryMetaData> curr = i.next();
			HGQuery q = ToQueryMap.toQuery(graph, curr.getKey()); // toQueryMap.get(curr.getKey().getClass()).getQuery(graph, curr.getKey());
			result = new PredicateBasedFilter(graph, result, new DelayedSetLoadPredicate(q));
		}
		
		return result;
	}
}