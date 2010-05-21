/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.cond2qry;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.type.TypeUtils;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.indexing.ByTargetIndexer;
import org.hypergraphdb.indexing.HGIndexer;
import org.hypergraphdb.query.*;

/**
 * 
 * @author Borislav Iordanov
 */
@SuppressWarnings("unchecked")
public class ExpressionBasedQuery<ResultType> extends HGQuery<ResultType>
{
	private HGQuery<ResultType> query = null; 
	private HGQueryCondition condition;
		
	/**
	 * <p>Transform a query condition into a disjunctive normal form.</p>
	 * 
	 * @param C A condition, based on ands and ors to be transformed.
	 * @return The disjunctive normal form of C.
	 */
	private static HGQueryCondition toDNF(HGQueryCondition C)
	{
		if (C instanceof And)
		{			
			And and = (And)C;
			HashSet<HGQueryCondition> andSet = new HashSet<HGQueryCondition>();			
			for (int i = 0; i < and.size(); i++)
			{
				HGQueryCondition sub = and.get(i);
				sub = toDNF(sub);
				// here 'sub' is either a primitive condition, a single 'And' or a
				// list of Or-ed Ands or primitives.
				if (sub instanceof And)
				{
					for (HGQueryCondition subsub:(And)sub)
						if (!andSet.contains(subsub))
							andSet.add(subsub);					
				}
				else if (sub instanceof Or)
				{
					Or result = new Or();
					for (HGQueryCondition subsub:(Or)sub)
					{
						And newsub = new And();
						newsub.add(subsub);
						newsub.addAll(andSet);
						newsub.addAll(and.subList(i + 1, and.size()));
						result.add(newsub);
					}
					return toDNF(result);
				}				
				else
					andSet.add(sub);
			}
			and = new And();
			and.addAll(andSet);
			return and;
		}
		else if (C instanceof Or)
		{
			Or or = (Or)C;
			HashSet<HGQueryCondition> orSet = new HashSet<HGQueryCondition>();
			for (int i = 0; i < or.size(); i++)
			{
				HGQueryCondition sub = or.get(i);
				sub = toDNF(sub);
				if (sub instanceof Or)
				{					
					for (HGQueryCondition subsub:(Or)sub)
						if (!orSet.contains(subsub))
							orSet.add(subsub);
				}
				else if (!orSet.contains(sub))
					orSet.add(sub);
			}
			return or;
		}
        else if (C instanceof MapCondition)
        {
            MapCondition mcond = (MapCondition)C;
            return new MapCondition(toDNF(mcond.getCondition()), mcond.getMapping());
        }		
		else
			return C;
	}
	
	private boolean checkConsistent(AtomTypeCondition c1, AtomTypeCondition c2)
	{
		HGHandle h1 = (c1.getTypeHandle() != null) ? 
						c1.getTypeHandle() : 
						graph.getTypeSystem().getTypeHandle(c1.getJavaClass());
		if (c2.getTypeHandle() != null)
			return h1.equals(c2.getTypeHandle());
		else
			return h1.equals(graph.getTypeSystem().getTypeHandle(c2.getJavaClass()));
	}
	
	private boolean checkConsistent(TypedValueCondition c1, AtomTypeCondition c2)
	{
		HGHandle h1 = (c1.getTypeHandle() != null) ? 
				c1.getTypeHandle() : 
				graph.getTypeSystem().getTypeHandle(c1.getJavaClass());
		if (c2.getTypeHandle() != null)
			return h1.equals(c2.getTypeHandle());
		else
			return h1.equals(graph.getTypeSystem().getTypeHandle(c2.getJavaClass()));		
	}
	
	private boolean checkConsistent(TypedValueCondition tc, AtomValueCondition vc)
	{
		return HGUtils.eq(tc.getValue(), vc.getValue()) && tc.getOperator() == vc.getOperator();
	}

	private boolean checkConsistent(HGAtomType type, AtomPartCondition vc)
	{
		return TypeUtils.getProjection(graph, type, vc.getDimensionPath()) != null;
	}
	
	// apply a few simply transformation for common cases...
	// this kind of assumes that we are already in DNF because 
	// conjunction are handled by transforming them as if we are at 
	// the top level
	private HGQueryCondition simplify(HGQueryCondition cond)
	{
		if (cond instanceof And)
		{
			And in = (And)cond;
			And out = new And();
			out.addAll(in);
/*			for (HGQueryCondition c : in)
			{
				c = simplify(c);
				if (c instanceof And)
					out.addAll((And)c);
				else if (c == Nothing.Instance)
					return c;
				else
					out.add(c);
			} */
			
			// At the end of the following step, the conjunction will have
			// either a single TypedValueCondition or at most one AtomTypeCondition
			// and one AtomValueCondition. This step insures that there are no
			// contradictory conditions amongst the condition of type
			// AtomValueCondition, AtomTypeCondition and TypedValueCondition
			AtomTypeCondition byType = null;
			AtomValueCondition byValue = null;
			TypedValueCondition byTypedValue = null;
			HashSet<OrderedLinkCondition> oLinks = new HashSet<OrderedLinkCondition>();
			HashSet<AtomPartCondition> byPart = new HashSet<AtomPartCondition>();
			boolean has_ordered = false;
			boolean has_ra = false;
			for (Iterator<HGQueryCondition> i = out.iterator(); i.hasNext(); )
			{
				HGQueryCondition c = i.next();
				if (c instanceof AtomTypeCondition)
				{
					if (byType == null)
					{
						if (byTypedValue != null)
							if(!checkConsistent(byTypedValue, (AtomTypeCondition)c))
								return Nothing.Instance;
							else
								i.remove();
						else
							byType = (AtomTypeCondition)c;
					}
					else if (checkConsistent(byType, (AtomTypeCondition)c))
						i.remove();
					else
						return Nothing.Instance;							
				}
				else if (c instanceof AtomValueCondition)
				{
					if (byValue == null)
					{						
						if (byTypedValue != null)
							if(!checkConsistent(byTypedValue, (AtomValueCondition)c))
								return Nothing.Instance;
							else
								i.remove();
						else
							byValue = (AtomValueCondition)c;						
					}
					else if (byValue.equals((AtomValueCondition)c))
						i.remove();
					else
						return Nothing.Instance;
				}
				else if (c instanceof TypedValueCondition)
				{
					if (byTypedValue ==  null)
						byTypedValue = (TypedValueCondition)byTypedValue;
					else if (byTypedValue.equals((TypedValueCondition)c))
						i.remove();
					else
						return Nothing.Instance;					
				}
				else if (c instanceof AtomPartCondition)
				{
					byPart.add((AtomPartCondition)c);
				}
				else if (c instanceof OrderedLinkCondition)
				{
					oLinks.add((OrderedLinkCondition)c);
				}
				else
				{
					ConditionToQuery transform = ToQueryMap.getInstance().get(c.getClass());
					if (transform != null)
					{
						QueryMetaData qmd = transform.getMetaData(graph, c);
						has_ordered = has_ordered || qmd.ordered;
						has_ra = has_ra || qmd.randomAccess;
					}
				}
			}
			
			HGHandle typeHandle  = null;
			
			if (byTypedValue != null)
			{
				if (byType != null)
					if (!checkConsistent(byTypedValue, byType))
						return Nothing.Instance;
					else
					{
						out.remove(byType);
						if (byType.getTypeHandle() != null)
							typeHandle = byType.getTypeHandle();
						byType = null;
					}
				if (byValue != null)
					if (!checkConsistent(byTypedValue, byValue))
						return Nothing.Instance;
					else
					{
						out.remove(byValue);
						byValue = null;
					}				
				if (typeHandle == null)
					if (byTypedValue.getTypeHandle() != null)
						typeHandle = byTypedValue.getTypeHandle();
					else
						typeHandle = graph.getTypeSystem().getTypeHandle(byTypedValue.getJavaClass());
			}
			else if (byType != null)
			{
				if (byType.getTypeHandle() != null)
					typeHandle  = byType.getTypeHandle();
				else
					typeHandle = graph.getTypeSystem().getTypeHandle(byType.getJavaClass());
				if (byValue != null)
				{
					out.add(byTypedValue = new TypedValueCondition(typeHandle, byValue.getValue(), byValue.getOperator()));
					out.remove(byType);
					out.remove(byValue);					
					byType = null;
					byValue = null;
				}
			}
			
			// now, we check for indexing by value parts, if we find an appropriate index
			// then we can eliminate the "type" predicate altogether (since bypart indices
			// are always for a particular atom type) and a "by value" condition is kept
			// only as a predicate
			if (typeHandle != null && byPart.size() > 0)
			{
				HGAtomType type = (HGAtomType)graph.get(typeHandle);
				if (type == null)
					throw new HGException("No type for type handle " + typeHandle + " in this HyperGraph instance.");
				for (AtomPartCondition pc : byPart)									
					if (!checkConsistent(type, pc))
						return Nothing.Instance;
					else
					{
						ByPartIndexer indexer = new ByPartIndexer(typeHandle, pc.getDimensionPath());
						HGIndex idx = graph.getIndexManager().getIndex(indexer);
						if (idx != null)
						{
							if (byType != null)
							{
								out.remove(byType);
								byType = null;
							}
							else if (byTypedValue != null)
							{
								out.remove(byTypedValue);
								out.add(new ValueAsPredicateOnly(byTypedValue.getValue(), 
															     byTypedValue.getOperator()));
								byTypedValue = null;
							}
							out.remove(pc);
							out.add(new IndexedPartCondition(typeHandle, idx, pc.getValue(), pc.getOperator()));
						}
					}
			}
			// Check for "by-target" indices within an OrderedLinkConditions and replace
			// the corresponding 'incident' condition with one based on the index.
			// Here would be an opportunity to use HGTypeStructuralInfo on a link type and
			// possibly eliminate the OrderedLinkCondition (and resulting predicate call during
			// query execution) altogether
			if (typeHandle != null)
				for (OrderedLinkCondition c : oLinks)
				{					
					for (int ti = 0; ti < c.targets().length; ti++)
					{					
						HGHandle targetHandle = c.targets()[ti];
						if (targetHandle.equals(HGHandleFactory.anyHandle))
							continue;
						ByTargetIndexer indexer = new ByTargetIndexer(typeHandle, ti);
						HGIndex<HGPersistentHandle, HGPersistentHandle> idx = graph.getIndexManager().getIndex(indexer);
						if (idx != null)
						{
							if (byType != null)
							{
								out.remove(byType);
								byType = null;
							}
							else if (byTypedValue != null)
							{
								out.remove(byTypedValue);
								out.add(new AtomValueCondition(byTypedValue.getValue(), 
															   byTypedValue.getOperator()));
								byTypedValue = null;
							}							
							out.add(new IndexCondition<HGPersistentHandle, HGPersistentHandle>(
										idx, graph.getPersistentHandle(targetHandle)));
							out.remove(new IncidentCondition(targetHandle));
						}
					}
				}
			return out;
		}
		else if (cond instanceof Or)
		{
			Or in = (Or)cond;
			Or out = new Or();
			for (HGQueryCondition c : in)
			{
				c = simplify(c);
				if (c instanceof Or)
					out.addAll((Or)c);				
				else if (c != Nothing.Instance)
					out.add(c);
			}
			return out;
		}
		else if (cond instanceof MapCondition)
		{
		    MapCondition mcond = (MapCondition)cond;
		    return new MapCondition(simplify(mcond.getCondition()),
		                            mcond.getMapping());
		}
		else
			return cond;
	}	

	private List<AtomPartCondition> getAtomIndexedPartsConditions(HyperGraph graph, HGHandle hType, Object value)
	{
		ArrayList<AtomPartCondition> L = new ArrayList<AtomPartCondition>();
		List<HGIndexer> indexers = graph.getIndexManager().getIndexersForType(hType);
		if (indexers == null)
			return L;
		for (HGIndexer idx : indexers)
		{
			if (idx instanceof ByPartIndexer)
			{
				String [] dimPath = ((ByPartIndexer)idx).getDimensionPath();
				Object partValue = TypeUtils.project(graph, hType, value, dimPath, true).getValue();
				L.add(new AtomPartCondition(dimPath, partValue));
			}
		}
		return L;
	}
	
	private HGQueryCondition expand(HyperGraph graph, HGQueryCondition cond)
	{
		if (cond instanceof TypePlusCondition)
		{
			TypePlusCondition ac = (TypePlusCondition)cond;
			if (ac.getJavaClass() == null)
				ac.setJavaClass(graph.getTypeSystem().getClassForType(ac.getBaseType()));
			else if (ac.getBaseType() == null)
				ac.setBaseType(graph.getTypeSystem().getTypeHandle(ac.getJavaClass()));
			Or orCondition = new Or();
            for (HGHandle h : ac.getSubTypes(graph))
            	orCondition.add(new AtomTypeCondition(h));
            cond = orCondition;
		}
		else if (cond instanceof AtomTypeCondition)
		{
			AtomTypeCondition tc = (AtomTypeCondition)cond;
			if (tc.getJavaClass() == null)
				tc.setJavaClass(graph.getTypeSystem().getClassForType(tc.getTypeHandle()));
			else if (tc.getTypeHandle() == null)
				tc.setTypeHandle(graph.getTypeSystem().getTypeHandle(tc.getJavaClass()));			
		}
		else if (cond instanceof TypedValueCondition && ((TypedValueCondition)cond).getOperator() == ComparisonOperator.EQ)
		{
			TypedValueCondition tc = (TypedValueCondition)cond;
			if (tc.getJavaClass() == null)
				tc.setJavaClass(graph.getTypeSystem().getClassForType(tc.getTypeHandle()));
			else if (tc.getTypeHandle() == null)
				tc.setTypeHandle(graph.getTypeSystem().getTypeHandle(tc.getJavaClass()));
			List<AtomPartCondition> indexedParts = getAtomIndexedPartsConditions(graph, tc.getTypeHandle(), tc.getValue());
			if (!indexedParts.isEmpty())
			{
				And and = hg.and(cond);
				for (AtomPartCondition pc : indexedParts)
					and.add(pc);
				cond = and;
			}
		}
		else if (cond instanceof AtomValueCondition && ((AtomValueCondition)cond).getOperator() == ComparisonOperator.EQ)
		{
			AtomValueCondition vc = (AtomValueCondition)cond;
            Object value = vc.getValue();
            if (value == null)
                throw new HGException("Search by null values is not supported yet.");
            HGHandle type = graph.getTypeSystem().getTypeHandle(value);			
			List<AtomPartCondition> indexedParts = getAtomIndexedPartsConditions(graph, type, value);
			if (!indexedParts.isEmpty())
			{
				And and = hg.and(cond, new AtomTypeCondition(type));
				for (AtomPartCondition pc : indexedParts)
					and.add(pc);
				cond = and;
			}
		}		
		else if (cond instanceof And)
		{
			And result = new And();
			for (HGQueryCondition sub : (And)cond)
				result.add(expand(graph, sub));
			cond = result;
		}
		else if (cond instanceof Or)
		{
			Or result = new Or();
			for (HGQueryCondition sub : (Or)cond)
				result.add(expand(graph, sub));
			cond = result;			
		}
		else if (cond instanceof OrderedLinkCondition)
		{
			And result = new And();
			result.add(cond);
			for (HGHandle h : ((OrderedLinkCondition)cond).targets())
				if (!h.equals(HGHandleFactory.anyHandle()))
					result.add(new IncidentCondition(h));
			cond = result;
		}
		else if (cond instanceof LinkCondition)
		{
			And result = new And();
			for (HGHandle h : ((LinkCondition)cond).targets())
				if (!h.equals(HGHandleFactory.anyHandle()))
					result.add(new IncidentCondition(h));
			cond = result;
		}		
		else if (cond instanceof MapCondition)
		{
            MapCondition mcond = (MapCondition)cond;
            cond = new MapCondition(expand(graph, mcond.getCondition()),
                                    mcond.getMapping());		    
		}
		return cond;
	}
	
	public ExpressionBasedQuery(final HyperGraph graph, final HGQueryCondition condition)
	{
		this.graph = graph;	
		this.condition = graph.getTransactionManager().ensureTransaction(new Callable<HGQueryCondition>() {
			public HGQueryCondition call()
			{
				return simplify(toDNF(expand(graph, condition))); 
			}
		});
		query = ToQueryMap.toQuery(graph, this.condition);
	}
	
    public HGSearchResult<ResultType> execute()
    {    	
        return query.execute();
    }
    
    /** 
     * <p>Return a possibly simplified and normalized version of the condition with
     * which this query was constructed.</p>
     */
    public HGQueryCondition getCondition()
    {
    	return condition;
    }
    
    public HGQuery getCompiledQuery()
    {
        return query;
    }
}
