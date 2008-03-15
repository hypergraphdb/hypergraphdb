/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.type.TypeUtils;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.query.*;
import org.hypergraphdb.query.impl.*;
import org.hypergraphdb.algorithms.*;
import org.hypergraphdb.atom.HGSubsumes;

/**
 * 
 * @author Borislav Iordanov
 */
class ExpressionBasedQuery extends HGQuery
{
	private HGQueryCondition condition;
	private HyperGraph graph;
	
	private static interface ConditionToQuery
	{
		HGQuery getQuery(HyperGraph hg, HGQueryCondition condition);
		QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition condition);
	}
	
	private static HashMap<Class, ConditionToQuery> toQueryMap = 
		new HashMap<Class, ConditionToQuery>();
	
	static
	{
		toQueryMap.put(AnyAtomCondition.class, new ConditionToQuery()
		{ 
			public HGQuery getQuery(HyperGraph hg, HGQueryCondition c)
			{
				return new IndexScanQuery(hg.indexByType, false);						
			}
			
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				QueryMetaData x = QueryMetaData.MISTERY.clone();
				x.predicateCost = 0.5;
				return x;
			}			
		});
		toQueryMap.put(AtomTypeCondition.class, new ConditionToQuery()
		{
			public HGQuery getQuery(HyperGraph hg, HGQueryCondition c)
			{
				AtomTypeCondition ac = (AtomTypeCondition)c;
                HGHandle h = ac.getTypeHandle();
                if (h == null)
                    h = hg.getTypeSystem().getTypeHandle(ac.getJavaClass());
				return new SearchableBasedQuery(hg.indexByType, 
												hg.getPersistentHandle(h),
												ComparisonOperator.EQ);									
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				QueryMetaData x = QueryMetaData.ORACCESS.clone();
				x.predicateCost = 1;
				return x;
			}			
		});
		toQueryMap.put(TypePlusCondition.class, new ConditionToQuery()
		{
			public HGQuery getQuery(HyperGraph hg, HGQueryCondition c)
			{
				TypePlusCondition ac = (TypePlusCondition)c;
				Or orCondition = new Or();
                for (HGHandle h : ac.getSubTypes(hg))
                	orCondition.add(new AtomTypeCondition(h));
                return toQueryMap.get(Or.class).getQuery(hg, orCondition);
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				TypePlusCondition ac = (TypePlusCondition)c;
				Or orCondition = new Or();
                for (HGHandle h : ac.getSubTypes(hg))
                	orCondition.add(new AtomTypeCondition(h));
                return toQueryMap.get(Or.class).getMetaData(hg, orCondition);
			}			
		});		
		toQueryMap.put(TypedValueCondition.class, new ConditionToQuery()
		{
			public HGQuery getQuery(HyperGraph hg, HGQueryCondition c)
			{
                //
                // TODO: how to we deal with null values? For the String
                // primitive type at least, nulls are possible.
                //
				TypedValueCondition vc = (TypedValueCondition)c;
                Object value = vc.getValue();
                HGHandle typeHandle = vc.getTypeHandle();
                if (typeHandle == null)
                	typeHandle = hg.getTypeSystem().getTypeHandle(vc.getJavaClass());
                HGAtomType type = hg.getTypeSystem().getType(typeHandle);
                if (type == null)
                    throw new HGException("Cannot search by value " + value + 
                    		" of unknown HGAtomType with handle " + typeHandle);
                
                if (type instanceof HGSearchable && vc.getOperator() == ComparisonOperator.EQ ||
                	type instanceof HGOrderedSearchable)
                	//
                	// Find value handle by value and pipe into 'indexByValue' search, then filter
                	// by the expected type to make sure that it matches the actual type of the atoms
                	// so far obtained. 
                	//
                	return new PredicateBasedFilter(hg,
                			new PipeQuery(new SearchableBasedQuery((HGSearchable)type, value, vc.getOperator()),
                						  new SearchableBasedQuery(hg.indexByValue, 
                         										  null,
                         										  ComparisonOperator.EQ)),
                         	new AtomTypeCondition(typeHandle));     	
                else // else, we need to scan all atoms of the given type 
                	return new PredicateBasedFilter(hg,
                			new IndexBasedQuery(hg.indexByType, 
                								hg.getPersistentHandle(typeHandle)),
                			new AtomValueCondition(vc.getValue(), vc.getOperator()));
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				TypedValueCondition vc = (TypedValueCondition)c;
                HGHandle typeHandle = vc.getTypeHandle();
                if (typeHandle == null)
                	typeHandle = hg.getTypeSystem().getTypeHandle(vc.getJavaClass());
                HGAtomType type = hg.getTypeSystem().getType(typeHandle);
                if (type == null)
                    throw new HGException("Cannot search by value" + 
                    		" of unknown HGAtomType with handle " + typeHandle);
                QueryMetaData qmd;
                if (type instanceof HGSearchable && vc.getOperator() == ComparisonOperator.EQ ||
                	type instanceof HGOrderedSearchable)
                {
                	 qmd = QueryMetaData.MISTERY.clone();
                }
                else
                {
                	qmd = QueryMetaData.ORDERED.clone();
                }
                qmd.predicateCost = 2.5;
                return qmd;
			}	
		}); 
		toQueryMap.put(AtomValueCondition.class, new ConditionToQuery()
		{
			public HGQuery getQuery(HyperGraph hg, HGQueryCondition c)
			{
                //
                // TODO: how to we deal with null values? For the String
                // primitive type at least, nulls are possible. We can only deal
				// with nulls if the type is know in which case a TypedValueCondition
				// must have been used.
                //
                AtomValueCondition vc = (AtomValueCondition)c;
                Object value = vc.getValue();
                if (value == null)
                    throw new HGException("Search by null values is not supported yet.");
                HGHandle type = hg.getTypeSystem().getTypeHandle(value);
                
				return toQueryMap.get(TypedValueCondition.class).
					getQuery(hg, new TypedValueCondition(type, 
														 vc.getValue(), 
														 vc.getOperator()));                
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
                AtomValueCondition vc = (AtomValueCondition)c;
                Object value = vc.getValue();
                if (value == null)
                    throw new HGException("Search by null values is not supported yet.");
                HGHandle type = hg.getTypeSystem().getTypeHandle(value);
                
				return toQueryMap.get(TypedValueCondition.class).
					getMetaData(hg, new TypedValueCondition(type, 
														    vc.getValue(), 
															vc.getOperator()));               
				
			}
		});
		toQueryMap.put(IncidentCondition.class, new ConditionToQuery()
        {
			public HGQuery getQuery(final HyperGraph hg, final HGQueryCondition c)
			{
				final HGPersistentHandle handle = hg.getPersistentHandle(((IncidentCondition)c).getTarget());
				return new HGQuery()
				{
					public HGSearchResult execute()
					{
						return hg.getStore().getIncidenceResultSet(handle);
					}
				};
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				QueryMetaData x = QueryMetaData.ORACCESS.clone();
				x.predicateCost = 1;
				return x;
			}
		});
		toQueryMap.put(LinkCondition.class, new ConditionToQuery()
        {
			public HGQuery getQuery(HyperGraph hg, HGQueryCondition c)
			{
				LinkCondition lc = (LinkCondition)c;
				ArrayList<HGQuery> L = new ArrayList<HGQuery>();
				for (HGHandle t : lc.targets())
					L.add(toQuery(hg, new IncidentCondition(t)));
				if (L.isEmpty())
					return HGQuery.NOP;
				else if (L.size() == 1)
					return L.get(0);
				else
				{
					Iterator<HGQuery> i = L.iterator();
					IntersectionQuery result = new IntersectionQuery(i.next(), 
																	 i.next(), 
																	 new ZigZagIntersectionResult());
					while (i.hasNext())
						result = new IntersectionQuery(i.next(), 
													   result,
													   new ZigZagIntersectionResult());
					return result;
				}
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				QueryMetaData qmd;
				if (((LinkCondition)c).targets().size() == 0)
				{
					qmd = QueryMetaData.EMPTY.clone();
				}
				else
				{
					qmd = QueryMetaData.ORDERED.clone();
				}
				qmd.predicateCost = 0.5;
				return qmd;
			}
		});
		toQueryMap.put(SubsumesCondition.class, new ConditionToQuery()
        {
			public HGQuery getQuery(HyperGraph hg, HGQueryCondition c)
			{
				SubsumesCondition sc = (SubsumesCondition)c;
				HGHandle startAtom = sc.getSpecificHandle();
				if (startAtom == null && sc.getSpecificValue() != null)
				{
					startAtom = hg.getHandle(sc.getSpecificValue());
					if (startAtom == null)
						throw new HGException("Unable to translate 'subsumed' condition into a query since it is not based on an existing HyperGraph atom.");
				}				
				return new TraversalBasedQuery(
						new HGBreadthFirstTraversal(
							startAtom,
							new DefaultALGenerator(hg,
												   new AtomTypeCondition(hg.getTypeSystem().getTypeHandle(HGSubsumes.class)),
												   null,
												   false,
												   true,
												   true)
						), TraversalBasedQuery.ReturnType.targets);
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				QueryMetaData x = QueryMetaData.MISTERY.clone();
				// this is kind of approx. as the predicate may return very quickly 
				// or end up doing an all separate query on its own
				x.predicateCost = 5;
				return x;
			}
		});
		toQueryMap.put(SubsumedCondition.class, new ConditionToQuery()
        {
			public HGQuery getQuery(HyperGraph hg, HGQueryCondition c)
			{
				SubsumedCondition sc = (SubsumedCondition)c;
				HGHandle startAtom = sc.getGeneralHandle();
				if (startAtom == null && sc.getGeneralValue() != null)
				{
					startAtom = hg.getHandle(sc.getGeneralValue());
					if (startAtom == null)
						throw new HGException("Unable to translate 'subsumed' condition into a query since it is not based on an existing HyperGraph atom.");
				}
				return new TraversalBasedQuery(
						new HGBreadthFirstTraversal(
							startAtom,
							new DefaultALGenerator(hg,
												   new AtomTypeCondition(hg.getTypeSystem().getTypeHandle(HGSubsumes.class)),
												   null,
												   false,
												   true,
												   false)
						), TraversalBasedQuery.ReturnType.targets);
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				QueryMetaData x = QueryMetaData.MISTERY.clone();
				// this is kind of approx. as the predicate may return very quickly 
				// or end up doing an all separate query on its own
				x.predicateCost = 5;
				return x;
			}
		});		
		toQueryMap.put(OrderedLinkCondition.class, new ConditionToQuery()
        {
			public HGQuery getQuery(HyperGraph hg, HGQueryCondition c)
			{				
				OrderedLinkCondition lc = (OrderedLinkCondition)c;
				ArrayList<HGQuery> L = new ArrayList<HGQuery>();
				for (HGHandle t : lc.targets())
					L.add(toQuery(hg, new IncidentCondition(t)));
				if (L.isEmpty())
					return HGQuery.NOP;
				else if (L.size() == 1)
					return L.get(0);
				else
				{
					Iterator<HGQuery> i = L.iterator();
					IntersectionQuery result = new IntersectionQuery(i.next(), 
																	 i.next(),
																	 new ZigZagIntersectionResult());
					while (i.hasNext())
						result = new IntersectionQuery(i.next(), 
													   result,
													   new ZigZagIntersectionResult());
					// the following will find all links (unordered) with the given target
					// set and then filter to insure that the targets are properly ordered.
					return new PredicateBasedFilter(hg, result, lc);				
				}				
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				QueryMetaData qmd;
				if (((OrderedLinkCondition)c).targets().length == 0)					
					qmd = QueryMetaData.EMPTY.clone();
				else
					qmd = QueryMetaData.MISTERY.clone();
				qmd.predicateCost = 0.5;
				return qmd;
			}
		});		
		toQueryMap.put(MapCondition.class, new ConditionToQuery()
        {
			public HGQuery getQuery(HyperGraph hg, HGQueryCondition c)
			{				
				MapCondition mc = (MapCondition)c;
				HGQuery query = toQuery(hg, mc.getCondition());
				return new ResultMapQuery(query, mc.getMapping());
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				MapCondition mc = (MapCondition)c;
				QueryMetaData qmd = toQueryMap.get(mc.getCondition().getClass()).getMetaData(hg, mc.getCondition());
				qmd.randomAccess = false;
				qmd.ordered = false; // should we have an order preserving mapping?
				qmd.predicateCost = -1;
				return qmd;
			}
		});
		toQueryMap.put(IndexedPartCondition.class, new ConditionToQuery()
        {
			public HGQuery getQuery(HyperGraph hg, HGQueryCondition c)
			{				
				IndexedPartCondition ip = (IndexedPartCondition)c;
				if (ip.getIndex() instanceof HGSortIndex)						
					return new IndexBasedQuery((HGSortIndex)ip.getIndex(), 
											   ip.getPartValue(), 
											   ip.getOperator());
				else
					return new IndexBasedQuery(ip.getIndex(), ip.getPartValue());
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				return QueryMetaData.ORACCESS.clone();
			}
		});		
		toQueryMap.put(And.class, new ConditionToQuery()
        {
			public HGQuery getQuery(HyperGraph graph, HGQueryCondition c)
			{
				And and = (And)c;
				
				//
				// Trivial limit cases.
				//
				if (and.size() == 0)
					return HGQuery.NOP;
				else if (and.size() == 1)
					return toQuery(graph, and.get(0));
				
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
					ConditionToQuery transformer = toQueryMap.get(sub.getClass());
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
					result = new IntersectionQuery(toQueryMap.get(c1.getClass()).getQuery(graph, c1), 
												   toQueryMap.get(c2.getClass()).getQuery(graph, c2), 
												   new ZigZagIntersectionResult());
					while (i.hasNext())
					{
						c1 = i.next().getKey();
						result = new IntersectionQuery(result, 
													   toQueryMap.get(c1.getClass()).getQuery(graph, c1), 
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
						result = new IntersectionQuery(toQueryMap.get(c1.getClass()).getQuery(graph, c1), 
													   toQueryMap.get(c2.getClass()).getQuery(graph, c2), 
													   new SortedIntersectionResult()); 
					}
					while (i.hasNext())
					{
						c1 = i.next().getKey();
						result = new IntersectionQuery(result, 
													   toQueryMap.get(c1.getClass()).getQuery(graph, c1), 
													   new SortedIntersectionResult());					
					}						
				}
				else if (O.size() == 1)
				{
					c1 = O.keySet().iterator().next();
					if (result == null)
						result = toQueryMap.get(c1.getClass()).getQuery(graph, c1);
					else
						result = new IntersectionQuery(result, 
													   toQueryMap.get(c1.getClass()).getQuery(graph, c1),
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
						result = toQueryMap.get(c1.getClass()).getQuery(graph, c1);
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
						result = toQueryMap.get(c1.getClass()).getQuery(graph, c1);
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
					P.put(new RABasedPredicate(toQueryMap.get(c1.getClass()).getQuery(graph, c1)), curr.getValue());
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
					HGQuery q = toQueryMap.get(curr.getKey().getClass()).getQuery(graph, curr.getKey());
					result = new PredicateBasedFilter(graph, result, new DelayedSetLoadPredicate(q));
				}
				
				return result;
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				QueryMetaData x = QueryMetaData.ORACCESS.clone(); // assume we have ORACCESS, but check below
				boolean ispredicate = true;
				x.predicateCost = 0;
				for (HGQueryCondition sub : ((And)c))
				{
					ConditionToQuery transformer = toQueryMap.get(sub.getClass());
					if (transformer == null)
						if (! (sub instanceof HGAtomPredicate))
							throw new HGException("Condition " + sub + " is not query translatable, nor a predicate.");
						else 
						{
							x.ordered = false;
							x.randomAccess = false;
							continue;
						}
					QueryMetaData subx = transformer.getMetaData(hg, sub);
					ispredicate = ispredicate && subx.predicateCost > -1;
					x.predicateCost += subx.predicateCost;					
					x.ordered = x.ordered && subx.ordered;
					x.randomAccess = x.randomAccess && subx.randomAccess;
				}
				if (!ispredicate)
					x.predicateCost = -1;
				return x;
			}
		});
		toQueryMap.put(Or.class, new ConditionToQuery()
        {
			public HGQuery getQuery(HyperGraph hg, HGQueryCondition c)
			{
				Or or = (Or)c;
				if (or.size() == 0)
					return HGQuery.NOP;
				else if (or.size() == 1)
					return toQuery(hg, or.get(0));
				
				// TODO - we need to do better, even for this sloppy algorithm, we can
				// can try to factor out common conditions in conjunctions, make sure 
				// all conjuction end up in a treatable form (ordered or randomAccess) etc.
				
				HGQuery q1 = toQuery(hg, or.get(0));
				if (q1 == null)
					throw new HGException("Untranslatable condition " + or.get(0));
				HGQuery q2 = toQuery(hg, or.get(1));
				if (q2 == null)
					throw new HGException("Untranslatable condition " + or.get(1));
				UnionQuery result = new UnionQuery(q1, q2);
				for (int i = 2; i < or.size(); i++)
				{
					q1 = toQuery(hg, or.get(i));
					if (q1 == null)
						throw new HGException("Untranslatable condition " + or.get(i));					
					result = new UnionQuery(result, q1);
				}
				return result;
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				QueryMetaData x = QueryMetaData.ORACCESS.clone(); // TODO - this is only true in the worst case!
				boolean ispredicate = true;
				x.predicateCost = 0;
				for (HGQueryCondition sub : ((Or)c))
				{
					if (! (sub instanceof HGAtomPredicate))
						ispredicate = false;					
					ConditionToQuery transformer = toQueryMap.get(sub.getClass());
					if (transformer == null)
						if (! (sub instanceof HGAtomPredicate))
							throw new HGException("Condition " + sub + " is not query translatable, nor a predicate.");
						else 
						{
							x.ordered = false;
							x.randomAccess = false;
							continue;
						}					
					QueryMetaData subx = transformer.getMetaData(hg, sub);
					ispredicate = ispredicate && subx.predicateCost > -1;
					x.predicateCost += subx.predicateCost;
					x.ordered = x.ordered && subx.ordered;
					x.randomAccess = x.randomAccess && subx.randomAccess;
				}
				if (!ispredicate)
					x.predicateCost = -1;
				else
					x.predicateCost  /= ((Or)c).size();
				return x;
			}
		});
		toQueryMap.put(AtomPartCondition.class, new ConditionToQuery()
        {
			public HGQuery getQuery(HyperGraph hg, HGQueryCondition c)
			{
//				AtomPartCondition apc = (AtomPartCondition)c;
//				throw new HGException("Can't use an AtomPartCondition alone, please restrict your query by atom type or other general criteria.");
				return null;
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				// throw new HGException("Can't use an AtomPartCondition alone, please restrict your query by atom type or other general criteria.");
				QueryMetaData qmd = QueryMetaData.MISTERY;
				qmd.predicateCost = 1.5;
				qmd.predicateOnly = true;
				return qmd;
			}			
		});
		toQueryMap.put(AtomProjectionCondition.class, new ConditionToQuery()
        {
			public HGQuery getQuery(final HyperGraph graph, final HGQueryCondition c)
			{
				final AtomProjectionCondition apc = (AtomProjectionCondition)c;
				final HGQueryCondition bc = apc.getBaseSetCondition();
				HGHandle type = null;				
				// Special case that can be handled slightly more efficiently when the referent type is known:
				if (bc instanceof AtomTypeCondition)
					type = ((AtomTypeCondition)bc).getTypeHandle();					
				final HGHandle baseType = type;
				return new HGQuery()
				{
					public HGSearchResult execute() 
					{
						HGQuery q = ExpressionBasedQuery.toQuery(graph, bc); 
						return new ProjectionAtomResultSet(graph, 
														   q.execute(),
														   apc.getDimensionPath(), 
														   baseType);
					}
				};
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				QueryMetaData x = QueryMetaData.MISTERY.clone();
				x.predicateCost = 100; // this is because they will be a query involved in the predicate etc...expensive stuff
				return x;
			}
		});
	}
	
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
			and = (And)and.clone();
			for (int i = 0; i < and.size(); i++)
			{
				HGQueryCondition sub = and.get(i);
				sub = toDNF(sub);
				// here 'sub' is either a primitive condition, a single 'And' or a
				// list of Or-ed Ands or primitives.
				if (sub instanceof And)
				{
					and.remove(i--);
					for (HGQueryCondition subsub:(And)sub)
						and.add(subsub);					
				}
				else if (sub instanceof Or)
				{
					and.remove(i--);
					Or result = new Or();
					for (HGQueryCondition subsub:(Or)sub)
					{
						And newsub = (And)and.clone();
						newsub.add(subsub);
						result.add(newsub);
					}
					return toDNF(result);
				}				
			}
			return and;
		}
		else if (C instanceof Or)
		{
			Or or = (Or)C;
			or = (Or)or.clone();
			for (int i = 0; i < or.size(); i++)
			{
				HGQueryCondition sub = or.get(i);
				sub = toDNF(sub);
				if (sub instanceof Or)
				{
					or.remove(i--);					
					for (HGQueryCondition subsub:(Or)sub)
						or.add(subsub);
				}
				else
					or.set(i, sub);
			}
			return or;
		}
		else
			return C;
	}
	
	private static HGQueryCondition parse(String condition)
	{
		return null;
	}
	
	private static HGQuery toQuery(HyperGraph hg, HGQueryCondition condition)
	{
		ConditionToQuery transformer = (ConditionToQuery)toQueryMap.get(condition.getClass());
		if (transformer == null)
			throw new HGException("The query condition '" + condition + 
					"' could not be translated to an executable query because it is not specific enough. " +
					"Please try to contrain the query futher, for example by specifying the atom's types or " +
					"incidence sets or some indexed property value.");
		else
			return transformer.getQuery(hg, condition);
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
				else
				{
					ConditionToQuery transform = toQueryMap.get(c.getClass());
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
					out.add(new TypedValueCondition(typeHandle, byValue.getValue(), byValue.getOperator()));
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
				for (AtomPartCondition pc : byPart)
					if (!checkConsistent((HGAtomType)graph.get(typeHandle), pc))
						return Nothing.Instance;
					else
					{
						ByPartIndexer indexer = new ByPartIndexer(typeHandle, pc.getDimensionPath());
						HGIndex idx = graph.getIndexManager().getIndex(indexer);
						if (idx != null)
						{
							if (byType != null)
								out.remove(byType);
							else if (byTypedValue != null)
							{
								out.remove(byTypedValue);
								out.add(new AtomValueCondition(byTypedValue.getValue(), 
															   byTypedValue.getOperator()));
							}
							out.remove(pc);
							out.add(new IndexedPartCondition(typeHandle, idx, pc.getValue(), pc.getOperator()));
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
		else
			return cond;
	}	
	
/*	public ExpressionBasedQuery(HyperGraph hg, String condition)
	{
		this.graph = hg;
		this.condition = simplify(toDNF(parse(condition)));		
	} */
	
	public ExpressionBasedQuery(HyperGraph hg, HGQueryCondition condition)
	{
		this.graph = hg;		
		this.condition = simplify(toDNF(condition));
	}
	
    public <T> HGSearchResult<T> execute()
    {
    	HGQuery query = toQuery(graph, condition);
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
}
