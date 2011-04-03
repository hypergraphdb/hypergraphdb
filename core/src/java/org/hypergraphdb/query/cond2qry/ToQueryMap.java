/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.cond2qry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.hypergraphdb.HGBidirectionalIndex;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGOrderedSearchable;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGSearchable;
import org.hypergraphdb.HGSortIndex;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.algorithms.DefaultALGenerator;
import org.hypergraphdb.algorithms.HGBreadthFirstTraversal;
import org.hypergraphdb.atom.HGSubgraph;
import org.hypergraphdb.atom.HGSubsumes;
import org.hypergraphdb.query.And;
import org.hypergraphdb.query.AnyAtomCondition;
import org.hypergraphdb.query.AtomPartCondition;
import org.hypergraphdb.query.AtomProjectionCondition;
import org.hypergraphdb.query.AtomTypeCondition;
import org.hypergraphdb.query.AtomValueCondition;
import org.hypergraphdb.query.BFSCondition;
import org.hypergraphdb.query.ComparisonOperator;
import org.hypergraphdb.query.DFSCondition;
import org.hypergraphdb.query.HGAtomPredicate;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.IncidentCondition;
import org.hypergraphdb.query.IndexCondition;
import org.hypergraphdb.query.IndexedPartCondition;
import org.hypergraphdb.query.LinkCondition;
import org.hypergraphdb.query.MapCondition;
import org.hypergraphdb.query.Nothing;
import org.hypergraphdb.query.Or;
import org.hypergraphdb.query.OrderedLinkCondition;
import org.hypergraphdb.query.SubgraphContainsCondition;
import org.hypergraphdb.query.SubgraphMemberCondition;
import org.hypergraphdb.query.SubsumedCondition;
import org.hypergraphdb.query.SubsumesCondition;
import org.hypergraphdb.query.TargetCondition;
import org.hypergraphdb.query.TypePlusCondition;
import org.hypergraphdb.query.TypedValueCondition;
import org.hypergraphdb.query.impl.HandleArrayResultSet;
import org.hypergraphdb.query.impl.IndexBasedQuery;
import org.hypergraphdb.query.impl.IndexScanQuery;
import org.hypergraphdb.query.impl.IntersectionQuery;
import org.hypergraphdb.query.impl.LinkTargetsResultSet;
import org.hypergraphdb.query.impl.PipeQuery;
import org.hypergraphdb.query.impl.PredicateBasedFilter;
import org.hypergraphdb.query.impl.ProjectionAtomResultSet;
import org.hypergraphdb.query.impl.ResultMapQuery;
import org.hypergraphdb.query.impl.SearchableBasedQuery;
import org.hypergraphdb.query.impl.SortedIntersectionResult;
import org.hypergraphdb.query.impl.TraversalBasedQuery;
import org.hypergraphdb.query.impl.UnionQuery;
//import org.hypergraphdb.query.impl.ZigZagIntersectionResult;
import org.hypergraphdb.type.HGAtomType;

@SuppressWarnings("unchecked")
public class ToQueryMap extends HashMap<Class<?>, ConditionToQuery>
{
	private static final long serialVersionUID = -1;
	private static final ToQueryMap instance = new ToQueryMap();
	
	static
	{
	    instance.put(Nothing.class, new ConditionToQuery()
        { 
            public HGQuery<?> getQuery(HyperGraph graph, HGQueryCondition c)
            {
                return HGQuery.NOP;                     
            }
            
            public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
            {
                return QueryMetaData.EMPTY;
            }           
        });
		instance.put(AnyAtomCondition.class, new ConditionToQuery()
		{ 
			public HGQuery<?> getQuery(HyperGraph graph, HGQueryCondition c)
			{
				return new IndexScanQuery(graph.getIndexManager().getIndexByType(), false);						
			}
			
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				QueryMetaData x = QueryMetaData.MISTERY.clone(c);
				x.predicateCost = 0.5;
				return x;
			}			
		});
		instance.put(AtomTypeCondition.class, new ConditionToQuery()
		{
			private HGPersistentHandle getTypeHandle(HyperGraph graph, HGQueryCondition c)
			{
				AtomTypeCondition ac = (AtomTypeCondition)c;
                HGHandle h = ac.getTypeHandle();
                if (h == null)
                    h = graph.getTypeSystem().getTypeHandle(ac.getJavaClass());
                return graph.getPersistentHandle(h);
			}
			
			public HGQuery<?> getQuery(HyperGraph graph, HGQueryCondition c)
			{
				return new SearchableBasedQuery(graph.getIndexManager().getIndexByType(), 
												getTypeHandle(graph, c),
												ComparisonOperator.EQ);									
			}
			public QueryMetaData getMetaData(HyperGraph graph, HGQueryCondition c)
			{
				QueryMetaData x = QueryMetaData.ORACCESS.clone(c);
				x.predicateCost = 1;
				x.sizeExpected = 
				x.sizeLB = 
				x.sizeUB = graph.getIndexManager().getIndexByType().count(getTypeHandle(graph, c));
				return x;
			}			
		});
		instance.put(TypePlusCondition.class, new ConditionToQuery()
		{
			public HGQuery<?> getQuery(HyperGraph hg, HGQueryCondition c)
			{
				TypePlusCondition ac = (TypePlusCondition)c;
				Or orCondition = new Or();
                for (HGHandle h : ac.getSubTypes(hg))
                	orCondition.add(new AtomTypeCondition(h));
                return HGQuery.make(hg, orCondition); //instance.get(Or.class).getQuery(hg, orCondition);
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				TypePlusCondition ac = (TypePlusCondition)c;
				Or orCondition = new Or();
                for (HGHandle h : ac.getSubTypes(hg))
                	orCondition.add(new AtomTypeCondition(h));
                return toMetaData(hg, orCondition); //instance.get(Or.class).getMetaData(hg, orCondition);
			}			
		});		
		instance.put(TypedValueCondition.class, new ConditionToQuery()
		{
			public HGQuery<?> getQuery(HyperGraph hg, HGQueryCondition c)
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
                			new PipeQuery(new SearchableBasedQuery((HGSearchable<?,?>)type, value, vc.getOperator()),
                						  new SearchableBasedQuery(hg.getIndexManager().getIndexByValue(), 
                         										  null,
                         										  ComparisonOperator.EQ)),
                         	new AtomTypeCondition(typeHandle));     	
                else // else, we need to scan all atoms of the given type 
                	return new PredicateBasedFilter(hg,
                			new IndexBasedQuery(hg.getIndexManager().getIndexByType(), 
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
                	 qmd = QueryMetaData.MISTERY.clone(c);
                }
                else
                {
                	qmd = QueryMetaData.ORDERED.clone(c);
                }
                qmd.predicateCost = 2.5;
                return qmd;
			}	
		}); 
		instance.put(AtomValueCondition.class, new ConditionToQuery()
		{
			public HGQuery<?> getQuery(HyperGraph hg, HGQueryCondition c)
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
                
				return instance.get(TypedValueCondition.class).
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
                
				return instance.get(TypedValueCondition.class).
					getMetaData(hg, new TypedValueCondition(type, 
														    vc.getValue(), 
															vc.getOperator()));               
				
			}
		});
		instance.put(ValueAsPredicateOnly.class, new ConditionToQuery()
		{
			public HGQuery<?> getQuery(HyperGraph hg, HGQueryCondition c)
			{
				return null;
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				QueryMetaData qmd = QueryMetaData.MISTERY.clone();
				qmd.predicateOnly = true;
				qmd.pred = (HGAtomPredicate)c;
				return qmd;
			}
		});		
		instance.put(TargetCondition.class, new ConditionToQuery()
        {
			public HGQuery<HGHandle> getQuery(final HyperGraph graph, final HGQueryCondition c)
			{
				final HGPersistentHandle handle = graph.getPersistentHandle(((TargetCondition)c).getLink());
				return new HGQuery<HGHandle>()
				{
					public HGSearchResult<HGHandle> execute()
					{
						if (graph.isLoaded(handle))
							return new LinkTargetsResultSet((HGLink)graph.get(handle));
						else
						{
							HGPersistentHandle [] A = graph.getStore().getLink(handle);
							if (A == null)
								throw new NullPointerException("No link data for handle " + handle);
							return new HandleArrayResultSet(A, 2);
						}
					}
				};
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				QueryMetaData x = QueryMetaData.MISTERY.clone(c);
				x.predicateCost = 1;
				return x;
			}
		});		
		instance.put(IncidentCondition.class, new IncidentToQuery());
		instance.put(LinkCondition.class, new LinkToQuery());
		instance.put(SubsumesCondition.class, new ConditionToQuery()
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
				QueryMetaData x = QueryMetaData.MISTERY.clone(c);
				// this is kind of approx. as the predicate may return very quickly 
				// or end up doing an all separate query on its own
				x.predicateCost = 5;
				return x;
			}
		});
		instance.put(BFSCondition.class, new ConditionToQuery()
        {
			public HGQuery getQuery(HyperGraph graph, HGQueryCondition c)
			{
				BFSCondition cc = (BFSCondition)c;
				return new TraversalBasedQuery(cc.getTraversal(graph), TraversalBasedQuery.ReturnType.targets);
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				QueryMetaData x = QueryMetaData.MISTERY.clone(c);
				x.predicateCost = -1;
				x.predicateOnly = false;
				return x;
			}
		});
		instance.put(DFSCondition.class, new ConditionToQuery()
        {
			public HGQuery getQuery(HyperGraph graph, HGQueryCondition c)
			{
				DFSCondition cc = (DFSCondition)c;
				return new TraversalBasedQuery(cc.getTraversal(graph), TraversalBasedQuery.ReturnType.targets);
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				return QueryMetaData.MISTERY.clone(c);
			}
		});			
		instance.put(SubsumedCondition.class, new ConditionToQuery()
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
				QueryMetaData x = QueryMetaData.MISTERY.clone(c);
				// this is kind of approx. as the predicate may return very quickly 
				// or end up doing an all separate query on its own
				x.predicateCost = 5;
				return x;
			}
		});		
		instance.put(OrderedLinkCondition.class, new ConditionToQuery()
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
																	 new SortedIntersectionResult());
					while (i.hasNext())
						result = new IntersectionQuery(i.next(), 
													   result,
													   new SortedIntersectionResult());
					// the following will find all links (unordered) with the given target
					// set and then filter to insure that the targets are properly ordered.
					return new PredicateBasedFilter(hg, result, lc);				
				}				
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				QueryMetaData qmd;
				if (((OrderedLinkCondition)c).targets().length == 0)					
					qmd = QueryMetaData.EMPTY.clone(c);
				else
					qmd = QueryMetaData.MISTERY.clone(c);
				qmd.predicateCost = 0.5;
				return qmd;
			}
		});		
		instance.put(MapCondition.class, new ConditionToQuery()
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
				QueryMetaData qmd = instance.get(mc.getCondition().getClass()).getMetaData(hg, mc.getCondition());
				qmd.randomAccess = false;
				qmd.ordered = false; // should we have an order preserving mapping?
				qmd.predicateCost = -1;
				qmd.cond = c;
				return qmd;
			}
		});
		instance.put(IndexCondition.class, new ConditionToQuery()
        {
			public HGQuery<?> getQuery(HyperGraph graph, HGQueryCondition c)
			{				
				IndexCondition ic = (IndexCondition)c;
				if (ic.getOperator() != ComparisonOperator.EQ)
				{
					if (! (ic.getIndex() instanceof HGSortIndex))
						throw new IllegalArgumentException("Invalid operator : " + ic.getOperator() + 
								" for index " + ic.getIndex() + " and key " + ic.getKey());
					else
						return new IndexBasedQuery(ic.getIndex(), ic.getKey(), ic.getOperator());					
				}
				else
					return new IndexBasedQuery(ic.getIndex(), ic.getKey());
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				return QueryMetaData.ORACCESS.clone(c);
			}
		});				
		instance.put(IndexedPartCondition.class, new ConditionToQuery()
        {
			public HGQuery<?> getQuery(HyperGraph hg, HGQueryCondition c)
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
				return QueryMetaData.ORACCESS.clone(c);
			}
		});		
		instance.put(And.class, new AndToQuery());
		instance.put(Or.class, new ConditionToQuery()
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
				QueryMetaData x = QueryMetaData.ORACCESS.clone(c); // TODO - this is only true in the worst case!
				boolean ispredicate = true;
				x.predicateCost = 0;
				for (HGQueryCondition sub : ((Or)c))
				{
					if (! (sub instanceof HGAtomPredicate))
						ispredicate = false;					
					ConditionToQuery transformer = instance.get(sub.getClass());
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
		instance.put(AtomPartCondition.class, new ConditionToQuery()
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
				QueryMetaData qmd = QueryMetaData.MISTERY.clone(c);
				qmd.predicateCost = 1.5;
				qmd.predicateOnly = true;
				return qmd;
			}			
		});
		instance.put(AtomProjectionCondition.class, new ConditionToQuery()
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
						HGQuery q = toQuery(graph, bc); 
						return new ProjectionAtomResultSet(graph, 
														   q.execute(),
														   apc.getDimensionPath(), 
														   baseType);
					}
				};
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
				QueryMetaData x = QueryMetaData.MISTERY.clone(c);
				x.predicateCost = 100; // this is because they will be a query involved in the predicate etc...expensive stuff
				return x;
			}
		});
		instance.put(SubgraphMemberCondition.class, new ConditionToQuery()
		{
			public QueryMetaData getMetaData(HyperGraph graph,
											 HGQueryCondition condition)
			{
				return QueryMetaData.ORACCESS.clone(condition);
			}

			public HGQuery<?> getQuery(final HyperGraph graph,
									   final HGQueryCondition condition)
			{
				return new HGQuery<HGPersistentHandle>()
				{
					public HGSearchResult<HGPersistentHandle> execute() 
					{
						HGIndex<HGPersistentHandle, HGPersistentHandle> idx = HGSubgraph.getIndex(graph);
						return idx.find(((SubgraphMemberCondition)condition).getSubgraphHandle().getPersistent());
					}
				};				
			}			
		});
        instance.put(SubgraphContainsCondition.class, new ConditionToQuery()
        {
            public QueryMetaData getMetaData(HyperGraph graph,
                                             HGQueryCondition condition)
            {
                return QueryMetaData.ORACCESS.clone(condition);
            }

            public HGQuery<?> getQuery(final HyperGraph graph,
                                       final HGQueryCondition condition)
            {
                return new HGQuery<HGPersistentHandle>()
                {
                    public HGSearchResult<HGPersistentHandle> execute() 
                    {
                        HGIndex<HGPersistentHandle, HGPersistentHandle> idx = HGSubgraph.getReverseIndex(graph);
                        return idx.find(((SubgraphContainsCondition)condition).getAtom().getPersistent());
                    }
                };              
            }           
        });		
	}
	
	public static ToQueryMap getInstance() { return instance; }
	
	static <ResultType> HGQuery<ResultType> toQuery(HyperGraph hg, HGQueryCondition condition)
	{
		ConditionToQuery transformer = (ConditionToQuery)instance.get(condition.getClass());
		if (transformer == null)
			throw new HGException("The query condition '" + condition + 
					"' could not be translated to an executable query either because it is not specific enough. " +
					"Please try to contrain the query futher, for example by specifying the atom's types or " +
					"incidence sets or some indexed property value.");
		else
		{
			HGQuery<ResultType> q = (HGQuery<ResultType>)transformer.getQuery(hg, condition);
			q.setHyperGraph(hg);
			return q;
		}
	}

	static QueryMetaData toMetaData(HyperGraph hg, HGQueryCondition condition)
	{
		ConditionToQuery transformer = (ConditionToQuery)instance.get(condition.getClass());
		if (transformer == null)
			throw new HGException("The query condition '" + condition + 
					"' could not be translated to an executable query because it is not specific enough. " +
					"Please try to contrain the query futher, for example by specifying the atom's types or " +
					"incidence sets or some indexed property value.");
		else
			return transformer.getMetaData(hg, condition);
	}	
}