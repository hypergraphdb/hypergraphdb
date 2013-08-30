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
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.algorithms.DefaultALGenerator;
import org.hypergraphdb.algorithms.HGBreadthFirstTraversal;
import org.hypergraphdb.atom.HGSubgraph;
import org.hypergraphdb.atom.HGSubsumes;
import org.hypergraphdb.indexing.DirectValueIndexer;
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
import org.hypergraphdb.query.IsCondition;
import org.hypergraphdb.query.LinkCondition;
import org.hypergraphdb.query.MapCondition;
import org.hypergraphdb.query.Nothing;
import org.hypergraphdb.query.Or;
import org.hypergraphdb.query.OrderedLinkCondition;
import org.hypergraphdb.query.PositionedIncidentCondition;
import org.hypergraphdb.query.QueryCompile;
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
import org.hypergraphdb.util.ArrayBasedSet;
import org.hypergraphdb.util.Pair;
import org.hypergraphdb.util.Ref;
import org.hypergraphdb.util.RefResolver;

@SuppressWarnings("unchecked")
public class ToQueryMap extends HashMap<Class<?>, ConditionToQuery<?>> implements RefResolver<Class<? extends HGQueryCondition>, ConditionToQuery<?>>
{
	private static final long serialVersionUID = -1;
	protected static final ToQueryMap instance = new ToQueryMap();
	
	public static ToQueryMap getInstance() { return instance; }
	
	static
	{
		instance.put(Nothing.class, new ConditionToQuery()
		{ 
			public HGQuery<?> getQuery(HyperGraph graph, HGQueryCondition c)
			{
				return HGQuery.NOP();                     
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
			
			public HGQuery<?> getQuery(final HyperGraph graph, HGQueryCondition c)
			{				
				final AtomTypeCondition ac = (AtomTypeCondition)c;
				if (!hg.isVar(ac.getTypeReference()) && ac.getTypeHandle() == null &&
						graph.getTypeSystem().getTypeHandleIfDefined(ac.getJavaClass()) == null)
					return HGQuery.NOP();
				return new SearchableBasedQuery(graph.getIndexManager().getIndexByType(), 
												new Ref<HGPersistentHandle>() {
												public HGPersistentHandle get()
												{
													return ac.getTypeHandle(graph).getPersistent();
												}},
												ComparisonOperator.EQ);									
			}
			public QueryMetaData getMetaData(HyperGraph graph, HGQueryCondition c)
			{
				AtomTypeCondition ac = (AtomTypeCondition)c;
				QueryMetaData x = QueryMetaData.ORACCESS.clone(c);
				x.predicateCost = 1;
				if (hg.isVar(ac.getTypeReference()))
				{
					// TODO: maybe we can get better estimates here if we collect some global
					// database statistics, e.g. in the HGStats bean
//					x.sizeExpected = ...
//					x.sizeLB = ...
//					x.sizeUB = ..
				}
				else
				{
				    HGHandle typeHandle = ac.typeHandleIfAvailable(graph);
					x.sizeExpected = 
							x.sizeLB = 
							x.sizeUB = (typeHandle == null) ? 0 : 
							    graph.getIndexManager().getIndexByType().count(typeHandle.getPersistent());
				}
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
                return QueryCompile.toMetaData(hg, orCondition); //instance.get(Or.class).getMetaData(hg, orCondition);
			}			
		});		
		instance.put(TypedValueCondition.class, new TypedValueToQuery()); 
		instance.put(AtomValueCondition.class, new ConditionToQuery()
		{
			HGQuery<Object> makeQuery(HyperGraph graph, Object value, ComparisonOperator op)
			{
		        if (value == null)
		            throw new HGException("Search by null values is not supported yet.");
		        HGHandle type = graph.getTypeSystem().getTypeHandle(value);
	            Pair<HGHandle, HGIndex<Object, HGPersistentHandle>> p = 
	                    ExpressionBasedQuery.findIndex(graph, new DirectValueIndexer<Object>(type));
	            if (p != null)
	                return (HGQuery<Object>)instance.get(IndexCondition.class).
	                    getQuery(graph, new IndexCondition(p.getSecond(), value, op));
	            else
	                return (HGQuery<Object>)instance.get(TypedValueCondition.class).
						getQuery(graph, new TypedValueCondition(type, 
															 value, 
															 op));                		        							
			}
			
			public HGQuery<?> getQuery(final HyperGraph graph, final HGQueryCondition c)
			{
		        //
		        // TODO: how to we deal with null values? For the String
		        // primitive type at least, nulls are possible. We can only deal
						// with nulls if the type is know in which case a TypedValueCondition
						// must have been used.
		        //
		        final AtomValueCondition vc = (AtomValueCondition)c;
		        if (hg.isVar(vc.getValueReference()))
		        	return new HGQuery<Object>() {
		        		public HGSearchResult<Object> execute()
		        		{
		        			return makeQuery(graph, vc.getValue(), vc.getOperator()).execute();
		        		}
		        	};
		        else
		        	return makeQuery(graph, vc.getValue(), vc.getOperator());
			}
			
			public QueryMetaData getMetaData(HyperGraph graph, HGQueryCondition c)
			{
				AtomValueCondition vc = (AtomValueCondition)c;
				if (hg.isVar(vc.getValueReference()))
					return QueryMetaData.MISTERY.clone(c);
		        Object value = vc.getValue();
		        if (value == null)
		            throw new HGException("Search by null values is not supported yet.");
		        HGHandle type = graph.getTypeSystem().getTypeHandle(value);		                
				return instance.get(TypedValueCondition.class).
					getMetaData(graph, new TypedValueCondition(type, 
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
				QueryMetaData qmd = QueryMetaData.MISTERY.clone(c);
				qmd.predicateOnly = true;
				return qmd;
			}
		});
		instance.put(TargetCondition.class, new ConditionToQuery()
        {
			public HGQuery<HGHandle> getQuery(final HyperGraph graph, final HGQueryCondition c)
			{
				return new HGQuery<HGHandle>()
				{
					public HGSearchResult<HGHandle> execute()
					{
						final HGPersistentHandle handle = ((TargetCondition)c).getLink().getPersistent();						
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
		instance.put(PositionedIncidentCondition.class, new PositionedIncidentToQuery());
		instance.put(LinkCondition.class, new LinkToQuery());
		instance.put(SubsumesCondition.class, new ConditionToQuery()
		{
			public HGQuery getQuery(HyperGraph hg, HGQueryCondition c)
			{
				SubsumesCondition sc = (SubsumesCondition)c;
				Ref<HGHandle> startAtom = sc.getSpecificHandleReference();
				if (startAtom == null && sc.getSpecificValue() != null)
				{
					throw new HGException("Unable to translate 'subsumed' condition into a query, please a handle for the specific entity.");
				}				
				return new TraversalBasedQuery(
						new HGBreadthFirstTraversal(
							startAtom,
							new DefaultALGenerator(hg,
												   new AtomTypeCondition(hg.getTypeSystem().getTypeHandle(HGSubsumes.class)),
												   null,
												   false,
												   true,
												   true),
							Integer.MAX_VALUE
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
				Ref<HGHandle> startAtom = sc.getGeneralHandleReference();
				if (startAtom == null && sc.getGeneralValue() != null)
				{
					throw new HGException("Unable to translate 'subsumed' condition into a query, please use a valid handle for the general entity.");
				}
				return new TraversalBasedQuery(
						new HGBreadthFirstTraversal(
							startAtom,
							new DefaultALGenerator(hg,
												   new AtomTypeCondition(hg.getTypeSystem().getTypeHandle(HGSubsumes.class)),
												   null,
												   false,
												   true,
												   false),
							Integer.MAX_VALUE
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
			public HGQuery<?> getQuery(HyperGraph hg, HGQueryCondition c)
			{				
				OrderedLinkCondition lc = (OrderedLinkCondition)c;
				ArrayList<HGQuery<?>> L = new ArrayList<HGQuery<?>>();
				for (Ref<HGHandle> t : lc.targets())
					L.add(QueryCompile.translate(hg, new IncidentCondition(t)));
				if (L.isEmpty())
					return HGQuery.NOP();
				else if (L.size() == 1)
					return L.get(0);
				else
				{
					Iterator<HGQuery<?>> i = L.iterator();
					IntersectionQuery result = new IntersectionQuery(i.next(), 
																	 i.next(),
																	 new SortedIntersectionResult.Combiner<HGHandle>());
					while (i.hasNext())
						result = new IntersectionQuery(i.next(), 
													   result,
													   new SortedIntersectionResult.Combiner<HGHandle>());
					// the following will find all links (unordered) with the given target
					// set and then filter to insure that the targets are properly ordered.
					return new PredicateBasedFilter<HGHandle>(hg, result, lc);				
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
				HGQuery query = QueryCompile.translate(hg, mc.getCondition());
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
						return new IndexBasedQuery(ic.getIndex(), ic.getKeyReference(), ic.getOperator());					
				}
				else
					return new IndexBasedQuery(ic.getIndex(), ic.getKeyReference(), ComparisonOperator.EQ);
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
			    IndexCondition ic = (IndexCondition)c;
			    if (ic.getOperator() == ComparisonOperator.EQ)
			        return QueryMetaData.ORACCESS.clone(c);
			    else
			    {
			        QueryMetaData qmd = QueryMetaData.MISTERY.clone(c);
			        return qmd;
			    }
			}
		});		
		instance.put(IndexedPartCondition.class, new ConditionToQuery()
		{
			public HGQuery<?> getQuery(HyperGraph hg, HGQueryCondition c)
			{				
				IndexedPartCondition ip = (IndexedPartCondition)c;
				if (ip.getIndex() instanceof HGSortIndex)						
					return new IndexBasedQuery((HGSortIndex)ip.getIndex(), 
											   ip.getPartValueReference(), 
											   ip.getOperator());
				else
					return new IndexBasedQuery(ip.getIndex(), ip.getPartValueReference());
			}
			public QueryMetaData getMetaData(HyperGraph hg, HGQueryCondition c)
			{
			    IndexedPartCondition ip = (IndexedPartCondition)c;
                if (ip.getOperator() == ComparisonOperator.EQ)
                    return QueryMetaData.ORACCESS.clone(c);
                else
                {
                    QueryMetaData qmd = QueryMetaData.MISTERY.clone(c);
                    return qmd;
                }
			}
		});		
		instance.put(And.class, new AndToQuery());
		instance.put(Or.class, new OrToQuery());
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
						HGQuery q = QueryCompile.translate(graph, bc); 
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
		instance.put(IsCondition.class, new ConditionToQuery()
		{
			public QueryMetaData getMetaData(HyperGraph graph, HGQueryCondition condition)
			{
	        	return QueryMetaData.ORACCESS.clone(condition);
			}
		
			public HGQuery<?> getQuery(final HyperGraph graph, final HGQueryCondition condition)
			{
				return new HGQuery<HGPersistentHandle>()
				{
					public HGSearchResult<HGPersistentHandle> execute() 
					{
						HGHandle h = ((IsCondition)condition).getAtomHandle();					
						ArrayBasedSet A = new ArrayBasedSet(new HGHandle[] {h});
						return A.getSearchResult();
					}
				};
			}
		});
	}
	
	public ConditionToQuery<?> resolve(Class<? extends HGQueryCondition> cl)
	{
	    return get(cl);
	}
}