package org.hypergraphdb.query.cond2qry;

import org.hypergraphdb.HGException
;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGOrderedSearchable;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGSearchable;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.query.AtomTypeCondition;
import org.hypergraphdb.query.AtomValueCondition;
import org.hypergraphdb.query.ComparisonOperator;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.TypedValueCondition;
import org.hypergraphdb.query.impl.IndexBasedQuery;
import org.hypergraphdb.query.impl.PipeQuery;
import org.hypergraphdb.query.impl.PredicateBasedFilter;
import org.hypergraphdb.query.impl.SearchableBasedQuery;
import org.hypergraphdb.type.HGAtomType;

public class TypedValueToQuery implements ConditionToQuery
{
	static class Query<T> extends HGQuery<T>
	{
		TypedValueCondition vc;
		HyperGraph graph;
		
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public HGSearchResult<T> execute()
		{
			//
			// TODO: how to we deal with null values? For the String
			// primitive type at least, nulls are possible.
			//
			
			HGHandle typeHandle = vc.getTypeHandle(graph);
			HGAtomType type = graph.get(typeHandle);
			if (type == null)
				throw new HGException("Cannot search by value " + vc.getValue()
						+ " of unknown HGAtomType with handle " + typeHandle);
			Object value = vc.getValue();
			if (type instanceof HGSearchable
					&& vc.getOperator() == ComparisonOperator.EQ
					|| type instanceof HGOrderedSearchable)
				//
				// Find value handle by value and pipe into 'indexByValue' search,
				// then filter
				// by the expected type to make sure that it matches the actual type
				// of the atoms
				// so far obtained.
				//
				// Note: not sure we need the predicate AtomTypeCondition here since
				// we are using the index of the type itself. Do we really have situations
				// where a parent type is providing the indexing ? We could be optimizing
				// this by checking if indeed the index is in a parent type and we are
				// looking for a sub-type. Though type checking is fast...
				//
				return new PredicateBasedFilter(
					graph, 
					new PipeQuery(
						new SearchableBasedQuery((HGSearchable<?, ?>) type, 
												 value,
												 vc.getOperator()), 
						new SearchableBasedQuery(graph.getIndexManager().getIndexByValue(), 
												 null,
												 ComparisonOperator.EQ)), 
						new AtomTypeCondition(typeHandle)).execute();
			else
				// else, we need to scan all atoms of the given type
				return new PredicateBasedFilter(graph, 
												new IndexBasedQuery(graph.getIndexManager().getIndexByType(),
																    typeHandle.getPersistent()),
												new AtomValueCondition(vc.getValue(), 
																	   vc.getOperator())).execute();
		}
	}
	
	public HGQuery<?> getQuery(HyperGraph graph, HGQueryCondition c)
	{
		Query<?> q = new Query<Object>();
		q.vc = (TypedValueCondition) c;
		q.graph = graph;
		return q;
	}

	public QueryMetaData getMetaData(HyperGraph graph, HGQueryCondition c)
	{
		TypedValueCondition vc = (TypedValueCondition) c;
		HGHandle typeHandle = vc.getTypeHandle(graph);
		HGAtomType type = graph.get(typeHandle);
		if (type == null)
			throw new HGException("Cannot search by value"
					+ " of unknown HGAtomType with handle " + typeHandle);
		QueryMetaData qmd;
		if (!hg.isVar(vc.getTypeReference()) && 
			(type instanceof HGSearchable && vc.getOperator() == ComparisonOperator.EQ
			  || type instanceof HGOrderedSearchable))
		{
			qmd = QueryMetaData.ORDERED.clone(c);
		}
		else
		{
			qmd = QueryMetaData.MISTERY.clone(c);
		}
		qmd.predicateCost = 2.5;
		return qmd;
	}
}