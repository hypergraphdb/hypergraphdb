/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.indexing.HGIndexer;
import org.hypergraphdb.query.*;
import org.hypergraphdb.query.cond2qry.ExpressionBasedQuery;
import org.hypergraphdb.query.impl.DerefMapping;
import org.hypergraphdb.query.impl.LinkProjectionMapping;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.type.HGCompositeType;
import org.hypergraphdb.type.HGTypedValue;
import org.hypergraphdb.type.TypeUtils;
import org.hypergraphdb.util.CompositeMapping;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Mapping;

/**
 * <p>
 * The <code>HGQuery</code> class represents an arbitrary query to the hypergraph
 * database. Queries can be defined either by using the query language, or they
 * can be built directly from query condition instances.
 * </p>
 * 
 * @author Borislav Iordanov
 */
@SuppressWarnings("unchecked")
public abstract class HGQuery<SearchResult> implements HGGraphHolder
{  	
	protected HyperGraph graph;	
	
	public final static HGQuery<Object> NOP = new HGQuery<Object>()
	{
		public HGSearchResult<Object> execute() { return (HGSearchResult<Object>)HGSearchResult.EMPTY; }
	};
	
/*	public static HGQuery make(HyperGraph hg, String expression)
	{
		return new ExpressionBasedQuery(hg, expression);
	} */

	public static <SearchResult> HGQuery<SearchResult> make(HyperGraph hg, HGQueryCondition condition)
	{
		return (HGQuery<SearchResult>)new ExpressionBasedQuery(hg, condition);
	}
	
	public HyperGraph getHyperGraph()
	{
		return graph;
	}

	public void setHyperGraph(HyperGraph graph)
	{
		this.graph = graph;
	}

	public abstract HGSearchResult<SearchResult> execute();
    
    /**
     * <p>
     * This class serves as a namespace to a set of syntactically concise functions
     * for constructing HyperGraph query conditions and performing HyperGraph queries. 
     * With a Java 5+ compiler, you can import the class into your file's namespace 
     * and build HG condition with a much
     * simpler syntax than constructing the expression tree explicitly. For example:
     * </p>
     * 
     * <p>
     * <code>
     * <pre>
     * ...other imports ...
     * import org.hypergraphdb.HGQuery.hg;
     * 
     * public void f(HyperGraph graph)
     * {
     *     // find all link with weight > 1.5
     *     HGSearchResult rs = graph.find(hg.and(hg.type(MyLink.class), hg.gt("weight", 1.5)));    
     * }
     * </pre>
     * </code>
     * </p>
     * <p>
     * or, even more concisely, if there is no naming conflict, import all the function names
     * directly:
     * </p>
     * <p>
     * <code>
     * <pre>
     * ...other imports ...
     * import org.hypergraphdb.HGQuery.hg.*;
     * 
     * public void f(HyperGraph graph)
     * {
     *     // find all link with weight > 1.5 
     *     HGSearchResult rs = graph.find(and(type(MyLink.class), gt("weight", 1.5)));    
     * }
     * </pre>
     * </code>
     * </p>
     *   
     * <p>
     * In addition, several methods names <code>get*</code> and <code>find*</code> will
     * execute queries and return their results in the form of individual objects or Java
     * collections, thus saving you from the burden of paying attention to always properly
     * close result sets.
     * </p>
     * 
     * @author Borislav Iordanov
     *
     */
    public static final class hg
    {
        /**
         * <p>
         * Return <code>assertAtom(graph, instance, false)</code>.
         * </p>    
         */                
        public static HGHandle assertAtom(final HyperGraph graph, final Object instance)
        {
            return assertAtom(graph, instance, false);
        }
        
        /**
         * <p>
         * Return the atom handle if <code>instance</code> is already a loaded atom in the cache.
         * Otherwise, get the default type corresponding to <code>instance.getClass</code> and
         * return <code>assertAtom(graph, instance, type, ignoreValue)</code>.
         * </p>
         */        
        public static HGHandle assertAtom(final HyperGraph graph, final Object instance, boolean ignoreValue)
        {
            if (instance == null)
                throw new NullPointerException("Can't assert a null atom without specifying a type for it.");
            HGHandle existing = graph.getHandle(instance);
            if (existing != null)
                return existing;
            HGHandle typeHandle = graph.getTypeSystem().getTypeHandle(instance.getClass());
            return assertAtomImpl(graph, instance, typeHandle, ignoreValue);            
        }
        
        /**
         * <p>
         * Return <code>assertAtom(graph, instance, type, false)</code>.
         * </p>    
         */        
        public static HGHandle assertAtom(final HyperGraph graph, final Object instance, final HGHandle type)
        {
            return assertAtom(graph, instance, type, false);
        }
        
        /**
         * <p>
         * Add a new atom to the specified graph only if it is not already there. An object
         * is considered in the graph if:
         * 
         * <ul>
         * <li>It is associated with a {@link HGHandle} in the graph's cache; or</li>
         * <li>A lookup for an
         * atom with the specified type, value and target set returns
         * a non-empty set. In this case the first atom from the query result is returned.</li>
         * </ul>
         * </p>
         * 
         * @param graph The {@link HyperGraph} database instance.
         * @param instance The object to be <em>asserted</em> as an atom.
         * @param type The type of the atom.
         * @param ignoreValue Whether to ignore the atom value while performing the lookup. The value 
         * of the atom is compared with <code>Object.equals</code> 
         * so it is important that that method be implemented properly for your Java object. The default 
         * implementation in the standard <code>Object</code> class won't work. If the object class
         * doesn't implement this method and there are no other way to uniquely identify the atom's
         * value known to HyperGraph, then please use the {@link addUnique} method to specify a condition
         * for lookup. If the value is not important, which is frequently the case with Java-type links,
         * then set this parameter to <code>true</code>. 
         * @return The {@link HGHandle} of the asserted atom (either existing or newly added).
         */         
        public static HGHandle assertAtom(final HyperGraph graph, 
                                          final Object instance, 
                                          final HGHandle type, 
                                          final boolean ignoreValue)
        {
            if (instance != null)
            {
                HGHandle existing = graph.getHandle(instance);
                if (existing != null)
                {
                    if (graph.getType(existing).equals(type))
                        return existing;
                }                
            }
            return assertAtomImpl(graph, instance, type, ignoreValue);
        }
        
        
        private static HGHandle assertAtomImpl(final HyperGraph graph, final Object instance, final HGHandle type, final boolean ignoreValue)
        {
            return graph.getTransactionManager().transact(new Callable<HGHandle>() {
                public HGHandle call()
                {
                    And and = new And();
                    and.add(type(type));
                    if (instance instanceof HGLink)
                        and.add(orderedLink(HGUtils.toHandleArray((HGLink)instance)));
                    List<HGIndexer> indexers = graph.getIndexManager().getIndexersForType(type);
                	boolean skipValue = false;
                    if (indexers != null)
                    {
                    	HashSet<String> dimensions = new HashSet<String>();
                    	for (HGIndexer idx : indexers)
	                    {
	                    	if (idx instanceof ByPartIndexer)
	                    	{
	                    		ByPartIndexer byPart = (ByPartIndexer)idx;
	                    		HGTypedValue prop = TypeUtils.project(graph, type, instance, byPart.getDimensionPath(), true);
	                    		and.add(new AtomPartCondition(byPart.getDimensionPath(), prop.getValue()));
	                    		if (byPart.getDimensionPath().length == 1)
	                    			dimensions.add(byPart.getDimensionPath()[0]);
	                    	}
	                    }
                    	
                    	// if we have a complex type where all dimensions are covered by indices,we don't need
                    	// to include the full atom value in the condition
                    	HGAtomType T = graph.get(type);
                    	if (!dimensions.isEmpty() && T instanceof HGCompositeType)
                    	{
                    		for (Iterator<String> dimIter = ((HGCompositeType)T).getDimensionNames(); dimIter.hasNext(); )
                    			dimensions.remove(dimIter.next());
                    		if (dimensions.isEmpty())
                    			skipValue = true;
                    	}
                    }
                    if (!ignoreValue && !skipValue)
                        and.add(eq(instance));                    
                    HGHandle h = findOne(graph, and);
                    return h == null ?  graph.add(instance, type) : h;                    
                }
            });            
        }
        
        public static HGQueryCondition guessUniquenessCondition(final HyperGraph graph, final Object instance)
        {
        	if (instance == null)
        		return null;
            HGHandle type = graph.getTypeSystem().getTypeHandle(instance.getClass());
            And and = new And();
            and.add(type(type));
            and.add(eq(instance));
            if (instance instanceof HGLink)
                and.add(orderedLink(HGUtils.toHandleArray((HGLink)instance)));
            List<HGIndexer> indexers = graph.getIndexManager().getIndexersForType(type);
            if (indexers != null) for (HGIndexer idx : indexers)
            {
            	if (idx instanceof ByPartIndexer)
            	{
            		ByPartIndexer byPart = (ByPartIndexer)idx;
            		Object prop = TypeUtils.project(graph, type, instance, byPart.getDimensionPath(), true);
            		and.add(new AtomPartCondition(byPart.getDimensionPath(), prop));
            	}
            }
            return and;
        }
        
    	/**
    	 * <p>
    	 * Add the given instance as an atom in the graph iff no atoms
    	 * match the passed in {@link HGQueryCondition}
    	 * </p>
    	 */
        public static HGHandle addUnique(final HyperGraph graph, 
        								 final Object instance, 
        								 final HGQueryCondition condition)
        {
        	return graph.getTransactionManager().transact(new Callable<HGHandle>() {
        		public HGHandle call()
        		{
                    HGHandle h = findOne(graph, condition);
                    return h == null ?  graph.add(instance) : h;        			
        		}
        	});
        }
        
        public static HGHandle addUnique(final HyperGraph graph, 
        								 final Object instance, 
        								 final HGHandle typeHandle, 
        								 final HGQueryCondition condition)
        {
        	return graph.getTransactionManager().transact(new Callable<HGHandle>() {
        		public HGHandle call()
        		{		        	
		            HGHandle h = findOne(graph, and(type(typeHandle), condition));
		            return h == null ?  graph.add(instance, typeHandle) : h;
        		}
        	});		            
        }
        
        public static HGHandle addUnique(HyperGraph graph, 
        								 Object instance, 
        								 Class javaClass, 
        								 HGQueryCondition condition)
        {            
            return addUnique(graph, instance, graph.getTypeSystem().getTypeHandle(javaClass), condition);
        }        
        
        public static AtomTypeCondition type(HGHandle h) { return new AtomTypeCondition(h); }
        public static AtomTypeCondition type(Class<?> c) { return new AtomTypeCondition(c); }
        public static TypePlusCondition typePlus(HGHandle h) { return new TypePlusCondition(h); }
        public static TypePlusCondition typePlus(Class<?> c) { return new TypePlusCondition(c); }        
        public static SubsumesCondition subsumes(HGHandle h) { return new SubsumesCondition(h); }
        public static SubsumedCondition subsumed(HGHandle h) { return new SubsumedCondition(h); }
        
        public static And and(HGQueryCondition...clauses)
        {
            And and = new And();
            for (HGQueryCondition x:clauses)
                and.add(x); 
            return and;            
        }
        public static Or or(HGQueryCondition...clauses)
        {
            Or or = new Or();
            for (HGQueryCondition x:clauses)
                or.add(x);
            return or;            
        }
        public static Not not(HGAtomPredicate c) { return new Not(c); }
        
        public static TargetCondition target(HGHandle h) { return new TargetCondition(h); }
        public static IncidentCondition incident(HGHandle h) { return new IncidentCondition(h); }
        public static LinkCondition link(HGHandle...h) { return new LinkCondition(h); }
        public static LinkCondition link(Collection<HGHandle> C) { return new LinkCondition(C); }
        public static OrderedLinkCondition orderedLink(HGHandle...h) { return new OrderedLinkCondition(h); }
        public static OrderedLinkCondition orderedLink(List<HGHandle> L) { return new OrderedLinkCondition(L); }
        public static ArityCondition arity(int i) { return new ArityCondition(i); }
        public static DisconnectedPredicate disconnected() { return new DisconnectedPredicate(); }
        
        public static AtomValueCondition value(Object value, ComparisonOperator op) { return new AtomValueCondition(value, op); }
        public static AtomValueCondition eq(Object x) { return value(x, ComparisonOperator.EQ); }
        public static AtomValueCondition lt(Object x) { return value(x, ComparisonOperator.LT); }        
        public static AtomValueCondition gt(Object x) { return value(x, ComparisonOperator.GT); }
        public static AtomValueCondition lte(Object x) { return value(x, ComparisonOperator.LTE); }
        public static AtomValueCondition gte(Object x) { return value(x, ComparisonOperator.GTE); }
        
        public static AtomPartCondition part(String path, Object value, ComparisonOperator op) {  return new AtomPartCondition(path.split("\\."), value, op); }
        public static AtomPartCondition eq(String path, Object x) { return part(path, x, ComparisonOperator.EQ); }
        public static AtomPartCondition lt(String path, Object x) { return part(path, x, ComparisonOperator.LT); }        
        public static AtomPartCondition gt(String path, Object x) { return part(path, x, ComparisonOperator.GT); }
        public static AtomPartCondition lte(String path, Object x) { return part(path, x, ComparisonOperator.LTE); }
        public static AtomPartCondition gte(String path, Object x) { return part(path, x, ComparisonOperator.GTE); }
        
        public static HGQueryCondition apply(Mapping<?,?> m, HGQueryCondition c) { return new MapCondition(c, m); }
        public static Mapping<HGLink, HGHandle> linkProjection(int targetPosition) { return new LinkProjectionMapping(targetPosition); }
        public static Mapping<HGHandle, Object> deref(HyperGraph graph) { return new DerefMapping(graph); }
        public static Mapping<HGHandle,HGHandle> targetAt(HyperGraph graph, int targetPosition) { return new CompositeMapping(deref(graph), linkProjection(targetPosition)); }
        public static HGQueryCondition all() { return new AnyAtomCondition(); }
     
        // traversals
        public static BFSCondition bfs(HGHandle start) { return new BFSCondition(start); }
        public static BFSCondition bfs(HGHandle start, 
        							   HGAtomPredicate lp, 
        							   HGAtomPredicate sp) 
        { 
        	BFSCondition c = new BFSCondition(start);
        	c.setLinkPredicate(lp);
        	c.setSiblingPredicate(sp);
        	return c;
        }
        public static BFSCondition bfs(HGHandle start, 
									   HGAtomPredicate lp, 
									   HGAtomPredicate sp,
									   boolean returnPreceeding,
									   boolean returnSucceeding) 
		{ 
			BFSCondition c = new BFSCondition(start);
			c.setLinkPredicate(lp);
			c.setSiblingPredicate(sp);
			c.setReturnPreceeding(returnPreceeding);
			c.setReturnSucceeding(returnSucceeding);
			return c;
		}        
        public static DFSCondition dfs(HGHandle start) { return new DFSCondition(start); }
        public static DFSCondition dfs(HGHandle start, 
        							   HGAtomPredicate lp, 
        							   HGAtomPredicate sp) 
        { 
        	DFSCondition c = new DFSCondition(start);
        	c.setLinkPredicate(lp);
        	c.setSiblingPredicate(sp);
        	return c;
        }
        public static DFSCondition dfs(HGHandle start, 
									   HGAtomPredicate lp, 
									   HGAtomPredicate sp,
									   boolean returnPreceeding,
									   boolean returnSucceeding) 
		{ 
			DFSCondition c = new DFSCondition(start);
			c.setLinkPredicate(lp);
			c.setSiblingPredicate(sp);
			c.setReturnPreceeding(returnPreceeding);
			c.setReturnSucceeding(returnSucceeding);
			return c;
		}
        
        static final HGHandle the_any_handle = new HGHandle(){};
        public static HGHandle anyHandle() { return the_any_handle; }
        
        /**
         * <p>
         * Count the number of atoms that match the query condition parameter. Retrieving
         * the count might require performing the actual query traversing the result set.
         * In cases where the condition is simple and the count is available directly from
         * an index, it is returned right away. Otherwise, the operation may be expensive
         * when the result set matching <code>cond</code> is large or it takes time
         * to obtain it. 
         * </p>
         * 
         * @param graph The HyperGraph against which the counting is performed.
         * @param cond The condition specifying the result set.
         * @return The number of atoms satisfying the query condition.
         */
        public static long count(final HyperGraph graph, final HGQueryCondition cond) 
        { 
        	return graph.getTransactionManager().ensureTransaction(new Callable<Long>() {
        	public Long call()
        	{
            	ResultSizeEstimation.Counter counter = ResultSizeEstimation.countersMap.get(cond.getClass());
            	if (counter == null)
            		return ResultSizeEstimation.countResultSet(graph, cond);
            	else
            		return counter.count(graph, cond);        		
        	}
        	});
        }
        
        /**
         * <p>
         * Count the result set from executing the given query. The query is executed and
         * the result set scanned completely.
         * </p>
         * 
         * @param query
         * @return
         */
        public static long count(final HGQuery<?> query)
        {
        	return query.getHyperGraph().getTransactionManager().ensureTransaction(new Callable<Long>() {
            	public Long call()
            	{
                	return ResultSizeEstimation.countResultSet(query);
            	}
            	});        	
        }
        
        //
        // Querying section.
        //
    	/**
    	 * <p>
    	 * Run a query based on the passed in condition. If the result set is not
    	 * empty, get and return the atom instance of the first element. Otherwise,
    	 * return <code>null</code>.
    	 * </p>
    	 * 
    	 * @param graph The HyperGraph database to query.
    	 * @param condition The query condition.
    	 * @return
    	 */
    	public static <T> T getOne(HyperGraph graph, HGQueryCondition condition)
    	{
    		HGHandle h = findOne(graph, condition);
    		return h == null ? null : (T)graph.get(h);
    	}
    	
    	/**
    	 * <p>
    	 * Run a query based on the passed in condition. If the result set is not
    	 * empty, return the first <code>HGHandle</code> element. Otherwise,
    	 * return <code>null</code>.
    	 * </p>
    	 * 
    	 * @param graph
    	 * @param condition
    	 * @return
    	 */
    	public static <T> T findOne(final HyperGraph graph, final HGQueryCondition condition)
    	{
        	return graph.getTransactionManager().ensureTransaction(new Callable<T>() {
            	public T call()
            	{
            		HGSearchResult<T> rs = null;
            		try
            		{
            			rs = graph.find(condition);
            			if (rs.hasNext())
            				return rs.next();
            			else
            				return null;
            		}
            		finally
            		{
            			if (rs != null) rs.close();
            		}    		
            	}
            	});
    	}
    	
    	/**
    	 * <p>
    	 * Run a query based on the specified condition and put all <code>HGHandle</code>s
    	 * from the result set into a <code>java.util.List</code>.
    	 * </p>
    	 *  
    	 * @param graph
    	 * @param condition
    	 * @return
    	 */
    	public static <T> List<T> findAll(final HyperGraph graph, final HGQueryCondition condition)
    	{
    		final ArrayList<T> result = new ArrayList<T>();
    		HGQuery<T> query = HGQuery.make(graph, condition);
    		HGUtils.queryBatchProcess(query,
    				new Mapping<T, Boolean>()
    				{
    					public Boolean eval(T x) { result.add(x); return Boolean.TRUE; }
    				},
    				500,
					null,
					1);
    		return result;    		
//        	return graph.getTransactionManager().ensureTransaction(new Callable<List<T>>() {
//            	public List<T> call()
//            	{
//            		ArrayList<T> result = new ArrayList<T>();
//            		HGSearchResult<T> rs = null;
//            		try
//            		{
//            			rs = graph.find(condition);
//            			while (rs.hasNext())
//            				result.add(rs.next());
//            			return result;
//            		}
//            		finally
//            		{
//            			if (rs != null) rs.close();
//            		}	
//            	}
//            	});    		      		
    	}

    	/**
    	 * <p>
    	 * Run a query based on the specified condition and put all atom instances
    	 * from the result set into a <code>java.util.List</code>.
    	 * </p>
    	 *  
    	 * @param graph
    	 * @param condition
    	 * @return
    	 */
    	public static <T> List<T> getAll(final HyperGraph graph, final HGQueryCondition condition)
    	{
    		final ArrayList<T> result = new ArrayList<T>();
    		HGQuery<HGHandle> query = HGQuery.make(graph, condition);
    		HGUtils.queryBatchProcess(query,
    				new Mapping<HGHandle, Boolean>()
    				{
    					public Boolean eval(HGHandle x) { result.add((T)graph.get(x)); return Boolean.TRUE; }
    				},
    				500,
					null,
					1);
    		return result;
//        	return graph.getTransactionManager().ensureTransaction(new Callable<List<T>>() {
//            	public List<T> call()
//            	{
//            		ArrayList<Object> result = new ArrayList<Object>();
//            		HGSearchResult<HGHandle> rs = null;
//            		try
//            		{
//            			rs = graph.find(condition);
//            			while (rs.hasNext())
//            				result.add(graph.get(rs.next()));
//            			return (List<T>)result;
//            		}
//            		finally
//            		{
//            			if (rs != null) rs.close();
//            		}	
//            	}
//            	});    		     		
    	}
    	
    	public static <T> List<T> findAll(final HGQuery<T> query)
    	{
    		final ArrayList<T> result = new ArrayList<T>();
    		HGUtils.queryBatchProcess(query,
    				new Mapping<T, Boolean>()
    				{
    					public Boolean eval(T x) { result.add(x); return Boolean.TRUE; }
    				},
    				500,
					null,
					1);
    		return result;    		
//        	return query.getHyperGraph().getTransactionManager().ensureTransaction(new Callable<List<T>>() {
//            	public List<T> call()
//            	{
//                	
//            		ArrayList<T> result = new ArrayList<T>();
//            		HGSearchResult<T> rs = null;
//            		try
//            		{
//            			rs = query.execute();
//            			while (rs.hasNext())
//            				result.add(rs.next());
//            			return result;
//            		}
//            		finally
//            		{
//            			if (rs != null) rs.close();
//            		}      	
//            	}
//            	});		
    	}    	
    }
}
