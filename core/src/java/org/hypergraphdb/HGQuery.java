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
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import org.hypergraphdb.atom.HGSubgraph;
import org.hypergraphdb.atom.HGSubsumes;
import org.hypergraphdb.atom.HGTypeStructuralInfo;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.indexing.HGIndexer;
import org.hypergraphdb.query.And;
import org.hypergraphdb.query.AnyAtomCondition;
import org.hypergraphdb.query.ArityCondition;
import org.hypergraphdb.query.AtomPartCondition;
import org.hypergraphdb.query.AtomPartRegExPredicate;
import org.hypergraphdb.query.AtomTypeCondition;
import org.hypergraphdb.query.AtomValueCondition;
import org.hypergraphdb.query.AtomValueRegExPredicate;
import org.hypergraphdb.query.BFSCondition;
import org.hypergraphdb.query.ComparisonOperator;
import org.hypergraphdb.query.DFSCondition;
import org.hypergraphdb.query.DisconnectedPredicate;
import org.hypergraphdb.query.HGAtomPredicate;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.IncidentCondition;
import org.hypergraphdb.query.IsCondition;
import org.hypergraphdb.query.LinkCondition;
import org.hypergraphdb.query.MapCondition;
import org.hypergraphdb.query.Not;
import org.hypergraphdb.query.Or;
import org.hypergraphdb.query.OrderedLinkCondition;
import org.hypergraphdb.query.PositionedIncidentCondition;
import org.hypergraphdb.query.SubgraphContainsCondition;
import org.hypergraphdb.query.SubgraphMemberCondition;
import org.hypergraphdb.query.SubsumedCondition;
import org.hypergraphdb.query.SubsumesCondition;
import org.hypergraphdb.query.TargetCondition;
import org.hypergraphdb.query.TypePlusCondition;
import org.hypergraphdb.query.cond2qry.ExpressionBasedQuery;
import org.hypergraphdb.query.impl.DerefMapping;
import org.hypergraphdb.query.impl.LinkProjectionMapping;
import org.hypergraphdb.transaction.HGTransactionConfig;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.type.HGCompositeType;
import org.hypergraphdb.type.HGTypedValue;
import org.hypergraphdb.type.TypeUtils;
import org.hypergraphdb.util.CompositeMapping;
import org.hypergraphdb.util.Constant;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Mapping;
import org.hypergraphdb.util.Ref;
import org.hypergraphdb.util.Var;
import org.hypergraphdb.util.VarContext;

/**
 * <p>
 * The <code>HGQuery</code> class represents an arbitrary query to the {@link HyperGraph}
 * database. Queries can be constructed out of {@link HGQueryCondition}s via the static {@link make}
 * method and then executed through the {@link execute} method. 
 * </p>
 * 
 * <p>
 * Queries can be parametarized by a set of variables. You can get and set variables for a
 * given query execution with the {@link #var(String)} and {@link #var(String, Object)} methods. 
 * Queries that used variables must be "pre-compiled" by first calling the {@link #make(Class, HyperGraph)}
 * method and then {@link #compile(HGQueryCondition)} with the query condition right away. Multiple
 * calls to {@link #compile(HGQueryCondition)} on the same <code>HGQuery</code> instance are not
 * advised. 
 * </p>
 * 
 * <p>
 * To ensure thread-safety and allow a single <code>HGQuery</code> instance to be pre-compiled and
 * reused from multiple threads, each thread of execution will get its own copy of the set of 
 * variables, cloned from the initial set that was created during the compilation of the query. 
 * This initial set of variables remains untouched by the <code>var</code> methods, but can be modified
 * by the analogous {@link #initialVar(String)} and {@link #initialVar(String, Object)}.   
 * </p>
 * 
 * @author Borislav Iordanov
 */
@SuppressWarnings("unchecked")
public abstract class HGQuery<SearchResult> implements HGGraphHolder
{  	
	protected HyperGraph graph;	
	
	protected VarContext ctx = VarContext.ctx();
	
    /**
     * <p>
     * Return the thread-local copy of a given variable. Use this method to 
     * obtain the value of a variable that will be used when executing this query
     * in the current thread.
     * </p>
     */
	public <T> Var<T> var(String name)
	{
		return (Var<T>)ctx.get(name);
	}
	
	/**
	 * <p>Modify the thread-local copy of a given variable. Use this method to 
     * modify the value of a variable that will be used when executing this query
     * in the current thread.
	 * </p>
	 */
	public <T> HGQuery<SearchResult> var(String name, T value)
	{
		ctx.get(name).set(value);
		return this;
	}

	public <T> T initialValue(String name)
	{
		return (T)ctx.getGlobalValue(name);
	}
	
	public <T> HGQuery<SearchResult> initialVar(String name, T value)
	{
		ctx.setGlobalValue(name, value);
		return this;
	}
	
	/**
	 * A query that return the empty result set.
	 */
	public static <ResultType> HGQuery<ResultType> NOP() {return new HGQuery<ResultType>()
	{
		public HGSearchResult<ResultType> execute() { return (HGSearchResult<ResultType>)HGSearchResult.EMPTY; }
	};};

	/**
	 * <p>
	 * Create a new query returning all atoms matching the given {@link HGQueryCondition}. The query condition
	 * can be constructed directly from classes implementing the {@link HGQueryCondition} interface or 
	 * by using  the expression building static methods of the {@link hg} class nested here.
	 * </p>
	 * 
	 * <p>
	 * The result of this method can be executed repeatedly. In place where performance is critical and
	 * a given query is executed often, it is beneficial to construct it before hand and reuse. The
	 * performance gain will depend on how complex the condition is. Constructing a query involves
	 * creating an execution plan based on indices etc. Therefore, a trivial condition that only
	 * constraints the type of the atoms doesn't take much time to translate into a query, while a more
	 * complex one involving some structural pattern will burn valuable extra cycles in building a query plan.  
	 * </p>
	 * 
	 * <p>
	 * <b>IMPORTANT NOTE:</b> This method does not support variables in the query condition. If your
	 * condition contains variables created by one of the {@link #var(String)} methods, please use
	 * the {@link #make(Class, HyperGraph)} to create the query and then invoke its {@link #compile(HGQueryCondition)}
	 * method to specify the condition.
	 * </p>
	 * 
	 * @param <SearchResult> The type of the return result. 
	 * @param graph The {@link HyperGraph} instance against which the query will be executed.
	 * @param condition The {@link HGQueryCondition} specifying which atoms to return from the graph.
	 * @return An executable <code>HGQuery</code> object.
	 */
	public static <SearchResult> HGQuery<SearchResult> make(HyperGraph graph, HGQueryCondition condition)
	{
		return (HGQuery<SearchResult>)new ExpressionBasedQuery<SearchResult>(graph, condition);
	}
	
	/**
	 * <p>
	 * Compile the passed in condition as <code>this</code> query object and return <code>this</code>. 
	 * </p>
	 */
	public HGQuery<SearchResult> compile(HGQueryCondition condition)
	{
		return this;		
	}

	/**
	 * <p>
	 * Create a new query with the given result type and bound to the given {@link HyperGraph} instance.
	 * </p>
	 * @param type The type of the result objects. This parameter is only necessary to avoid type casts.
	 * @param graph The database instance.
	 * @return A new, empty <code>HGQuery</code> object. The returned query doesn't have a condition yet. 
	 * Please call its {@link #compile(HGQueryCondition)} method in order to specify a condition. 
	 */
	public static <SearchResult> HGQuery<SearchResult> make(Class<SearchResult> type, HyperGraph graph)
	{
		return new ExpressionBasedQuery<SearchResult>(graph, true);
	}
	
	/**
	 * <p>Return the {@link HyperGraph} instance against which this query is executed.</p>
	 */
	public HyperGraph getHyperGraph()
	{
		return graph;
	}

	/**
	 * <p>Specify the HyperGraph instance against which this method is executed. This method is intended
	 * mostly for internal use. A <code>HGQuery</code> object is normally constructed by using
	 * information from a specific graph instance (e.g. available indices). Therefore, changing
	 * the graph instance and executing it again may fail if the metadata used in building the query
	 * differs between the two graphs.
	 * </p>  
	 */
	public void setHyperGraph(HyperGraph graph)
	{
		this.graph = graph;
	}

	/**
	 * <p>
	 * Execute the query and return the result set. Note that queries are lazily executed so that
	 * results are actually obtained when one iterates (using the <code>next</code> and <code>prev</code>
	 * of the returned object).
	 * </p>
	 */
	public abstract HGSearchResult<SearchResult> execute();
    
	/**
	 * <p>
	 * Execute the query and return the first result, if any. Otherwise return <code>null</code>.
	 * </p>
	 */
	public SearchResult findOne()
	{
    	return getHyperGraph().getTransactionManager().ensureTransaction(new Callable<SearchResult>() {
        	public SearchResult call()
        	{        	
        		HGSearchResult<SearchResult> rs = null;
        		try
        		{
        			rs = execute();
        			return rs.hasNext() ? rs.next() : null;
        		}
        		finally
        		{
        			if (rs != null) rs.close();
        		}
        	}
        	},
        	HGTransactionConfig.READONLY);						
	}
	
	/**
	 * <p>
	 * Execute the query and accumulate the results in a <code>List</code>.
	 * </p>
	 */
	public List<SearchResult> findAll()
	{
    	return getHyperGraph().getTransactionManager().ensureTransaction(new Callable<List<SearchResult>>() {
        	public List<SearchResult> call()
        	{        	
        		ArrayList<SearchResult> result = new ArrayList<SearchResult>();
        		HGSearchResult<SearchResult> rs = null;
        		try
        		{
        			rs = execute();
        			while (rs.hasNext())
        				result.add(rs.next());
        			return result;
        		}
        		finally
        		{
        			if (rs != null) rs.close();
        		}
        	}
        	},
        	HGTransactionConfig.READONLY);				
	}
	
	/**
	 * <p>
	 * Execute the query and accumulate the results in a <code>Set</code>.
	 * </p>
	 */
	public Set<SearchResult> findInSet()
	{
    	return getHyperGraph().getTransactionManager().ensureTransaction(new Callable<Set<SearchResult>>() {
        	public Set<SearchResult> call()
        	{            	
        		HashSet<SearchResult> result = new HashSet<SearchResult>();
        		HGSearchResult<SearchResult> rs = null;
        		try
        		{
        			rs = execute();
        			while (rs.hasNext())
        				result.add(rs.next());
        			return result;
        		}
        		finally
        		{
        			if (rs != null) rs.close();
        		}      	
        	}
        	}, 
        	HGTransactionConfig.READONLY);						
	}
	
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
    	public static <T> HGQuery<T> make(Class<T> type, HyperGraph graph)
    	{
    		return HGQuery.make(type, graph);
    	}
    	
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
            return graph.getTransactionManager().ensureTransaction(new Callable<HGHandle>() {
                public HGHandle call()
                {
                    And and = new And();
                    and.add(type(type));
                    if (instance instanceof HGLink)
                    {
                    	HGTypeStructuralInfo typeMeta = graph.getTypeSystem().getTypeMetaData(type);
                    	if (typeMeta != null && !typeMeta.isOrdered())                   		
                    		and.add(link((HGLink)instance));
                    	else
                    		and.add(orderedLink(HGUtils.toHandleArray((HGLink)instance)));
                    }
                    List<HGIndexer<?,?>> indexers = graph.getIndexManager().getIndexersForType(type);
                	boolean skipValue = false;
                    if (indexers != null)
                    {
                    	HashSet<String> dimensions = new HashSet<String>();
                    	for (HGIndexer<?,?> idx : indexers)
	                    {
	                    	if (idx instanceof ByPartIndexer)
	                    	{
	                    		ByPartIndexer<?> byPart = (ByPartIndexer<?>)idx;
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
        
        /**
         * <p>
         * Construct a {@link HGQueryCondition} that uniquely identifies an atom based on
         * the passed in <code>Object</code> instance.
         * </p>
         * 
         * @param graph The {@link HyperGraph} database instance.
         * @param instance The object for which an unique atom condition must be constructed.
         * @return <code>null</code> if <code>instance == null</code>, otherwise a query condition
         * identifying <code>instance</code> in the database.
         */
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
            List<HGIndexer<?,?>> indexers = graph.getIndexManager().getIndexersForType(type);
            if (indexers != null) for (HGIndexer<?,?> idx : indexers)
            {
            	if (idx instanceof ByPartIndexer)
            	{
            		ByPartIndexer<?> byPart = (ByPartIndexer<?>)idx;
            		Object prop = TypeUtils.project(graph, type, instance, byPart.getDimensionPath(), true);
            		and.add(new AtomPartCondition(byPart.getDimensionPath(), prop));
            	}
            }
            return and;
        }
        
    	/**
    	 * <p>
    	 * Add the given instance as an atom in the graph if and only if no atoms
    	 * match the passed in {@link HGQueryCondition}
    	 * </p>
    	 */
        public static HGHandle addUnique(final HyperGraph graph, 
        								 final Object instance, 
        								 final HGQueryCondition condition)
        {
        	return graph.getTransactionManager().ensureTransaction(new Callable<HGHandle>() {
        		public HGHandle call()
        		{
                    HGHandle h = findOne(graph, condition);
                    return h == null ?  graph.add(instance) : h;        			
        		}
        	});
        }
        
        /**
         * <p>
         * Add a new atom of a given type only if there's no atom matching the passed in
         * {@link HGQueryCondition}. Note that an {@link AtomTypeCondition}
         * based on the <code>typeHandle</code> parameter is "and-ed" with the <code>condition</code>
         * parameter.   
         * </p>
         */
        public static HGHandle addUnique(final HyperGraph graph, 
        								 final Object instance, 
        								 final HGHandle typeHandle, 
        								 final HGQueryCondition condition)
        {
        	return graph.getTransactionManager().ensureTransaction(new Callable<HGHandle>() {
        		public HGHandle call()
        		{		        	
		            HGHandle h = findOne(graph, and(type(typeHandle), condition));
		            return h == null ?  graph.add(instance, typeHandle) : h;
        		}
        	});		            
        }
        
        /**
         * <p>
         * Add a new atom of a given type only if there's no atom matching the passed in
         * {@link HGQueryCondition}. Note that an {@link AtomTypeCondition}
         * based on the <code>javaClass</code> parameter is "and-ed" with the <code>condition</code>
         * parameter.   
         * </p>
         */
        public static HGHandle addUnique(HyperGraph graph, 
        								 Object instance, 
        								 Class<?> javaClass, 
        								 HGQueryCondition condition)
        {            
            return addUnique(graph, instance, graph.getTypeSystem().getTypeHandle(javaClass), condition);
        }        
       
        /**
         * <p>
         * Return a condition that matches a regular expression pattern against the string value of an atom. 
         * </p>
         * 
         * @param pattern The pattern following the syntax of {@link java.util.regex.Pattern}.
         */
        public static AtomValueRegExPredicate matches(String pattern) { return new AtomValueRegExPredicate(Pattern.compile(pattern)); }
        
        /**
         * <p>
         * Return a condition that matches a regular expression pattern against the string value of an atom. 
         * </p>
         * 
         */
        public static AtomValueRegExPredicate matches(Pattern pattern) { return new AtomValueRegExPredicate(pattern); }
        
        /**
         * <p>
         * Return a condition that matches a regular expression pattern against the string value of 
         * an atom's projection (property) along the given path. 
         * </p>
         * 
         * @param pattern The pattern following the syntax of {@link java.util.regex.Pattern}.
         */
        public static AtomPartRegExPredicate matches(String path, String pattern) { return new AtomPartRegExPredicate(path.split("\\."), Pattern.compile(pattern)); }
        
        /**
         * <p>
         * Return a condition that matches a regular expression pattern against the string value of 
         * an atom's projection (property) along the given path. 
         * </p>
         */
        public static AtomPartRegExPredicate matches(String path, Pattern pattern) { return new AtomPartRegExPredicate(path.split("\\."), pattern); }        
        
        /**
         * <p>
         * Return the "identity" condition that evaluates to true for a specific handle. It
         * translates to a result set containing the specified atom handle. 
         * </p>
         * 
         * @param atomHandle
         * @return
         */
        public static IsCondition is(HGHandle atomHandle) { return new IsCondition(atomHandle); }
        
        /**
         * <p>Return a {@link HGQueryCondition} constraining the type of the result
         * to the type identified by <code>typeHandle</code>.</p>
         * @see AtomTypeCondition
         */
        public static AtomTypeCondition type(HGHandle typeHandle) { return new AtomTypeCondition(typeHandle); }
        
        /**
         * <p>Return a {@link HGQueryCondition} constraining the type of the result
         * to the type identified by <code>type</code> {@link Ref}. The content of the reference
         * can be a <code>Class</code> or a {@HGLink} and you can use a variable reference in a
         * compiled query. </p>
         */
        public static AtomTypeCondition type(Ref<?> type) { return new AtomTypeCondition(type); }
        
        /**
         * <p>Return a {@link HGQueryCondition} constraining the type of the result
         * to the type corresponding to the Java class <code>clazz</code>.</p>
         * @see AtomTypeCondition
         */
        public static AtomTypeCondition type(Class<?> clazz) { return new AtomTypeCondition(clazz); }

        /**
         * <p>Return a {@link HGQueryCondition} constraining the type of the result
         * to the type identified by <code>typeHandle</code> and all its sub-types. The set
         * of sub-types is obtained as the closure of the {@link HGSubsumes} relation.</p> 
         * @see TypePlusCondition
         */
        public static TypePlusCondition typePlus(HGHandle h) { return new TypePlusCondition(h); }
        /**
         * <p>Return a {@link HGQueryCondition} constraining the type of the result
         * to the type corresponding to the Java class <code>clazz</code> and all its sub-types. The set
         * of sub-types is obtained as the closure of the {@link HGSubsumes} relation.</p> 
         * @see TypePlusCondition
         */
        public static TypePlusCondition typePlus(Class<?> clazz) { return new TypePlusCondition(clazz); }
        /**
         * <p>Return a condition constraining the result set to atoms more general than the passed in
         * <code>specific</code> parameter. This condition is generally useful when searching for types,
         * but it is applicable to any set atoms interlinked with the {@link HGSubsumes} relation.
         * @see SubsumesCondition
         */
        public static SubsumesCondition subsumes(HGHandle specific) { return new SubsumesCondition(specific); }
        /**
         * <p>Return a condition constraining the result set to atoms more general than the passed in
         * <code>specific</code> reference parameter. This condition is generally useful when searching for types,
         * but it is applicable to any set atoms interlinked with the {@link HGSubsumes} relation.
         * @see SubsumesCondition
         */
        public static SubsumesCondition subsumes(Ref<HGHandle> specific) { return new SubsumesCondition(specific); }        
        /**
         * <p>Return a condition constraining the result set to atoms more specific than the passed in
         * <code>general</code> parameter. This condition is generally useful when searching for types,
         * but it is applicable to any set atoms interlinked with the {@link HGSubsumes} relation.
         * @see SubsumedCondition
         */
        public static SubsumedCondition subsumed(HGHandle general) { return new SubsumedCondition(general); }
        /**
         * <p>Return a condition constraining the result set to atoms more specific than the passed in
         * <code>general</code> reference parameter. This condition is generally useful when searching for types,
         * but it is applicable to any set atoms interlinked with the {@link HGSubsumes} relation.
         * @see SubsumedCondition
         */
        public static SubsumedCondition subsumed(Ref<HGHandle> general) { return new SubsumedCondition(general); }
        
        /**
         * <p>Return a conjunction (logical <code>and</code>) of conditions - atoms in the result set will have
         * to match all condition in the parameter list.</p>
         * @see And
         */
        public static And and(HGQueryCondition...clauses)
        {
            And and = new And();
            for (HGQueryCondition x:clauses)
                and.add(x); 
            return and;            
        }
        
        /**
         * <p>Return a disjunction (logical <code>or</code>) of conditions - atoms in the result set will have
         * to match at least one of the conditions in the parameter list.</p>
         * @see Or
         */
        public static Or or(HGQueryCondition...clauses)
        {
            Or or = new Or();
            for (HGQueryCondition x:clauses)
                or.add(x);
            return or;            
        }
        
        /**
         * <p>Return the negation of an {@link HGAtomPredicate}. </p>
         * @see Not
         */
        public static Not not(HGAtomPredicate predicate) { return new Not(predicate); }
        
        /**
         * <p>
         * Return a query condition that constraints the result set to atoms that are targets to a specific link. 
         * </p> 
         * @param linkHandle The handle of the {@link HGLink} whose target the resulting atom should be.
         * @see TargetCondition 
         */
        public static TargetCondition target(HGHandle linkHandle) { return new TargetCondition(linkHandle); }
        
        /**
         * <p>
         * Return a query condition that constraints the result set to atoms that are targets to a specific link as
         * specified by the {@link HGHandle} {@link Ref} parameter. 
         * </p> 
         * @param linkHandle A (possibly {@link Var}) reference to the handle of the {@link HGLink} whose target 
         * the resulting atom should be.
         * @see TargetCondition 
         */
        public static TargetCondition target(Ref<HGHandle> linkHandle) { return new TargetCondition(linkHandle); }
        
        /**
         * <p>
         * Return a condition constraining the result to links to a specific atom.
         * </p>         
         * @param atomHandle The atom to which resulting links should point to.
         * @see IncidentCondition
         */
        public static IncidentCondition incident(HGHandle atomHandle) { return new IncidentCondition(atomHandle); }
        public static IncidentCondition incident(Ref<?> atomHandle) { return new IncidentCondition((Ref<HGHandle>)atomHandle); }
        
				/**
				 * <p>
				 * Return a condition constraining the query result set to links pointing to a target atom
				 * positioned within a predetermined range in the link tuple. 
				 * </p>
				 * 
				 * @param target
				 *          A {@link Ref} to the target atom specified as a {@link HGHandle}. 
				 * @param lowerBound 
				 *          A {@link Ref} to the lower bound of the desired range. If the number is negative, it is
				 *          counted from the last target in the tuple (e.g. -1 means the last target, -2 the penultimate 
				 *          target etc.). 
				 * @param upperBound 
				 *          A {@link Ref} to the upper bound of the desired range. If the number is negative, it is
				 *          counted from the last target in the tuple (e.g. -1 means the last target, -2 the penultimate 
				 *          target etc.). 
				 * @see PositionedIncidentCondition
				 */
        public static PositionedIncidentCondition incidentAt(Ref<HGHandle> target, Ref<Integer> lowerBound, Ref<Integer> upperBound) 
        { return new PositionedIncidentCondition(target, lowerBound, upperBound, hg.constant(false)); }
        
        /**
         * @see {@link #incidentAt(Ref, Ref, Ref)}.
         */
        public static PositionedIncidentCondition incidentAt(HGHandle target, int lowerBound, int upperBound) 
    		{ return hg.incidentAt(hg.constant(target), hg.constant(lowerBound), hg.constant(upperBound)); }
        
        /**
         * @see {@link #incidentAt(Ref, Ref, Ref)} - <code>position</code> is used both as lower and upper bound.
         */
        public static PositionedIncidentCondition incidentAt(Ref<HGHandle> target, int position) 
    		{ return hg.incidentAt(target, hg.constant(position), hg.constant(position)); }
        
        /**
         * @see {@link #incidentAt(Ref, Ref, Ref)} - <code>position</code> is used both as lower and upper bound.
         */
        public static PositionedIncidentCondition incidentAt(HGHandle target, int position) 
    		{ return hg.incidentAt(hg.constant(target), hg.constant(position), hg.constant(position)); }

        /**
         * Same as {@link hg#incidentAt(Ref, Ref, Ref) except uses the complement of the specified range. 
         * That is, if you specify a range of [2,4], it will match links that do point to the desired target,
         * but not at positions 2-4. 
         * @see {@link PositionedIncidentCondition}
         */
        public static PositionedIncidentCondition incidentNotAt(Ref<HGHandle> target, Ref<Integer> lowerBound, Ref<Integer> upperBound) 
        { return new PositionedIncidentCondition(target, lowerBound, upperBound, hg.constant(true)); }
        
        /**
         * @see {@link #incidentNotAt(Ref, Ref, Ref)}.
         */
        public static PositionedIncidentCondition incidentNotAt(HGHandle target, int lowerBound, int upperBound) 
    		{ return hg.incidentNotAt(hg.constant(target), hg.constant(lowerBound), hg.constant(upperBound)); }
        
        /**
         * @see {@link #incidentNotAt(Ref, Ref, Ref)} - <code>position</code> is used both as lower and upper bound.
         */
        public static PositionedIncidentCondition incidentNotAt(HGHandle target, int position) 
    		{ return hg.incidentNotAt(hg.constant(target), hg.constant(position), hg.constant(position)); }
        
        /**
         * @see {@link #incidentNotAt(Ref, Ref, Ref)} - <code>position</code> is used both as lower and upper bound.
         */
        public static PositionedIncidentCondition incidentNotAt(Ref<HGHandle> target, int position) 
    		{ return hg.incidentNotAt(target, hg.constant(position), hg.constant(position)); }
        
        /**
         * <p>Return a condition constraining the query result set to links pointing to a target set 
         * of atoms. 
         * </p>
         * @param link The target set specified as a {@link HGLink} instance. The order of targets in this
         * link is ignored - it is treated as a set.
         * @see LinkCondition
         */
        public static LinkCondition link(HGLink link) { return new LinkCondition(link); }
        
        /**
         * <p>Return a condition constraining the query result set to links pointing to a target set 
         * of atoms. 
         * </p>
         * @param link The target set specified as a {@link HGHandle} array. The order of targets in this
         * array is ignored - it is treated as a set.
         * @see LinkCondition
         */
        public static LinkCondition link(HGHandle...h) { return new LinkCondition(h); }
        
        /**
         * <p>Return a condition constraining the query result set to links pointing to a target set 
         * of atoms, as specified by the list of {@link HGHandle} {@link Ref}. 
         * </p>
         * @param link The target set specified as an of references to {@link HGHandle}. The order of targets in this
         * array is ignored - it is treated as a set.
         * @see LinkCondition
         */
        public static LinkCondition link(Ref<HGHandle>...h) { return new LinkCondition(h); }
        
        /**
         * <p>Return a condition constraining the query result set to links pointing to a target set 
         * of atoms. 
         * </p>
         * @param C The target set specified as a Java collection.
         * @see LinkCondition
         */        
        public static LinkCondition link(Collection<HGHandle> C) { return new LinkCondition(C); }
        
        /**
         * <p>Return a condition constraining the query result set to being ordered links of a certain
         * form. 
         * </p>
         * @param h The target set specified as a {@link HGHandle} array. The order of targets in this
         * array sets the order of targets in the resulting atoms. 
         * @see OrderedLinkCondition
         */        
        public static OrderedLinkCondition orderedLink(HGHandle...h) { return new OrderedLinkCondition(h); }

        /**
         * <p>Return a condition constraining the query result set to being ordered links of a certain
         * form. 
         * </p>
         * @param h The target set specified as an of {@link Ref} {@link HGHandle}s. The order of targets in this
         * array sets the order of targets in the resulting atoms. 
         * @see OrderedLinkCondition
         */        
        public static OrderedLinkCondition orderedLink(Ref<HGHandle>...h) { return new OrderedLinkCondition(h); }
        
        /**
         * <p>Return a condition constraining the query result set to being ordered links of a certain
         * form. 
         * </p>
         * @param L The target set specified as a Java list. The order of targets in this
         * list sets the order of targets in the resulting atoms.
         * @see OrderedLinkCondition 
         */        
        public static OrderedLinkCondition orderedLink(List<HGHandle> L) { return new OrderedLinkCondition(L); }
        
        /**
         * <p>Return a condition constraining the query result set to being links with the specified arity (number
         * of targets).
         * </p>
         * @param i The arity of the atoms in the result set. 
         * @see ArityCondition
         */                
        public static ArityCondition arity(int i) { return new ArityCondition(i); }

        /**
         * <p>Return a condition constraining the query result set to being links with the specified arity (number
         * of targets).
         * </p>
         * @param i A {@link Ref} to the arity of the atoms in the result set. 
         * @see ArityCondition
         */                
        public static ArityCondition arity(Ref<Integer> i) { return new ArityCondition(i); }
        
        /**
         * <p>Return an atom predicate constraining the result set to atoms that are not connected
         * to other atoms, i.e. whose incidence set is empty.</p>
         * @see DisconnectedPredicate
         */
        public static DisconnectedPredicate disconnected() { return new DisconnectedPredicate(); }

        /**
         * <p>
         * Return a condition constraining the result set to atoms that are members of the 
         * specified subgraph.
         * </p>
         * @param subgraphHandle The atom handle of the {@link HGSubgraph} atoms.
         */
        public static SubgraphMemberCondition memberOf(HGHandle subgraphHandle) { return new SubgraphMemberCondition(subgraphHandle); }
        
        /**
         * <p>
         * Return a condition constraining the result set to atoms that are instances of
         * {@link HGSubgraph} and containing the specified atom.
         * </p>
         * @param The atom that return subgraphs should contain.
         */
        public static SubgraphContainsCondition contains(HGHandle atom) { return new SubgraphContainsCondition(atom); }
        
        /**
         * <p>
         * Return a condition that constraints resulting atoms by a specific value and {@link ComparisonOperator}.
         * </p>
         * 
         * @param value The value to compare with.
         * @param op The {@link ComparisonOperator} to use in the comparison.
         * @see AtomValueCondition
         */
        public static AtomValueCondition value(Object value, ComparisonOperator op) { return new AtomValueCondition(value, op); }
        
        /**
         * <p>
         * Return a condition that constraints resulting atoms by a specific value 
         * {@link Ref} and {@link ComparisonOperator}.
         * </p>
         * 
         * @param value The value reference to compare with.
         * @param op The {@link ComparisonOperator} to use in the comparison.
         * @see AtomValueCondition
         */
        public static AtomValueCondition value(Ref<Object> value, ComparisonOperator op) 
        	{ return new AtomValueCondition((Ref<Object>)(value == null ? hg.constant(value) : value), op); }
        
        /**
         * <p>Return a condition constraining resulting atoms to atoms whose value is equal to 
         * the passed in <code>x</code> parameter.
         * </p>
         * @see AtomValueCondition
         */
        public static AtomValueCondition eq(Object x) { return value(x, ComparisonOperator.EQ); }

        /**
         * <p>Return a condition constraining resulting atoms to atoms whose value is equal to 
         * the passed in <code>x</code> {@link Ref} parameter.
         * </p>
         * @see AtomValueCondition
         */
        public static AtomValueCondition eq(Ref<Object> x) { return value(x, ComparisonOperator.EQ); }
        
        /**
         * <p>Return a condition constraining resulting atoms to atoms whose value is less than 
         * the passed in <code>x</code> parameter.
         * </p>
         * @see AtomValueCondition 
         */
        public static AtomValueCondition lt(Object x) { return value(x, ComparisonOperator.LT); }        

        /**
         * <p>Return a condition constraining resulting atoms to atoms whose value is less than 
         * the passed in <code>x</code> {@link Ref} parameter.
         * </p>
         * @see AtomValueCondition 
         */
        public static AtomValueCondition lt(Ref<Object> x) { return value(x, ComparisonOperator.LT); }        
        
        /**
         * <p>Return a condition constraining resulting atoms to atoms whose value is greater than 
         * the passed in <code>x</code> parameter.
         * </p>
         * @see AtomValueCondition 
         */
        public static AtomValueCondition gt(Object x) { return value(x, ComparisonOperator.GT); }

        /**
         * <p>Return a condition constraining resulting atoms to atoms whose value is greater than 
         * the passed in <code>x</code> {@link Ref} parameter.
         * </p>
         * @see AtomValueCondition 
         */
        public static AtomValueCondition gt(Ref<Object> x) { return value(x, ComparisonOperator.GT); }
        
        /**
         * <p>Return a condition constraining resulting atoms to atoms whose value is less than or equal to 
         * the passed in <code>x</code> parameter.
         * </p>
         * @see AtomValueCondition 
         */
        public static AtomValueCondition lte(Object x) { return value(x, ComparisonOperator.LTE); }

        /**
         * <p>Return a condition constraining resulting atoms to atoms whose value is less than or equal to 
         * the passed in <code>x</code> {@link Ref} parameter.
         * </p>
         * @see AtomValueCondition 
         */
        public static AtomValueCondition lte(Ref<Object> x) { return value(x, ComparisonOperator.LTE); }
        
        /**
         * <p>Return a condition constraining resulting atoms to atoms whose value is greater than equal to 
         * the passed in <code>x</code> parameter.
         * </p>
         * @see AtomValueCondition 
         */
        public static AtomValueCondition gte(Object x) { return value(x, ComparisonOperator.GTE); }

        /**
         * <p>Return a condition constraining resulting atoms to atoms whose value is greater than equal to 
         * the passed in <code>x</code> {@link Ref} parameter.
         * </p>
         * @see AtomValueCondition 
         */
        public static AtomValueCondition gte(Ref<Object> x) { return value(x, ComparisonOperator.GTE); }
        
        /**
         * <p>
         * Return a condition constraining the result to atoms of some {@link HGCompositeType} and having
         * a certain part (e.g. a Java property) as specified by the <code>value</code> and <code>op</code>
         * {@link ComparisonOperator} parameters.  
         * </p>
         * @param path The path of the property with the nested value structure of the atom. This is specified with
         * "dotted" notation. For example: <code>user.address.street</code>.
         * @param value The value to compare the atom part against.
         * @param op The {@link ComparisonOperator} to use. Not that if <code>op != ComparisonOperator.EQ</code>, the
         * atom part must be <code>Comparable</code>.
         * @see AtomPartCondition
         */
        public static AtomPartCondition part(String path, Object value, ComparisonOperator op) 
        	{  return new AtomPartCondition(path.split("\\."), value, op); }
        
        /**
         * See {@link #part(String, Object, ComparisonOperator)}. This method specifying the value as a 
         * {@link Ref}.
         */
        public static AtomPartCondition part(String path, Ref<Object> value, ComparisonOperator op) 
        	{  return new AtomPartCondition(path.split("\\."), (Ref<Object>)(value == null ? hg.constant(value) : value), op); }
        
        /**
         * <p>
         * Return a condition constraining the result to atoms of some {@link HGCompositeType} and having
         * a part (e.g. a Java property) equal to the specified <code>value</code>.
         * </p>
         * @param path The path of the property with the nested value structure of the atom. This is specified with
         * "dotted" notation. For example: <code>user.address.street</code>.
         * @param value The value to compare the atom part against.
         * @see AtomPartCondition
         */
        public static AtomPartCondition eq(String path, Object x) { return part(path, x, ComparisonOperator.EQ); }

        /**
         * See {@link #eq(String, Object)}. This method specifying the value as a 
         * {@link Ref}.
         */
        public static AtomPartCondition eq(String path, Ref<Object> x) 
        	{ return part(path, (Ref<Object>)(x == null ? hg.constant(x) : x), ComparisonOperator.EQ); }
        
        /**
         * <p>
         * Return a condition constraining the result to atoms of some {@link HGCompositeType} and having
         * a part (e.g. a Java property) less than the specified <code>value</code>.
         * </p>
         * @param path The path of the property with the nested value structure of the atom. This is specified with
         * "dotted" notation. For example: <code>user.address.street</code>.
         * @param value The value to compare the atom part against.
         * @see AtomPartCondition
         */
        public static AtomPartCondition lt(String path, Object x) { return part(path, x, ComparisonOperator.LT); }        

        /**
         * See {@link #lt(String, Object)}. This method specifying the value as a 
         * {@link Ref}.
         */
        public static AtomPartCondition lt(String path, Ref<Object> x) { return part(path, x, ComparisonOperator.LT); }        
        
        /**
         * <p>
         * Return a condition constraining the result to atoms of some {@link HGCompositeType} and having
         * a part (e.g. a Java property) greater than the specified <code>value</code>.
         * </p>
         * @param path The path of the property with the nested value structure of the atom. This is specified with
         * "dotted" notation. For example: <code>user.address.street</code>.
         * @param value The value to compare the atom part against.
         * @see AtomPartCondition
         */
        public static AtomPartCondition gt(String path, Object x) { return part(path, x, ComparisonOperator.GT); }

        /**
         * See {@link #gt(String, Object)}. This method specifying the value as a 
         * {@link Ref}.
         */
        public static AtomPartCondition gt(String path, Ref<Object> x) { return part(path, x, ComparisonOperator.LT); }        
        
        /**
         * <p>
         * Return a condition constraining the result to atoms of some {@link HGCompositeType} and having
         * a part (e.g. a Java property) less than or equal to the specified <code>value</code>.
         * </p>
         * @param path The path of the property with the nested value structure of the atom. This is specified with
         * "dotted" notation. For example: <code>user.address.street</code>.
         * @param value The value to compare the atom part against.
         * @see AtomPartCondition
         */
        public static AtomPartCondition lte(String path, Object x) { return part(path, x, ComparisonOperator.LTE); }
        
        /**
         * See {@link #lte(String, Object)}. This method specifying the value as a 
         * {@link Ref}.
         */
        public static AtomPartCondition lte(String path, Ref<Object> x) { return part(path, x, ComparisonOperator.LT); }        
        
        /**
         * <p>
         * Return a condition constraining the result to atoms of some {@link HGCompositeType} and having
         * a part (e.g. a Java property) greater than or equal to the specified <code>value</code>.
         * </p>
         * @param path The path of the property with the nested value structure of the atom. This is specified with
         * "dotted" notation. For example: <code>user.address.street</code>.
         * @param value The value to compare the atom part against.
         * @see AtomPartCondition
         */
        public static AtomPartCondition gte(String path, Object x) { return part(path, x, ComparisonOperator.GTE); }
        
        /**
         * See {@link #gte(String, Object)}. This method specifying the value as a 
         * {@link Ref}.
         */
        public static AtomPartCondition gte(String path, Ref<Object> x) { return part(path, x, ComparisonOperator.LT); }        
        
        /**
         * <p>
         * Return a "condition" that transforms the result set by applying an arbitrary {@link Mapping} to each 
         * element. 
         * </p>
         * 
         * @param m The {@link Mapping} to apply.
         * @param c The underlying condition to evaluate before applying the mapping.
         * @see MapCondition
         */
        public static HGQueryCondition apply(Mapping<?,?> m, HGQueryCondition c) { return new MapCondition(c, m); }
        
        /**
         * <p>
         * Return a {@link Mapping} that takes a link atom and returns a target at the given position.
         * </p>
         * @param targetPosition The position of the target to be returned.
         * @see LinkProjectionMapping
         */
        public static Mapping<HGLink, HGHandle> linkProjection(int targetPosition) { return new LinkProjectionMapping(targetPosition); }
        /**
         * <p>
         * Return a {@link Mapping} that takes a link atom and returns a target at the given position.
         * </p>
         * @param targetPosition A {@link Ref} to the position of the target to be returned.
         * @see LinkProjectionMapping
         */
        public static Mapping<HGLink, HGHandle> linkProjection(Ref<Integer> targetPosition) { return new LinkProjectionMapping(targetPosition); }
        
        /**
         * <p>
         * Return a {@link Mapping} that takes a {@link HGHandle} of an atom and return its runtime against
         * through a call to <code>HyperGraph.get</code>.
         * </p>
         * @param graph The {@link HyperGraph} instance against which to dereference the atom.
         * @see DerefMapping
         */
        public static <T> Mapping<HGHandle, T> deref(HyperGraph graph) { return new DerefMapping<T>(graph); }
        
        /**
         * <p>
         * Short for <code>hg.apply(hg.deref(graph), condition)</code> - assuming the <code>condition</code>
         * parameter yields a result set of {@link HGHandle}s, the returned condition will yield a result set
         * of atoms loaded from the <code>graph</code> parameter. 
         * </p>
         * 
         * @param graph
         * @param condition
         * @return
         */
        public static HGQueryCondition deref(HyperGraph graph, HGQueryCondition condition) { return apply(deref(graph), condition); }
        
        /**
         * <p>
         * Return a {@link Mapping} that given a handle to a link will return the target (handle) at the specified
         * target position. This is a {@link CompositeMapping} of <code>deref(graph)</code> and <code>linkProjection(targetPosition)</code>.
         * </p>
         * @param graph The HyperGraph instance.
         * @param targetPosition The target position.
         * @see CompositeMapping
         */
        public static Mapping<HGHandle,HGHandle> targetAt(HyperGraph graph, int targetPosition) 
        { 
        	Mapping<HGHandle, HGLink> f = deref(graph);
        	return new CompositeMapping<HGHandle, HGHandle, HGLink>(f, linkProjection(targetPosition)); 
        }
        
        /**
         * <p>
         * Return a condition that will yield all atoms in the graph.
         * </p>
         * @see AnyAtomCondition
         */
        public static HGQueryCondition all() { return new AnyAtomCondition(); }
     
        /**
         * <p>
         * Return a condition whose result set is the breadth first traversal of the graph
         * starting a given atom.
         * </p>
         * @param start The starting atom.
         * @see BFSCondition
         */
        public static BFSCondition bfs(HGHandle start) { return new BFSCondition(start); }
        
        /**
         * <p>
         * Return a condition whose result set is the breadth first traversal of the graph
         * starting a given atom.
         * </p>
         * @param start A {@link Ref} to the starting atom.
         * @see BFSCondition
         */
        public static BFSCondition bfs(Ref<HGHandle> start) { return new BFSCondition(start); }
        
        /**
         * <p>
         * Return a condition whose result set is the breadth first traversal of the graph
         * starting a given atom.
         * </p>
         * @param start The starting atom.
         * @param lp A filtering {@link HGAtomPredicate} constraining what links to follow - only
         * links satisfying this predicate will be followed.
         * @param sp A filtering {@link HGAtomPredicate} - only atoms satisfying this predicate
         * will be *traversed*. If you want all atoms to be traversed, but examine only a subset
         * of them, use a conjunction of this condition and an {@link HGAtomPredicate}, e.g.
         * <code>hg.and(hg.type(someType), hg.bfs(startingAtom))</code>. 
         * @see BFSCondition
         */
        public static BFSCondition bfs(HGHandle start, 
        							   HGAtomPredicate lp, 
        							   HGAtomPredicate sp) 
        { 
        	BFSCondition c = new BFSCondition(start);
        	c.setLinkPredicate(lp);
        	c.setSiblingPredicate(sp);
        	return c;
        }

        /**
         * <p>
         * Return a condition whose result set is the breadth first traversal of the graph
         * starting a given atom.
         * </p>
         * @param start A {@link Ref} to the starting atom.
         * @param lp A filtering {@link HGAtomPredicate} constraining what links to follow - only
         * links satisfying this predicate will be followed.
         * @param sp A filtering {@link HGAtomPredicate} - only atoms satisfying this predicate
         * will be *traversed*. If you want all atoms to be traversed, but examine only a subset
         * of them, use a conjunction of this condition and an {@link HGAtomPredicate}, e.g.
         * <code>hg.and(hg.type(someType), hg.bfs(startingAtom))</code>. 
         * @see BFSCondition
         */
        public static BFSCondition bfs(Ref<HGHandle> start, 
        							   HGAtomPredicate lp, 
        							   HGAtomPredicate sp) 
        { 
        	BFSCondition c = new BFSCondition(start);
        	c.setLinkPredicate(lp);
        	c.setSiblingPredicate(sp);
        	return c;
        }
        
        
        /**
         * <p>
         * Return a condition whose result set is the breadth first traversal of the graph
         * starting a given atom.
         * </p>
         * @param start The starting atom.
         * @param lp A filtering {@link HGAtomPredicate} constraining what links to follow - only
         * links satisfying this predicate will be followed.
         * @param sp A filtering {@link HGAtomPredicate} - only atoms satisfying this predicate
         * will be *traversed*. If you want all atoms to be traversed, but examine only a subset
         * of them, use a conjunction of this condition and an {@link HGAtomPredicate}, e.g.
         * <code>hg.and(hg.type(someType), hg.bfs(startingAtom))</code>.
         * @param returnPreceding Whether to return siblings preceding the current atom in an ordered link.
         * @param returnSucceeding Whether to return siblings following the current atom in an ordered link.
         * @see BFSCondition 
         */        
        public static BFSCondition bfs(HGHandle start, 
									   HGAtomPredicate lp, 
									   HGAtomPredicate sp,
									   boolean returnPreceding,
									   boolean returnSucceeding) 
		{ 
			BFSCondition c = new BFSCondition(start);
			c.setLinkPredicate(lp);
			c.setSiblingPredicate(sp);
			c.setReturnPreceeding(returnPreceding);
			c.setReturnSucceeding(returnSucceeding);
			return c;
		}        

        
        /**
         * <p>
         * Return a condition whose result set is the breadth first traversal of the graph
         * starting a given atom.
         * </p>
         * @param start A {@link Ref} to the starting atom.
         * @param lp A filtering {@link HGAtomPredicate} constraining what links to follow - only
         * links satisfying this predicate will be followed.
         * @param sp A filtering {@link HGAtomPredicate} - only atoms satisfying this predicate
         * will be *traversed*. If you want all atoms to be traversed, but examine only a subset
         * of them, use a conjunction of this condition and an {@link HGAtomPredicate}, e.g.
         * <code>hg.and(hg.type(someType), hg.bfs(startingAtom))</code>.
         * @param returnPreceding Whether to return siblings preceding the current atom in an ordered link.
         * @param returnSucceeding Whether to return siblings following the current atom in an ordered link.
         * @see BFSCondition 
         */        
        public static BFSCondition bfs(Ref<HGHandle> start, 
									   HGAtomPredicate lp, 
									   HGAtomPredicate sp,
									   boolean returnPreceding,
									   boolean returnSucceeding) 
		{ 
			BFSCondition c = new BFSCondition(start);
			c.setLinkPredicate(lp);
			c.setSiblingPredicate(sp);
			c.setReturnPreceeding(returnPreceding);
			c.setReturnSucceeding(returnSucceeding);
			return c;
		}        
        
        /**
         * <p>
         * Return a condition whose result set is the depth first traversal of the graph
         * starting with a given atom.
         * </p>
         * @param start The starting atom.
         * @see DFSCondition
         */        
        public static DFSCondition dfs(HGHandle start) { return new DFSCondition(start); }

        /**
         * <p>
         * Return a condition whose result set is the depth first traversal of the graph
         * starting with a given atom.
         * </p>
         * @param start A {@link Ref} to the starting atom.
         * @see DFSCondition
         */        
        public static DFSCondition dfs(Ref<HGHandle> start) { return new DFSCondition(start); }
        
        /**
         * <p>
         * Return a condition whose result set is the depth first traversal of the graph
         * starting with a given atom.
         * </p>
         * @param start The starting atom.
         * @param linkPredicate A filtering {@link HGAtomPredicate} constraining what links to follow - only
         * links satisfying this predicate will be followed.
         * @param siblingPredicate A filtering {@link HGAtomPredicate} - only atoms satisfying this predicate
         * will be *traversed*. If you want all atoms to be traversed, but examine only a subset
         * of them, use a conjunction of this condition and an {@link HGAtomPredicate}, e.g.
         * <code>hg.and(hg.type(someType), hg.bfs(startingAtom))</code>.
         * @see DFSCondition 
         */        
        public static DFSCondition dfs(HGHandle start, 
        							   HGAtomPredicate linkPredicate, 
        							   HGAtomPredicate siblingPredicate) 
        { 
        	DFSCondition c = new DFSCondition(start);
        	c.setLinkPredicate(linkPredicate);
        	c.setSiblingPredicate(siblingPredicate);
        	return c;
        }

        /**
         * <p>
         * Return a condition whose result set is the depth first traversal of the graph
         * starting with a given atom.
         * </p>
         * @param start A {@link Ref} to the starting atom.
         * @param linkPredicate A filtering {@link HGAtomPredicate} constraining what links to follow - only
         * links satisfying this predicate will be followed.
         * @param siblingPredicate A filtering {@link HGAtomPredicate} - only atoms satisfying this predicate
         * will be *traversed*. If you want all atoms to be traversed, but examine only a subset
         * of them, use a conjunction of this condition and an {@link HGAtomPredicate}, e.g.
         * <code>hg.and(hg.type(someType), hg.bfs(startingAtom))</code>.
         * @see DFSCondition 
         */        
        public static DFSCondition dfs(Ref<HGHandle> start, 
        							   HGAtomPredicate linkPredicate, 
        							   HGAtomPredicate siblingPredicate) 
        { 
        	DFSCondition c = new DFSCondition(start);
        	c.setLinkPredicate(linkPredicate);
        	c.setSiblingPredicate(siblingPredicate);
        	return c;
        }
        
        /**
         * <p>
         * Return a condition whose result set is the depth first traversal of the graph
         * starting a given atom.
         * </p>
         * @param start The starting atom.
         * @param linkPredicate A filtering {@link HGAtomPredicate} constraining what links to follow - only
         * links satisfying this predicate will be followed.
         * @param siblingPredicate A filtering {@link HGAtomPredicate} - only atoms satisfying this predicate
         * will be *traversed*. If you want all atoms to be traversed, but examine only a subset
         * of them, use a conjunction of this condition and an {@link HGAtomPredicate}, e.g.
         * <code>hg.and(hg.type(someType), hg.bfs(startingAtom))</code>.
         * @param returnPreceding Whether to return siblings preceding the current atom in an ordered link.
         * @param returnSucceeding Whether to return siblings following the current atom in an ordered link.
         * @see DFSCondition 
         */         
        public static DFSCondition dfs(HGHandle start, 
									   HGAtomPredicate linkPredicate, 
									   HGAtomPredicate siblingPredicate,
									   boolean returnPreceeding,
									   boolean returnSucceeding) 
		{ 
			DFSCondition c = new DFSCondition(start);
			c.setLinkPredicate(linkPredicate);
			c.setSiblingPredicate(siblingPredicate);
			c.setReturnPreceeding(returnPreceeding);
			c.setReturnSucceeding(returnSucceeding);
			return c;
		}

        /**
         * <p>
         * Return a condition whose result set is the depth first traversal of the graph
         * starting a given atom.
         * </p>
         * @param start A {@link Ref} to the starting atom.
         * @param linkPredicate A filtering {@link HGAtomPredicate} constraining what links to follow - only
         * links satisfying this predicate will be followed.
         * @param siblingPredicate A filtering {@link HGAtomPredicate} - only atoms satisfying this predicate
         * will be *traversed*. If you want all atoms to be traversed, but examine only a subset
         * of them, use a conjunction of this condition and an {@link HGAtomPredicate}, e.g.
         * <code>hg.and(hg.type(someType), hg.bfs(startingAtom))</code>.
         * @param returnPreceding Whether to return siblings preceding the current atom in an ordered link.
         * @param returnSucceeding Whether to return siblings following the current atom in an ordered link.
         * @see DFSCondition 
         */         
        public static DFSCondition dfs(Ref<HGHandle> start, 
									   HGAtomPredicate linkPredicate, 
									   HGAtomPredicate siblingPredicate,
									   boolean returnPreceeding,
									   boolean returnSucceeding) 
		{ 
			DFSCondition c = new DFSCondition(start);
			c.setLinkPredicate(linkPredicate);
			c.setSiblingPredicate(siblingPredicate);
			c.setReturnPreceeding(returnPreceeding);
			c.setReturnSucceeding(returnSucceeding);
			return c;
		}
        
        static final HGHandle the_any_handle = new HGHandle()
        {
            public HGPersistentHandle getPersistent()
            {
                // A persistent "any handle" should never be used
                // in this context. If needed, it has to be obtained
                // from the handle factory associated with the graph.
                // The runtime "any handle" here is only to be used
                // as a don't care in query expressions.
                return null; 
            }
        };
        
        /**
         * <p>Return a special handle indicating a "don't care" in a condition expression (e.g.
         * when specifying the form of a link in a <code>hg.orderedLink(hg.anyHandle(), x, y)</code>
         * condition.</p>
         * <p>
         * <strong>Node:</strong> This is not a {@link org.hypergraphdb.HGPersistentHandle}. To get a persistent "don't care" handle, 
         * please use {@link org.hypergraphdb.HGHandleFactory.anyHandle()}.
         * </p>
         */
        public static HGHandle anyHandle() { return the_any_handle; }
        
        /**
         * <p>Create a new variable with an initial value and attach it to the current variable context.</p>
         * 
         * @param name The name of the variable. Any previous variable with that name in the current context will
         * be overridden.
         * @param initialValue The initial value of the variable.
         * @return The {@link Var} instance.
         */
        public static <T> Var<T> var(String name, T initialValue) 
        {
        	Var<T> v = VarContext.ctx().get(name);
        	VarContext.ctx().setGlobalValue(name, initialValue);
        	return v;
        }

        /**
         * <p>Create a new variable and attach it to the current variable context.</p>
         * 
         * @param name The name of the variable. Any previous variable with that name in the current context will
         * be overriden.
         * @return The {@link Var} instance.
         */
        public static <T> Var<T> var(String name) 
        {
        	return VarContext.ctx().get(name);
        }

        /**
         * <p>Create a new variable and attach it to the current variable context.</p>
         * 
         * @param name The name of the variable. Any previous variable with that name in the current context will
         * be overridden.
         * @param t A class parameter allowing to specify the type of the variable without providing an initial
         * value. Use this to avoid ugly type casts.
         * @return The {@link Var} instance.
         */
        public static <T> Var<T> var(String name, Class<T> t) 
        {
        	return VarContext.ctx().get(name);
        }
        
        /**
         * <p>Create a new constant reference. The value of this reference cannot be changed.</p>
         * 
         * @param value The value of the constant.
         * @return An instance of {@link Constant} whose {@link Constant#get()} method will return
         * the passed in value.
         */
        public static <T> Ref<T> constant(T value)
        {
        	return new Constant<T>(value);
        }
        
        /**
         * <p>Return <code>true</code> if the passed in {@link Ref} is a {@link Var} and
         * <code>false</code> otherwise.
         */
        public static <T> boolean isVar(Ref<T> ref)
        {
        	return ref instanceof Var;
        }
        
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
        		ExpressionBasedQuery<?> q = (ExpressionBasedQuery<?>)HGQuery.make(graph, cond);
            	ResultSizeEstimation.Counter counter = ResultSizeEstimation.countersMap.get(q.getCondition().getClass());
            	if (counter == null)
            		return ResultSizeEstimation.countResultSet(q);
            	else
            		return counter.count(graph, q.getCondition());        		
        	}
        	}, HGTransactionConfig.READONLY);
        }
        
        /**
         * <p>
         * Count the result set from executing the given query. The query is executed and
         * the result set scanned completely.
         * </p>
         */
        public static long count(final HGQuery<?> query)
        {
        	return query.getHyperGraph().getTransactionManager().ensureTransaction(new Callable<Long>() {
            	public Long call()
            	{
                	return ResultSizeEstimation.countResultSet(query);
            	}
            	}, HGTransactionConfig.READONLY);        	
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
    	 */
    	public static <T> T getOne(final HyperGraph graph, final HGQueryCondition condition)
    	{
        	return graph.getTransactionManager().ensureTransaction(new Callable<T>() {
            	public T call()
            	{    		
		    		HGHandle h = null;
            		HGSearchResult<HGHandle> rs = null;
            		try
            		{
            			rs = graph.find(condition);
            			if (rs.hasNext())
            				h = rs.next();
            		}
            		finally
            		{
            			if (rs != null) rs.close();
            		}
		    		return h == null ? null : (T)graph.get(h);
            	}
            }, HGTransactionConfig.READONLY);
    	}
    	
    	/**
    	 * <p>
    	 * Run a query based on the passed in condition. If the result set is not
    	 * empty, return the first <code>HGHandle</code> element. Otherwise,
    	 * return <code>null</code>.
    	 * </p>
    	 * 
    	 * @param graph The {@link HyperGraph} instance to run the query against.
    	 * @param condition The query condition constraining the resulting atom.
    	 * @return The very first result from the result set, or <code>null</code> if
    	 * the result set is empty. 
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
            	}, HGTransactionConfig.READONLY);
    	}
    	
    	/**
    	 * <p>
    	 * Run a query based on the specified condition and put all <code>HGHandle</code>s
    	 * from the result set into a <code>java.util.List</code>.
    	 * </p>
    	 *  
    	 * @param graph The {@link HyperGraph} to run the query against.
    	 * @param condition The query condition constraining the result set.
    	 * @return A list of all results from the result set.
    	 */
    	public static <T> List<T> findAll(final HyperGraph graph, final HGQueryCondition condition)
    	{
//    		final ArrayList<T> result = new ArrayList<T>();
//    		HGQuery<T> query = HGQuery.make(graph, condition);
//    		HGUtils.queryBatchProcess(query,
//    				new Mapping<T, Boolean>()
//    				{
//    					public Boolean eval(T x) { result.add(x); return Boolean.TRUE; }
//    				},
//    				500,
//					null,
//					1);
//    		return result;    		
        	return graph.getTransactionManager().ensureTransaction(new Callable<List<T>>() {
            	public List<T> call()
            	{
            		ArrayList<T> result = new ArrayList<T>();
            		HGSearchResult<T> rs = null;
            		try
            		{
            			rs = graph.find(condition);
            			while (rs.hasNext())
            				result.add(rs.next());
            			return result;
            		}
            		finally
            		{
            			if (rs != null) rs.close();
            		}	
            	}
            	}, HGTransactionConfig.READONLY);
    	}

    	/**
    	 * <p>
    	 * Run a query based on the specified condition and put all atom instances
    	 * from the result set into a <code>java.util.List</code>.
    	 * </p>
    	 *  
         * @param graph The {@link HyperGraph} to run the query against.
         * @param condition The query condition constraining the result set.
         * @return A list of all results from the result set, dereferenced as <code>HGHandle</code>s
         * against the graph.
    	 */
    	public static <T> List<T> getAll(final HyperGraph graph, final HGQueryCondition condition)
    	{
        	return graph.getTransactionManager().ensureTransaction(new Callable<List<T>>() {
            	public List<T> call()
            	{
            		ArrayList<Object> result = new ArrayList<Object>();
            		HGSearchResult<HGHandle> rs = null;
            		try
            		{
            			rs = graph.find(condition);
            			while (rs.hasNext())
            				result.add(graph.get(rs.next()));
            			return (List<T>)result;
            		}
            		finally
            		{
            			if (rs != null) rs.close();
            		}	
            	}
            	}, HGTransactionConfig.READONLY);    		     		
    	}
    	
    	/**
    	 * <p>
    	 * Execute the given query, put all the elements from the result set in a <code>List</code>
    	 * and return that <code>List</code> 
    	 * </p>
    	 */
    	public static <T> List<T> findAll(final HGQuery<T> query)
    	{
        	return query.getHyperGraph().getTransactionManager().ensureTransaction(new Callable<List<T>>() {
            	public List<T> call()
            	{
                	
            		ArrayList<T> result = new ArrayList<T>();
            		HGSearchResult<T> rs = null;
            		try
            		{
            			rs = query.execute();
            			while (rs.hasNext())
            				result.add(rs.next());
            			return result;
            		}
            		finally
            		{
            			if (rs != null) rs.close();
            		}      	
            	}
            	}, HGTransactionConfig.READONLY);		
    	}
    	
    	/**
    	 * <p>
    	 * Execute the given query, put all atom instances
    	 * from the result set into a <code>java.util.List</code>. 
    	 * </p>
    	 */
    	public static <T> List<T> getAll(final HGQuery<HGHandle> query)
    	{
        	return query.getHyperGraph().getTransactionManager().ensureTransaction(new Callable<List<T>>() {
            	public List<T> call()
            	{
            		ArrayList<Object> result = new ArrayList<Object>();
            		HGSearchResult<HGHandle> rs = null;
            		try
            		{
            			rs = query.execute();
            			while (rs.hasNext())
            				result.add(query.getHyperGraph().get(rs.next()));
            			return (List<T>)result;
            		}
            		finally
            		{
            			if (rs != null) rs.close();
            		}	
            	}
            	}, HGTransactionConfig.READONLY);    		     		
    	}
   }
}
