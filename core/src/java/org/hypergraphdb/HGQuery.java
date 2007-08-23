/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb;

import java.util.ArrayList;
import java.util.List;

import org.hypergraphdb.query.*;
import org.hypergraphdb.query.impl.DerefMapping;
import org.hypergraphdb.query.impl.LinkProjectionMapping;
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
public abstract class HGQuery 
{    
	public final static HGQuery NOP = new HGQuery()
	{
		public HGSearchResult execute() { return HGSearchResult.EMPTY; }
	};
	
/*	public static HGQuery make(HyperGraph hg, String expression)
	{
		return new ExpressionBasedQuery(hg, expression);
	} */

	public static HGQuery make(HyperGraph hg, HGQueryCondition condition)
	{
		return new ExpressionBasedQuery(hg, condition);
	}

	public abstract HGSearchResult execute();
    
    /**
     * <p>
     * This class serves as a namespace to a set of syntactically concise functions
     * for constructing HyperGraph query conditions and performing HyperGraph queries. 
     * With a Java 5+ compiler, you can import the class into your file's namespace 
     * and build HG condition with a much
     * simpler syntax than constructing the expression tree explicitely. For example:
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
        public static HGQueryCondition type(HGHandle h) { return new AtomTypeCondition(h); }
        public static HGQueryCondition type(Class c) { return new AtomTypeCondition(c); }
        public static HGQueryCondition subsumes(HGHandle h) { return new SubsumesCondition(h); }
        public static HGQueryCondition subsumed(HGHandle h) { return new SubsumedCondition(h); }
        
        public static HGQueryCondition and(HGQueryCondition...clauses)
        {
            And and = new And();
            for (HGQueryCondition x:clauses)
                and.add(x); 
            return and;            
        }
        public static HGQueryCondition or(HGQueryCondition...clauses)
        {
            Or or = new Or();
            for (HGQueryCondition x:clauses)
                or.add(x);
            return or;            
        }
        public static HGQueryCondition not(HGAtomPredicate c) { return new Not(c); }
        
        public static HGQueryCondition incident(HGHandle h) { return new IncidentCondition(h); }
        public static HGQueryCondition link(HGHandle...h) { return new LinkCondition(h); }
        public static HGQueryCondition orderedLink(HGHandle...h) { return new OrderedLinkCondition(h); }
        public static HGQueryCondition arity(int i) { return new ArityCondition(i); }
       
        public static HGQueryCondition value(Object value, ComparisonOperator op) { return new AtomValueCondition(value, op); }
        public static HGQueryCondition eq(Object x) { return value(x, ComparisonOperator.EQ); }
        public static HGQueryCondition lt(Object x) { return value(x, ComparisonOperator.LT); }        
        public static HGQueryCondition gt(Object x) { return value(x, ComparisonOperator.GT); }
        public static HGQueryCondition lte(Object x) { return value(x, ComparisonOperator.LTE); }
        public static HGQueryCondition gte(Object x) { return value(x, ComparisonOperator.GTE); }
        
        public static HGQueryCondition part(String path, Object value, ComparisonOperator op) {  return new AtomPartCondition(path.split("\\."), value, op); }
        public static HGQueryCondition eq(String path, Object x) { return part(path, x, ComparisonOperator.EQ); }
        public static HGQueryCondition lt(String path, Object x) { return part(path, x, ComparisonOperator.LT); }        
        public static HGQueryCondition gt(String path, Object x) { return part(path, x, ComparisonOperator.GT); }
        public static HGQueryCondition lte(String path, Object x) { return part(path, x, ComparisonOperator.LTE); }
        public static HGQueryCondition gte(String path, Object x) { return part(path, x, ComparisonOperator.GTE); }
        
        public static HGQueryCondition apply(Mapping m, HGQueryCondition c) { return new MapCondition(c, m); }
        public static Mapping linkProjection(int targetPosition) { return new LinkProjectionMapping(targetPosition); }
        public static Mapping deref(HyperGraph graph) { return new DerefMapping(graph); }
        public static HGQueryCondition all() { return new AnyAtomCondition(); }
        
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
    	public static Object getOne(HyperGraph graph, HGQueryCondition condition)
    	{
    		HGHandle h = findOne(graph, condition);
    		return h == null ? null : graph.get(h);
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
    	public static HGHandle findOne(HyperGraph graph, HGQueryCondition condition)
    	{
    		HGSearchResult<HGHandle> rs = null;
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
    	public static <T> List<T> findAll(HyperGraph graph, HGQueryCondition condition)
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
    	public static List<Object> getAll(HyperGraph graph, HGQueryCondition condition)
    	{
    		ArrayList<Object> result = new ArrayList<Object>();
    		HGSearchResult<HGHandle> rs = null;
    		try
    		{
    			rs = graph.find(condition);
    			while (rs.hasNext())
    				result.add(graph.get(rs.next()));
    			return result;
    		}
    		finally
    		{
    			if (rs != null) rs.close();
    		}      		
    	}     	
    }
}