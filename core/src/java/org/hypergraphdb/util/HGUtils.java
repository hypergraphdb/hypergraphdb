/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.File;
import java.io.PrintStream;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGRandomAccessResult.GotoResult;

/**
 * 
 * <p>
 * The mandatory bag of static utility method class.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class HGUtils
{
	public static final HGPersistentHandle [] EMPTY_HANDLE_ARRAY = new HGPersistentHandle[0];
    
	public static boolean isEmpty(String s)
	{
		return s == null || s.length() == 0;
	}
	
	public static void throwRuntimeException(Throwable t)
	{
	    if (t instanceof RuntimeException)
	        throw (RuntimeException)t;
	    else if (t instanceof Error)
	        throw (Error)t;
	    else
	        throw new HGException(t);
	}
	
    public static Throwable getRootCause(Throwable t)
    {
    	if (t != null)
    		while (t.getCause() != null)
    			t = t.getCause();
        return t;
    }
    
    /**
     * <p>
     * Compare two objects for equality, checking for <code>null</code> values as well.
     * </p>
     */
	public static boolean eq(Object left, Object right)
	{
	    if (left == right)
	        return true;
	    else if (left == null)
	        return false;
	    else if (right == null)
	        return false;        
	    else
	        return left.equals(right);
	}

	/**
	 * <p>Compare two arrays for equality. This will perform a deep comparison, return
	 * <code>true</code> if and only if all elements of the passed in arrays are equal.</p> 
	 */
	public static boolean eq(Object [] left, Object [] right)
	{
        if (left == right)
                return true;
        else if (left == null || right == null)
                return false;
        else if (left.length != right.length)
                return false;
        for (int i = 0; i < left.length; i++)
                if (!eq(left[i], right[i]))
                        return false;
        return true;                    
	}

	public static boolean eq(byte [] left, byte [] right)
	{
		if (left == right)
			return true;
		else if (left == null || right == null || left.length != right.length)
			return false;
		for (int i = 0; i < left.length; i++)
			if (left[i] != right[i])
				return false;
		return true;
	}
	
	public static boolean eq(Ref<?> r1, Ref<?> r2)
	{
		if (r1 == null) return r2 == null;
		else if (r2 == null) return false;
		else return HGUtils.eq(r1.get(), r2.get());
	}

	public static boolean eq(Ref<?> [] left, Ref<?> [] right)
	{
        if (left == right)
                return true;
        else if (left == null || right == null)
                return false;
        else if (left.length != right.length)
                return false;
        for (int i = 0; i < left.length; i++)
                if (!eq(left[i], right[i]))
                        return false;
        return true;                    
	}
	
	/**
	 * <p>Return an object's hash code or 0 if the object is <code>null</code>.</p>
	 */
	public static int hashIt(Object o)
	{
		return HashCodeUtil.hash(0, o);
	}
	
	/**
	 * <p>Return a composite hash code of two objects.</p>
	 */
	public static int hashThem(Object one, Object two)
	{
	    return hashIt(one) | hashIt(two);
	}

	/**
	 * <p>
	 * Print the full stack trace of a <code>Throwable</code> object into a
	 * string buffer and return the corresponding string.  
	 * </p>
	 */
	public static String printStackTrace(Throwable t)
	{
	        java.io.StringWriter strWriter = new java.io.StringWriter();
	        java.io.PrintWriter prWriter = new java.io.PrintWriter(strWriter);
	        t.printStackTrace(prWriter);
	        prWriter.flush();
	        return strWriter.toString();
	}
	
	public static void printStackTrace(StackTraceElement [] trace, PrintStream out)
	{
		for (StackTraceElement el : trace)
			out.println(el.toString());
	}
	
	public static void logCallStack(PrintStream out)
	{
		printStackTrace(Thread.currentThread().getStackTrace(), out);
	}

	public static void closeNoException(HGSearchResult<?> rs)
	{
		if (rs == null) return;
		try { rs.close(); } catch (Throwable t) { }
	}
	
	public static void wrapAndRethrow(Throwable t)
	{
		if (t instanceof RuntimeException)
			throw (RuntimeException)t;
		else if (t instanceof Error)
			throw (Error)t;
		else
			throw new HGException(t);
	}
	
	/**
	 * <p>
	 * Load a class using the class loader configured for the passed in {@link HyperGraph} instance, if
	 * such a loader was configured. If no class loader specific to the DB instance was configured, try
	 * the thread context class loader. Finally, fall back to <code>HGUtils.class.getClassLoader()</code>.
	 * </p>
	 *
	 * <p>
	 * If you have a DB configured class loader that you need to override with a thread context
	 * class loader, you'd need to implement the delegating yourself.  
	 * </p>
	 * 
	 * @param <T>
	 * @param graph
	 * @param classname
	 * @return
	 * @throws ClassNotFoundException
	 */
	public static <T> Class<T> loadClass(HyperGraph graph, String classname) throws ClassNotFoundException
	{
		return loadClass(graph.getConfig(), classname);
	}
	
	/**
	 * <p>
	 * Load a class using the class loader configured in the {@link HGConfiguration} object, if
	 * such a loader was configured. If there is no class loader in the configuration, try
	 * the thread context class loader. Finally, fall back to <code>HGUtils.class.getClassLoader()</code>.
	 * </p>
	 *
	 * <p>
	 * If you have a DB configured class loader that you need to override with a thread context
	 * class loader, you'd need to implement the delegating yourself.  
	 * </p>
	 * 
	 * @param <T>
	 * @param graph
	 * @param classname
	 * @return
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	public static <T> Class<T> loadClass(HGConfiguration config, String classname) throws ClassNotFoundException
	{
		ClassLoader loader =  getClassLoader(config);
		if(classname.startsWith("[L"))
			return (Class<T>)Array.newInstance(loader.loadClass(
					classname.substring(2, classname.length() - 1)), 0).getClass();
		else if (classname.startsWith("["))
			return (Class<T>)Class.forName(classname);
		else
			return (Class<T>)loader.loadClass(classname);
	}
	
	/**
	 * Returns the configured, the context or the class's classloader.
	 *  
	 * @param graph
	 * @return never null.
	 */
	public static ClassLoader getClassLoader(HyperGraph graph)
	{
		return getClassLoader(graph.getConfig());
	}
	
	/**
	 * Returns the configured, the context or the class's classloader.
	 *  
	 * @param config
	 * @return never null.
	 */
	public static ClassLoader getClassLoader(HGConfiguration config) 
	{
		ClassLoader loader = config.getClassLoader();
		if (loader == null)
			loader = Thread.currentThread().getContextClassLoader();
		if (loader == null)
			loader = HGUtils.class.getClassLoader();
		return loader;
	}
	
	public static HGHandle [] toHandleArray(HGLink link)
	{
		if (link == null)
			return null;
		HGHandle [] A = new HGHandle[link.getArity()];
		for (int i = 0; i < link.getArity(); i++)
			A[i] = link.getTargetAt(i);
		return A;
	}
	
	/**
	 * 
	 * <p>
	 * Process a potentially large query result in batches where each batch is a encapsulated
	 * in a single transaction. It is assumed that the result of the <code>query</code>
	 * is a <code>HGRandomAccessResult</code> so it is possible to quickly position at
	 * the beginning of the next batch.  
	 * </p>
	 *
	 * <p>
	 * The <code>startAfter</code> and <code>first</code> parameters define a starting point
	 * for the process. Use either one of them, but not both.
	 * </p>
	 * 
	 * @param <T>
	 * @param query The query that produces the result set to be scanned.
	 * @param F The function to perform on that query. The function takes a query result item
	 * and return a boolean which indicates whether to continue processing (if <code>true</code>)
	 * or stop (if <code>false</code>).
	 * @param batchSize The number of result items to encapsulate in a single transaction.
	 * @param startAfter Start processing at the first element after this parameter.
	 * @param first A 1-based index of the first element to process.
	 * @return The total number of elements processed. 
	 */
    public static <T> long queryBatchProcess(HGQuery<T> query, 
											 Mapping<T, Boolean> F, 
											 int batchSize,
											 T startAfter,
											 long first)
	{
		HGSearchResult<T> rs = null;
		T lastProcessed = startAfter;
		long totalProcessed = 0;
		while (true)
		{			
		    T txLastProcessed = lastProcessed;
		    int currentProcessed = 0;
			query.getHyperGraph().getTransactionManager().beginTransaction();			
			try
			{
				rs = query.execute();
				if (txLastProcessed == null) // if we are just starting
				{
					for (long i = first; i > 0; i--) // skip first 'first' entries
					{
						if (!rs.hasNext())
						{
							rs.close();
							rs = null;
							query.getHyperGraph().getTransactionManager().endTransaction(false);
							return totalProcessed;
						}
						else 
							rs.next();
					}
				}
				else // else, position to the end of the last successful batch
				{
					GotoResult gt = null;
					if (rs instanceof HGRandomAccessResult)
					{
						HGRandomAccessResult<T> rars = (HGRandomAccessResult<T>)rs;
						gt = rars.goTo(txLastProcessed, false);
					}
					else
					{
					    rs.close();
					    rs = null;
						throw new HGException("Batch processing starting at a specific element is only supported for HGRandomAccessResult.");
					}
					if (gt == GotoResult.nothing) // last processed was actually last element in result set
					{
					    rs.close();
					    rs = null;
						query.getHyperGraph().getTransactionManager().endTransaction(false);
						return totalProcessed;
					}
					else if (gt == GotoResult.found)
					{
						if (!rs.hasNext())
						{
						    rs.close();
						    rs = null;
							query.getHyperGraph().getTransactionManager().endTransaction(false);
							return totalProcessed;
						}
						else
							rs.next();
					} // else we are already positioned after the last processed, which is not present for god know why?
				}				
//				double start = System.currentTimeMillis();
				if (! (rs instanceof HGRandomAccessResult))
					batchSize = Integer.MAX_VALUE;
				int i;
				for (i = 0; i < batchSize; i++)
				{
					T x = rs.current();
					if (!F.eval(x))
					{
						rs.close();
						rs = null;						
						query.getHyperGraph().getTransactionManager().endTransaction(true);
						return totalProcessed + currentProcessed;
					}
					txLastProcessed = x;	
					currentProcessed++;
					if (!rs.hasNext())
						break;
					else
						rs.next();					
				}
				rs.close();
				rs = null;
				query.getHyperGraph().getTransactionManager().endTransaction(true);
				lastProcessed = txLastProcessed;
				totalProcessed += currentProcessed;
//				double end = System.currentTimeMillis();
				if (i < batchSize)
					return totalProcessed;
//				System.out.println("Batch time " + (end - start) / 1000.0 + "s");
//				System.out.println("Total processed " + totalProcessed);
			}
			catch (Throwable t)
			{
			    Throwable cause = getRootCause(t);
			    if (query.getHyperGraph().getStore().getTransactionFactory().canRetryAfter(cause))
			        continue;
				try { query.getHyperGraph().getTransactionManager().endTransaction(false); }
				catch (Throwable tt) { tt.printStackTrace(System.err); }
				throw new RuntimeException(t);
			}
			finally
			{
				closeNoException(rs);
			}
		}		
	}
	
    public static void directoryRecurse(File top, Mapping<File, Boolean> mapping) 
    {        
        File[] subs = top.listFiles();        
        if (subs != null) 
        {        
            for(File sub : subs)
            {
                if (sub.isDirectory()) 
                    directoryRecurse(sub, mapping);
                mapping.eval(sub);            
            }            
            mapping.eval(top);
        }        
    }	
    
    /**
     * <p>
     * Delete a {@link HyperGraph} by removing the filesystem directory that holds it.
     * This method will first make sure to close the HyperGraph if it's currently open.   
     * </p>
     * 
     * @param location The location of the graph instance.
     */
    public static void dropHyperGraphInstance(String location)
    {
//        System.out.println("DROP HGDB at "+ location);
        if (HGEnvironment.isOpen(location))
        {
            HGEnvironment.get(location).close();
        }
        directoryRecurse(new File(location), 
                         new Mapping<File, Boolean>()
                         {
                            public Boolean eval(File f)
                            {
                                return f.delete();
                            }
                         }
        );
    }
    
    @SuppressWarnings("unchecked")
    public static <T> Class<T> getImplementationClass(String interfaceClassName, String defaultImplementation)
    {
      try
      {
      	String className = System.getProperty(interfaceClassName);
      	if (className == null)
      		className = defaultImplementation;
      	return (Class<T>)Class.forName(className);
      }
      catch (Exception ex)
      {
      	throw new HGException(ex);
      }
    }
    
    public static <T> T getImplementationOf(String interfaceClassName, String defaultImplementation)
    {
      Class<T> cl = getImplementationClass(interfaceClassName, defaultImplementation);
      try
      {            
      	return cl.newInstance();
      }
      catch (Exception e)
      {
      	throw new HGException(e);
      }
    }

    public static <T> Set<T> set(T...members)
    {
        HashSet<T> S = new HashSet<T>();
        for (T x:members) 
            S.add(x);
        return S;
    }
    
    @SuppressWarnings("unchecked")
    public static <T> T cloneObject(T p, Mapping<Pair<Object, String>, Boolean> propertyFilter) throws Exception
    {
        if (p == null)
            return null;
        
        if (p instanceof Cloneable)
        {
            Method cloneMethod = p.getClass().getMethod("clone", (Class[])null);
            if (cloneMethod != null)
                return (T)cloneMethod.invoke(p, (Object[])null);

        }
        else if (p.getClass().isArray())
        {
            Object [] A = (Object[])p;
            Class<?> type = p.getClass(); 
            Object [] ac = (Object[])Array.newInstance(type.getComponentType(), A.length);
            for (int i = 0; i < A.length; i++)
                ac[i] = cloneObject(A[i], propertyFilter);
            return (T)ac;
        }
        else if (identityCloneClasses.contains(p.getClass()))
            return p;
        
        //
        // Need to implement cloning ourselves. We do this by copying bean properties.
        //
        Constructor<?> cons = null;
        
        try
        {
            cons = p.getClass().getConstructor((Class[])null);
        }
        catch (Throwable t)
        {
            return p;
        }
        
        Object copy = cons.newInstance((Object[])null);
        
        if (p instanceof Collection)
        {
            Collection<Object> cc = (Collection<Object>)copy;
            for (Object el : (Collection<?>)p)
                cc.add(cloneObject(el, propertyFilter));            
        }
        else if (p instanceof Map)
        {
            Map<Object, Object> cm = (Map<Object, Object>)copy;
            for (Object key : ((Map<Object, Object>)p).keySet())
                cm.put(key, cloneObject(((Map<Object, Object>)p).get(key), propertyFilter));
        }
        else
        {
            BeanInfo bean_info = Introspector.getBeanInfo(p.getClass());        
            PropertyDescriptor beanprops [] = bean_info.getPropertyDescriptors();
            if (beanprops == null || beanprops.length == 0)
                copy = p;
            else for (PropertyDescriptor desc : beanprops)
            {
                Method rm = desc.getReadMethod();
                Method wm = desc.getWriteMethod();
                if (rm == null || wm == null)
                    continue;
                Object value = rm.invoke(p, (Object[])null);
                if (propertyFilter == null || propertyFilter.eval(new Pair<Object, String>(p, desc.getName())))
                    value = cloneObject(value, propertyFilter); 
                wm.invoke(copy, new Object[] { value });
            }
        }
        return (T)copy;
    }
    
    private static void privateVisit(Object object, Mapping<Object, Boolean> visitor, IdentityHashMap<Object, Boolean> visited)
    {
        if (visited.containsKey(object))
            return;
        visitor.eval(object);
        visited.put(object, Boolean.TRUE);
        if (! (object instanceof Iterable))
            return;        
        for (Object x : (Iterable<?>)object)
            privateVisit(x, visitor, visited);
    }
    
    public static void visit(Object object, Mapping<Object, Boolean> visitor)
    {
        privateVisit(object, visitor, new IdentityHashMap<Object, Boolean>());
    }
    
    static final Set<Class<?>> identityCloneClasses = new HashSet<Class<?>>();
    static
    {
        identityCloneClasses.add(String.class);
        identityCloneClasses.add(Byte.class);
        identityCloneClasses.add(Short.class);
        identityCloneClasses.add(Integer.class);
        identityCloneClasses.add(Long.class);
        identityCloneClasses.add(Float.class);
        identityCloneClasses.add(Double.class);
        identityCloneClasses.add(Boolean.class);
        identityCloneClasses.add(Character.class);        
    }
}