/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.util;

import java.io.PrintStream;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGSearchResult;

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
	public static boolean isEmpty(String s)
	{
		return s == null || s.length() == 0;
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
	
	/**
	 * <p>Return an object's hash code or 0 if the object is <code>null</code>.</p>
	 */
	public static int hashIt(Object o)
	{
	    if (o == null)
	        return 0;
	    else
	        return o.hashCode();
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
	
	@SuppressWarnings("unchecked")
	public static <T> Class<T> loadClass(String classname) throws ClassNotFoundException
	{
		return (Class<T>)Thread.currentThread().getContextClassLoader().loadClass(classname);		
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
}