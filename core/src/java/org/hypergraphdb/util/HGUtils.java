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
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
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
	public static boolean isEmpty(String s)
	{
		return s == null || s.length() == 0;
	}
	
	public static RuntimeException throwRuntimeException(Throwable t)
	{
	    if (t instanceof RuntimeException)
	        throw (RuntimeException)t;
	    else if (t instanceof Error)
	        throw (Error)t;
	    else
	        throw new RuntimeException(t);
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
	@SuppressWarnings("unchecked")
	public static <T> long queryBatchProcess(HGQuery<T> query, 
											 Mapping<T, Boolean> F, 
											 int batchSize,
											 T startAfter,
											 long first)
	{
		HGRandomAccessResult<T> rs = null;
		T lastProcessed = startAfter;
		long totalProcessed = 0;
		while (true)
		{			
			query.getHyperGraph().getTransactionManager().beginTransaction();			
			try
			{
				rs = (HGRandomAccessResult<T>)query.execute();
				if (lastProcessed == null)
				{
					while (first > 0)
					{
						if (!rs.hasNext())
						{
							query.getHyperGraph().getTransactionManager().endTransaction(false);
							return totalProcessed;
						}
						else 
							rs.next();
						first--;
					}
				}
				else
				{
					GotoResult gt = rs.goTo(lastProcessed, false);
					if (gt == GotoResult.nothing) // last processed was actually last element in result set
					{
						query.getHyperGraph().getTransactionManager().endTransaction(false);
						return totalProcessed;
					}
					else if (gt == GotoResult.found)
					{
						if (!rs.hasNext())
						{
							query.getHyperGraph().getTransactionManager().endTransaction(false);
							return totalProcessed;
						}
						else
							rs.next();
					} // else we are already positioned after the last processed, which is not present for god know why?
				}				
				double start = System.currentTimeMillis();
				for (int i = 0; i < batchSize; i++)
				{
					T x = rs.current();
					if (!F.eval(x))
					{
						rs.close();
						rs = null;						
						query.getHyperGraph().getTransactionManager().endTransaction(true);
						return totalProcessed;
					}
					lastProcessed = x;	
					totalProcessed++;
					if (!rs.hasNext())
						break;
					else
						rs.next();					
				}
				rs.close();
				rs = null;
				query.getHyperGraph().getTransactionManager().endTransaction(true);
				double end = System.currentTimeMillis();
				System.out.println("Batch time " + (end - start) / 1000.0 + "s");
				System.out.println("Total processed " + totalProcessed);
			}
			catch (Throwable t)
			{
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
}