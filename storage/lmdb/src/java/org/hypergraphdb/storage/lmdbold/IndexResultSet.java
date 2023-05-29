/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.lmdb;

import java.util.NoSuchElementException;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.util.CountMe;
import org.hypergraphdb.util.HGUtils;

import static org.hypergraphdb.storage.lmdb.LMDBUtils.checkArgNotNull;

import org.fusesource.lmdbjni.CursorOp;
import org.fusesource.lmdbjni.DatabaseEntry;
import org.fusesource.lmdbjni.LMDBException;
import org.fusesource.lmdbjni.OperationStatus;

/**
 * <p>
 * An <code>IndexResultSet</code> is based on a cursor over an indexed set of values.
 * Implementation of complex query execution may move the cursor position based on some
 * index key to speed up query processing.  
 * </p>
 * 
 * @author Borislav Iordanov
 */
@SuppressWarnings("unchecked")
public abstract class IndexResultSet<T> implements HGRandomAccessResult<T>, CountMe
{        
		private static final Object UNKNOWN = new Object();
    protected LmdbTxCursor cursor;
    protected Object current = UNKNOWN, prev = UNKNOWN, next = UNKNOWN;    
    protected DatabaseEntry key;        
    protected DatabaseEntry data;
    protected ByteArrayConverter<T> converter;
    protected int lookahead = 0;
    
    protected final void closeNoException()
    {
        try { close(); } catch (Throwable t) { }
    }
    
    protected final void checkCursor()
    {
        if (cursor == null || !cursor.isOpen())
            throw new HGException(
                    "DefaultIndexImpl.IndexResultSet: attempt to perform an operation on a closed or invalid cursor.");
    }
    
    /**
     * 
     * <p>
     * Copy <code>data</code> into the <code>entry</code>. Adjust <code>entry</code>'s
     * byte buffer if needed.
     * </p>
     *
     * @param dbEntry
     * @param data
     */
    protected void assignData(DatabaseEntry dbEntry, byte [] data)
    {
    	byte [] dest = dbEntry.getData();
    	if (dest == null || dest.length != data.length)
    	{
    		dest = new byte[data.length];
    		dbEntry.setData(dest);
    	}
    	System.arraycopy(data, 0, dest, 0, data.length);
//    	System.out.println(dbEntry.getData());
    }
    
    protected final void moveNext()
    {
//        checkCursor();
        prev = current;
        current = next;
        next = UNKNOWN;
        lookahead--;
/*        while (true)
        {
        	next = advance();
        	if (next == null)
        		break;
        	if (++lookahead == 1)
        		break;
        } */
    }
    
    protected final void movePrev()
    {
//        checkCursor();
        next = current;
        current = prev;
        prev = UNKNOWN;
        lookahead++;
/*        while (true)
        {
        	prev = back();
        	if (prev == null)
        		break;
        	if (--lookahead == -1)
        		break;
        } */
    }
    
    protected abstract T advance();
    protected abstract T back();
    
    /**
     * <p>Construct an empty result set.</p>
     */
    protected IndexResultSet()
    {
    }
    
//    static int idcounter = 0;    
//    int id = 0;
    
    /**
     * <p>Construct a result set matching a specific key.</p>
     * 
     * @param cursor
     * @param key
     */
    public IndexResultSet(LmdbTxCursor cursor, DatabaseEntry keyIn, ByteArrayConverter<T> converter)
    {
    	checkArgNotNull(cursor, "cursor");
    	
      this.converter = converter;
      this.cursor = cursor;
      this.key = new DatabaseEntry();
      this.data = new DatabaseEntry();
      if (keyIn != null)
      	assignData(this.key, keyIn.getData());

      try
	    {
	        cursor.cursor().get(CursorOp.GET_CURRENT, key, data);
	        next = converter.fromByteArray(data.getData(), 0, data.getData().length);
	        lookahead = 1;
	    }
	    catch (Throwable t)
	    {
	        throw new HGException(t);
	    }
    }

    protected void positionToCurrent(byte [] data)
    {
    		current = converter.fromByteArray(data, 0, data.length);
       	lookahead = 0;
        prev = next = UNKNOWN;
    }
    
    public void goBeforeFirst()
    {
    		checkCursor();
        try
        {
        		OperationStatus status = cursor.cursor().get(CursorOp.FIRST, key, data);
        		if (status == OperationStatus.SUCCESS)
            {
                current = UNKNOWN;
                prev = null;
                next = converter.fromByteArray(data.getData(), 0, data.getData().length);
                lookahead = 1;
            }
            else
            {
                prev = next = null;
                current = UNKNOWN;
                lookahead = 0;
            }
        }
        catch (Throwable t)
        {
            closeNoException();
            throw new HGException(t);
        }            
    }
    
    public void goAfterLast()
    {
    		checkCursor();
        try
        {
         		OperationStatus status = cursor.cursor().get(CursorOp.LAST, key, data);
         		if (status == OperationStatus.SUCCESS)
            {
                current = UNKNOWN;
                next = null;
                prev = converter.fromByteArray(data.getData(), 0, data.getData().length);
                lookahead = -1;
            }
            else
            {
                prev = next = null;
                current = UNKNOWN;
                lookahead = 0;
            }            
        }
        catch (Throwable t)
        {
            closeNoException();
            throw new HGException(t);
        }        
    }
    
    public GotoResult goTo(T value, boolean exactMatch)
    {
  		checkCursor();
    	byte [] B = converter.toByteArray(value);
    	assignData(data, B);
    	try
    	{
    	  OperationStatus status = null;
    		if (exactMatch)
    		{
    			status = cursor.cursor().get(CursorOp.GET_BOTH, key, data);
    			if (status == OperationStatus.SUCCESS)
    			{
    				positionToCurrent(data.getData());
    				return GotoResult.found; 
    			}
    			else
    				return GotoResult.nothing;
    		}
    		else 
    		{
    			status = cursor.cursor().get(CursorOp.GET_BOTH_RANGE, key, data);
    			if (status == OperationStatus.SUCCESS)
    			{
    				GotoResult result = HGUtils.eq(B, data.getData()) ? GotoResult.found : GotoResult.close; 
    				positionToCurrent(data.getData());
    				return result;
    			}    				
    			else
    				return GotoResult.nothing;
    		}
    	}
    	catch (Throwable t)
    	{
    		closeNoException();
    		throw new HGException(t);
    	}
    }
    
    public final void close()
    {
        if (cursor == null)
            return;

        try
        {
            current = next = prev = UNKNOWN;
            key = null;
            cursor.close();
        }
        catch (Throwable t)
        {
            throw new HGException("Exception while closing a DefaultIndexImpl cursor: " +
                                  t.toString(), t);
        }
        finally
        {
            cursor = null;
        }
    }
    
    public final T current()     
    {
    	if (current == UNKNOWN)
    		throw new NoSuchElementException();
    	return (T)current; 
    }
    
    public final boolean hasPrev()    
    {
    	if (prev == UNKNOWN)
    	{
    	    while (lookahead > -1)
    	    {
    	        prev = back();
    	        if (prev == null)
    	            break;
    	        lookahead--;
    	    }
//    		prev = back();
    	}
    	return prev != null; 
    }
    
    public final boolean hasNext()    
    { 
    	if (next == UNKNOWN)
    	{
            while (lookahead < 1)
            {
                next = advance();
                if (next == null)
                    break;
                lookahead++;
            }    	    
//    		next = advance();
    	}
    	return next != null; 
    }
    
    public final T prev()        
    { 
    	if (!hasPrev())
    		throw new NoSuchElementException();
    	movePrev(); 
    	return current(); 
    }
            
    public final T next()        
    { 
    	if (!hasNext())
    		throw new NoSuchElementException();
    	moveNext(); 
    	return current(); 
    }
    
    public final void remove()
    {
        throw new UnsupportedOperationException(
                "HG - IndexResultSet does not implement remove.");
    }
    
    public int count()
    {
    	checkCursor();
    	try
    	{
    		return (int)cursor.cursor().count();
    	}
    	catch (LMDBException ex)
    	{
    		throw new HGException(ex);
    	}
    }
    
    /**
     * Remove current element. After that cursor becomes invalid, so next(), prev()
     * operations will fail. However, a goTo operation should work. 
     */
    public void removeCurrent()
    {
    	checkCursor();
    	try
    	{
    		cursor.cursor().delete();
    	}
    	catch (Exception ex)
    	{
    		throw new HGException(ex);
    	}
    }
    
    public void reset() 
    {
      current = UNKNOWN;
      prev = UNKNOWN;
      next = UNKNOWN;    
      lookahead = 0;
    }
}
