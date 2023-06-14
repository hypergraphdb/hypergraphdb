/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.lmdb;

import java.util.NoSuchElementException;
import static org.hypergraphdb.storage.lmdb.LMDBUtils.checkArgNotNull;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.util.CountMe;
import org.hypergraphdb.util.HGUtils;
import org.lmdbjava.SeekOp;

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
public abstract class IndexResultSet<BufferType, T> implements HGRandomAccessResult<T>, CountMe
{        
	private static final Object UNKNOWN = new Object();
    protected LMDBTxCursor<BufferType> cursor;
    protected Object current = UNKNOWN, prev = UNKNOWN, next = UNKNOWN;    
    protected BufferType key;        
    protected BufferType data;
    protected ByteArrayConverter<T> converter;
    protected HGBufferProxyLMDB<BufferType> hgBufferProxy;
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
        
    protected final void moveNext()
    {
        prev = current;
        current = next;
        next = UNKNOWN;
        lookahead--;
    }
    
    protected final void movePrev()
    {
        next = current;
        current = prev;
        prev = UNKNOWN;
        lookahead++;
    }
    
    protected abstract T advance();
    protected abstract T back();
    
    /**
     * <p>Construct an empty result set.</p>
     */
    protected IndexResultSet()
    {
    }
    
    /**
     * <p>Construct a result set matching a specific key.</p>
     * 
     * @param cursor
     * @param key The key of this result set. <b>IMPORTANT: Please note that we are not making a defensive copy
     * of this key, we assume the content will not change.</b>
     */
    public IndexResultSet(LMDBTxCursor<BufferType> cursor, 
    					  BufferType key, 
    					  ByteArrayConverter<T> converter,
    					  HGBufferProxyLMDB<BufferType> hgBufferProxy)
    {
    	checkArgNotNull(cursor, "cursor");
    	
    	this.converter = converter;
    	this.hgBufferProxy = hgBufferProxy;
    	this.cursor = cursor;
    	this.key = key;
    	this.data = cursor.cursor().val();
    	
    	try
	    {    		
			byte [] value = this.hgBufferProxy.toBytes(this.data);
	        next = converter.fromByteArray(value, 0, value.length);
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
    		if (cursor.cursor().get(key, data, SeekOp.MDB_FIRST_DUP))
            {
                current = UNKNOWN;
                prev = null;
                byte [] value = this.hgBufferProxy.toBytes(data);
                next = converter.fromByteArray(value, 0, value.length);
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
     		if (cursor.cursor().get(key, data, SeekOp.MDB_LAST_DUP))
            {
                current = UNKNOWN;
                next = null;
                byte [] value = this.hgBufferProxy.toBytes(data);
                prev = converter.fromByteArray(value, 0, value.length);
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
    	this.data = this.hgBufferProxy.fromBytes(B);
    	try
    	{
    		if (exactMatch)
    		{
    			if (cursor.cursor().get(key, data, SeekOp.MDB_GET_BOTH))
    			{
    				positionToCurrent(this.hgBufferProxy.toBytes(data));
    				return GotoResult.found; 
    			}
    			else
    				return GotoResult.nothing;
    		}
    		else 
    		{    			
    			if (cursor.cursor().get(key, data, SeekOp.MDB_GET_BOTH_RANGE))
    			{
    				byte [] read = this.hgBufferProxy.toBytes(data);
    				GotoResult result = HGUtils.eq(B, read) ? GotoResult.found : GotoResult.close; 
    				positionToCurrent(read);
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
    	catch (Exception ex)
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
