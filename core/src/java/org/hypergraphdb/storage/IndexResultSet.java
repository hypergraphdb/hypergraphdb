/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.storage;

import java.util.NoSuchElementException;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.transaction.BDBTxCursor;
import org.hypergraphdb.util.HGUtils;

import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.OperationStatus;

/**
 * <p>
 * An <code>IndexResultSet</code> is based on a cursor over an indexed set of values.
 * Implementation of complex query execution may move the cursor position based on some
 * index key to speed up query processing.  
 * </p>
 * 
 * @author Borislav Iordanov
 */
public abstract class IndexResultSet<T> implements HGRandomAccessResult<T>
{        
    protected BDBTxCursor cursor;
    protected T current, prev, next;
    protected int lookahead = 0;
    protected DatabaseEntry key;        
    protected DatabaseEntry data = new DatabaseEntry();
    protected ByteArrayConverter<T> converter;
    
    protected final void closeNoException()
    {
        try { close(); } catch (Throwable t) { }
    }
    
    protected final void checkCursor()
    {
        if (!cursor.isOpen())
            throw new HGException(
                    "DefaultIndexImpl.IndexResultSet: attempt to perform an operation on a closed or invalid cursor.");            
    }
    
    protected final void moveNext()
    {
        checkCursor();
        prev = current;
        current = next;
        lookahead--;
        while (true)
        {
        	next = advance();
        	if (next == null)
        		break;
        	if (++lookahead == 1)
        		break;
        }
    }
    
    protected final void movePrev()
    {
        checkCursor();
        next = current;
        current = prev;
        lookahead++;
        while (true)
        {
        	prev = back();
        	if (prev == null)
        		break;
        	if (--lookahead == -1)
        		break;
        }
    }
    
    protected abstract T advance();
    protected abstract T back();
    
    /**
     * <p>Construct an empty result set.</p>
     */
    public IndexResultSet()
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
    public IndexResultSet(BDBTxCursor cursor, DatabaseEntry key, ByteArrayConverter<T> converter)
    {
/*        id = idcounter++;
        System.out.println("Constructing index set with id " + id);
        StackTraceElement e[]=Thread.currentThread().getStackTrace();
        for (int i=0; i <e.length; i++) {
             System.out.println(e[i]);
            } */
        this.converter = converter;
        this.cursor = cursor;
        this.key = key == null ? new DatabaseEntry() : key;
	    try
	    {
	        cursor.cursor().getCurrent(key, data, LockMode.DEFAULT);
	        next = converter.fromByteArray(data.getData());
	        lookahead = 1;
	    }
	    catch (Throwable t)
	    {
	        throw new HGException(t);
	    }
    }

    protected void positionToCurrent(byte [] data)
    {
		current = converter.fromByteArray(data);
        prev = back();
        if (prev != null)
        	advance();  	
		next = advance();		
        if (next != null) 
        	lookahead = 1;
        else 
        	lookahead = 0;
    }
    
    public GotoResult goTo(T value, boolean exactMatch)
    {
    	byte [] B = converter.toByteArray(value);
    	data.setData(B);
    	try
    	{
    		if (exactMatch)
    		{
    			if (cursor.cursor().getSearchBoth(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
    			{
    				positionToCurrent(data.getData());
    				return GotoResult.found; 
    			}
    			else
    				return GotoResult.nothing;
    		}
    		else 
    		{
    			byte [] save = new byte[data.getData().length];
    			System.arraycopy(data.getData(), 0, save, 0, save.length);   
    			if (cursor.cursor().getSearchBothRange(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
    			{
    				GotoResult result = HGUtils.eq(save, data.getData()) ? GotoResult.found : GotoResult.close; 
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
            current = next = prev = null;
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
    	if (current == null)
    		throw new NoSuchElementException();
    	return current; 
    }
    
    public final boolean hasPrev()    
    { 
    	return prev != null; 
    }
    
    public final boolean hasNext()    
    { 
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
    
    protected void finalize()
    {
/*        if (cursor != null)
        {
            
            System.out.print("WARNING: set id " + id +  " closing unclosed cursor in finalizer method -- DB is: "); 
            try
            {
                System.out.println(cursor.getDatabase().getDatabaseName());
            }
            catch (Exception ex)
            {
                ex.printStackTrace(System.err);
            }
        } */
        closeNoException();
    }
    
    public int count()
    {
    	try
    	{
    		return cursor.cursor().count();
    	}
    	catch (DatabaseException ex)
    	{
    		throw new HGException(ex);
    	}
    }    
}