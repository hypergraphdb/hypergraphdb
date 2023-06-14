/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.lmdb;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.util.HGUtils;
import org.lmdbjava.GetOp;
import org.lmdbjava.SeekOp;

/**
 * 
 * <p>
 * Scans the key elements of an index. Similar to KeyRangeForwardResultSet, but
 * instead of returning the data, it returns the keys. 
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class KeyScanResultSet<BufferType, T> extends IndexResultSet<BufferType, T>
{
	@Override
	protected T advance()
	{
    	checkCursor();
        try
        {
        	if (cursor.cursor().seek(SeekOp.MDB_NEXT_NODUP))
        	{
        		byte [] data = this.hgBufferProxy.toBytes(cursor.cursor().key());
        		return converter.fromByteArray(data, 0, data.length);
        	}
        	else
        		return null;
        }
        catch (Throwable t)
        {
            closeNoException();
            throw new HGException(t);
        }            
    }

	@Override
	protected T back()
	{
        try
        {
    		checkCursor();
        	if (cursor.cursor().seek(SeekOp.MDB_PREV_NODUP))
        	{
        		byte [] data = this.hgBufferProxy.toBytes(cursor.cursor().key());
        		return converter.fromByteArray(data, 0, data.length);
        	}
        	else
        		return null;
        }
        catch (Throwable t)
        {
            closeNoException();
            throw new HGException(t);
        }		
    }
    
    public KeyScanResultSet(LMDBTxCursor<BufferType> cursor, 
			BufferType key, 
			ByteArrayConverter<T> converter,
			HGBufferProxyLMDB<BufferType> hgBufferProxy)
    {
    	super(cursor, key, converter, hgBufferProxy);
    	
        this.converter = converter;
        this.cursor = cursor;

        if (key != null)
		    try
		    {
		        cursor.cursor().get(key, GetOp.MDB_SET);
		        byte [] keydata = this.hgBufferProxy.toBytes(key);
		        next = converter.fromByteArray(keydata, 0, keydata.length);
		        lookahead = 1;
		    }
		    catch (Throwable t)
		    {
		        throw new HGException(t);
		    }         
    } 	
    
    public GotoResult goTo(T value, boolean exactMatch)
    {
    	byte [] keydata = converter.toByteArray(value);
    	this.key = this.hgBufferProxy.fromBytes(keydata);
    	try
    	{
    		if (exactMatch)
    		{
				if (cursor.cursor().get(key, GetOp.MDB_SET))
				{
    				positionToCurrent(keydata);
    				return GotoResult.found;
    			}
    			else
    				return GotoResult.nothing;
    		}
    		else 
    		{
				if (cursor.cursor().get(key, GetOp.MDB_SET_RANGE))
				{
					byte [] closest = this.hgBufferProxy.toBytes(key);
    				positionToCurrent(closest);
    				return HGUtils.eq(keydata, closest) ? GotoResult.found : GotoResult.close;
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
    
	public boolean isOrdered()
	{
		return true;
	}    
}