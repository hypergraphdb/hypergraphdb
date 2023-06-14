/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.lmdb;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.util.HGUtils;
import org.lmdbjava.SeekOp;

class KeyRangeForwardResultSet<BufferType, T> extends IndexResultSet<BufferType, T>
{    
	byte [] initialKey;
	
    protected T advance()
    {
    	checkCursor();
        try
        {
        	if (cursor.cursor().seek(SeekOp.MDB_NEXT))
        	{
        		byte [] data = this.hgBufferProxy.toBytes(cursor.cursor().val());
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
    
    protected T back()
    {
    	SeekOp seekop = SeekOp.MDB_PREV;
        if (HGUtils.eq(this.hgBufferProxy.toBytes(this.key), initialKey))
            seekop = SeekOp.MDB_PREV_DUP;
        try
        {
    		checkCursor();
        	if (cursor.cursor().seek(seekop))
        	{
        		byte [] data = this.hgBufferProxy.toBytes(cursor.cursor().val());
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
    
    
    
    public KeyRangeForwardResultSet(LMDBTxCursor<BufferType> cursor, 
    								BufferType key, 
    								ByteArrayConverter<T> converter,
    		    					HGBufferProxyLMDB<BufferType> hgBufferProxy)
    {
        super(cursor, key, converter, hgBufferProxy);
        initialKey = this.hgBufferProxy.toBytes(key);        
    }        
    
    public boolean isOrdered()
    {
    	return true;
    }
}
