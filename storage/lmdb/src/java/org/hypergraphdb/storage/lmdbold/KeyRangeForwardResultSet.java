/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.lmdb;

import org.fusesource.lmdbjni.CursorOp;
import org.fusesource.lmdbjni.DatabaseEntry;
import org.fusesource.lmdbjni.OperationStatus;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.util.HGUtils;

class KeyRangeForwardResultSet<T> extends IndexResultSet<T>
{    
    private DatabaseEntry initialKey = null;
    
    protected T advance()
    {
    		checkCursor();
        try
        {
        		OperationStatus status = cursor.cursor().get(CursorOp.NEXT, key, data);
            if (status == OperationStatus.SUCCESS)
            	return converter.fromByteArray(data.getData(), 0, data.getData().length);
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
        if (HGUtils.eq(key.getData(), initialKey.getData()))
            return null;
        try
        {
        		checkCursor();
        		OperationStatus status = cursor.cursor().get(CursorOp.PREV, key, data);
            if (status == OperationStatus.SUCCESS)
                return converter.fromByteArray(data.getData(), 0, data.getData().length);
            else
                return null;
        }
        catch (Throwable t)
        {
            closeNoException();
            throw new HGException(t);
        }                        
    }
    
    
    
    public KeyRangeForwardResultSet(LmdbTxCursor cursor, DatabaseEntry key, ByteArrayConverter<T> converter)
    {
        super(cursor, key, converter);
        initialKey = new DatabaseEntry();
        assignData(initialKey, key.getData());        
    }        
    
    public boolean isOrdered()
    {
    	return true;
    }
}
