/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.bdb;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.util.HGUtils;

import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;

class KeyRangeBackwardResultSet<T> extends IndexResultSet<T>
{    
    private DatabaseEntry initialKey = null;
    
    protected T advance()
    {
        try
        {
            OperationStatus status = cursor.cursor().getPrev(key, data, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS)
                return converter.fromByteArray(data.getData(), data.getOffset(), data.getSize());
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
            OperationStatus status = cursor.cursor().getNext(key, data, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS)
                return converter.fromByteArray(data.getData(), data.getOffset(), data.getSize());
            else
                return null;
        }
        catch (Throwable t)
        {
            closeNoException();
            throw new HGException(t);
        }                        
    }

    public KeyRangeBackwardResultSet(BDBTxCursor cursor, DatabaseEntry key,  ByteArrayConverter<T> converter)
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