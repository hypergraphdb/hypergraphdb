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
import org.hypergraphdb.storage.bdb.IndexResultSet;

import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;

/**
 * <p>
 * Implements a BerkeleyDB <code>Cursor</code> based result set that iterates over
 * all duplicates of a given key.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class SingleKeyResultSet<T> extends IndexResultSet<T>
{
	private boolean ordered = false;
    
    public SingleKeyResultSet(BDBTxCursor cursor, DatabaseEntry key, ByteArrayConverter<T> converter)
    {
        super(cursor, key, converter);
        try
        {
        	ordered = cursor.cursor().getDatabase().getConfig().getSortedDuplicates();
        }
        catch (Throwable t)
        {
        	throw new HGException(t);
        }
    }   
    
    protected T advance()
    {
        try
        {
            OperationStatus status = cursor.cursor().getNextDup(key, data, LockMode.DEFAULT);
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
        try
        {
            OperationStatus status = cursor.cursor().getPrevDup(key, data, LockMode.DEFAULT);
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
    
    public boolean isOrdered()
    {
    	return ordered;
    }
}
