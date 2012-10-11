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

import com.sleepycat.db.SecondaryCursor;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;

/**
 * <p>A result set based on a BerkeleyDB secondary cursor. That is, when a BerkeleyDB has
 * a secondary DB, it is possible to use the keys of the secondary DB to get primary
 * key and data of the primary DB. This result set returns primary keys based on secondary
 * keys and it ignores the data (which usually will simply be the same thing as the secondary key).
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class SingleValueResultSet<T> extends IndexResultSet<T>
{
    private DatabaseEntry pkey = new DatabaseEntry();
    
    public SingleValueResultSet(BDBTxCursor cursor, DatabaseEntry keyIn, ByteArrayConverter<T> converter)
    {
        //
    	// The following is bit hacky because we want to avoid some of the default behavior
    	// of the super constructor, which is incorrect when the "values" we are interested in 
    	// are the DB's primary keys. So we duplicate its behavior and override instantiation
    	// of the current value.
        this.converter = converter;
        this.cursor = cursor;
        this.key = new DatabaseEntry();
        this.data = new DatabaseEntry();
        // TODO: for fixed size key and data,we should actually reuse the buffers, but
        // this has to be passed somehow as a configuration parameter to the HGIndex
        // implementation and down to result sets. It's a worthwhile optimization.
        this.key.setReuseBuffer(false);
        this.data.setReuseBuffer(false);
        if (keyIn != null)
        	assignData(key, keyIn.getData());        
	    try
	    {
	        ((SecondaryCursor)cursor.cursor()).getCurrent(key, pkey, data, LockMode.DEFAULT);
	        next = converter.fromByteArray(pkey.getData(), pkey.getOffset(), pkey.getSize());
	        lookahead = 1;
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
            OperationStatus status = ((SecondaryCursor)cursor.cursor()).getNextDup(key, pkey, data, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS)
                return converter.fromByteArray(pkey.getData(), pkey.getOffset(), pkey.getSize());
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
        		pkey = new DatabaseEntry();
            OperationStatus status = ((SecondaryCursor)cursor.cursor()).getPrevDup(key, pkey, data, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS)
                return converter.fromByteArray(pkey.getData(), pkey.getOffset(), pkey.getSize());
            else
                return null;
        }
        catch (Throwable t)
        {
            closeNoException();
            throw new HGException(t);
        }                        
    }
    
    public GotoResult goTo(T value, boolean exactMatch)
    {
    	byte [] B = converter.toByteArray(value);
    	assignData(pkey, B);
    	try
    	{
    		if (exactMatch)
    		{
    			if (((SecondaryCursor)cursor.cursor()).getSearchBoth(key, pkey, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
    			{
    				positionToCurrent(pkey.getData(), pkey.getOffset(), pkey.getSize());
    				return GotoResult.found; 
    			}
    			else
    				return GotoResult.nothing;
    		}
    		else
    		{
    			if (((SecondaryCursor)cursor.cursor()).getSearchBothRange(key, pkey, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
    			{
    				positionToCurrent(pkey.getData(), pkey.getOffset(), pkey.getSize());
    				return HGUtils.eq(B, pkey.getData()) ? GotoResult.found : GotoResult.close;
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
    	return false;
    }
}
