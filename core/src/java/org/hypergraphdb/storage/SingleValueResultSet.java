/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.storage;

import org.hypergraphdb.HGException;
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
public class SingleValueResultSet extends IndexResultSet
{
    private DatabaseEntry pkey = new DatabaseEntry();
    
    public SingleValueResultSet()
    {        
    }
    
    public SingleValueResultSet(SecondaryCursor cursor, DatabaseEntry key, ByteArrayConverter converter)
    {
        //
    	// The following is bit hacky because we want to avoid some of the default behavior
    	// of the super constructor, which is incorrect when the "values" we are interested in 
    	// are the DB's primary keys. So we duplicate its bebavior and override instantiation
    	// of the current value.
        this.converter = converter;
        this.cursor = cursor;
        this.key = key == null ? new DatabaseEntry() : key;        
	    try
	    {
	        ((SecondaryCursor)cursor).getCurrent(key, pkey, data, LockMode.DEFAULT);
	        next = converter.fromByteArray(pkey.getData());
	    }
	    catch (Throwable t)
	    {
	        throw new HGException(t);
	    }       
        
    }
    
    
    protected Object advance()
    {
        try
        {
            OperationStatus status = ((SecondaryCursor)cursor).getNextDup(key, pkey, data, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS)
                return converter.fromByteArray(pkey.getData());
            else
                return null;
        }
        catch (Throwable t)
        {
            closeNoException();
            throw new HGException(t);
        }     
    }

    protected Object back()
    {
        try
        {
            OperationStatus status = ((SecondaryCursor)cursor).getPrevDup(key, pkey, data, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS)
                return converter.fromByteArray(pkey.getData());
            else
                return null;
        }
        catch (Throwable t)
        {
            closeNoException();
            throw new HGException(t);
        }                        
    }
    
    public GotoResult goTo(Object value, boolean exactMatch)
    {
    	byte [] B = converter.toByteArray(value);
    	pkey.setData(B);
    	try
    	{
    		if (exactMatch)
    		{
    			if (((SecondaryCursor)cursor).getSearchBoth(key, pkey, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
    			{
    				positionToCurrent(pkey.getData());
    				return GotoResult.found; 
    			}
    			else
    				return GotoResult.nothing;
    		}
    		else
    		{
    			byte [] save = new byte[pkey.getData().length];
    			System.arraycopy(pkey.getData(), 0, save, 0, save.length);    		
    			if (((SecondaryCursor)cursor).getSearchBothRange(key, pkey, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
    			{
    				positionToCurrent(pkey.getData());
    				return HGUtils.eq(save, pkey.getData()) ? GotoResult.found : GotoResult.close;
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