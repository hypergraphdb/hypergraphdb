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
public class KeyScanResultSet<T> extends IndexResultSet<T>
{
	@Override
	protected T advance()
	{
				checkCursor();
        try
        {
	        	OperationStatus status = cursor.cursor().get(CursorOp.NEXT_NODUP, key, data);
	          if (status == OperationStatus.SUCCESS)
	          	return converter.fromByteArray(key.getData(), 0, key.getData().length);
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
				checkCursor();
				try
        {
						OperationStatus status = cursor.cursor().get(CursorOp.PREV_NODUP, key, data);
	          if (status == OperationStatus.SUCCESS)
	          	return converter.fromByteArray(key.getData(), 0, key.getData().length);
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
		return true;
	}
    
    public KeyScanResultSet(LmdbTxCursor cursor, DatabaseEntry keyIn, ByteArrayConverter<T> converter)
    {
        this.converter = converter;
        this.cursor = cursor;
        this.key = new DatabaseEntry();
        this.data = new DatabaseEntry();

        if (keyIn != null)
        	assignData(key, keyIn.getData());
		    try
		    {
		        cursor.cursor().get(CursorOp.GET_CURRENT, key, data);
		        next = converter.fromByteArray(key.getData(), 0, key.getData().length);
		        lookahead = 1;
		    }
		    catch (Throwable t)
		    {
		        throw new HGException(t);
		    }         
    } 	
    
    public GotoResult goTo(T value, boolean exactMatch)
    {
    	byte [] B = converter.toByteArray(value);
    	assignData(key, B);
    	try
    	{
    		if (exactMatch)
    		{
					OperationStatus status = cursor.cursor().get(CursorOp.SET, key, data);
          if (status == OperationStatus.SUCCESS) {
    				positionToCurrent(key.getData());
    				return GotoResult.found;
    			}
    			else
    				return GotoResult.nothing;
    		}
    		else 
    		{
					OperationStatus status = cursor.cursor().get(CursorOp.SET_RANGE, key, data);
          if (status == OperationStatus.SUCCESS) {
    				positionToCurrent(key.getData());
    				return HGUtils.eq(B, key.getData()) ? GotoResult.found : GotoResult.close;
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
}
