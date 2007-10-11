package org.hypergraphdb.storage;

import org.hypergraphdb.HGException;
import org.hypergraphdb.util.HGUtils;

import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;

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
public class KeyScanResultSet extends IndexResultSet
{

	@Override
	protected Object advance()
	{
        try
        {
            OperationStatus status = cursor.getNextNoDup(key, data, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS)
            	return converter.fromByteArray(key.getData());
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
	protected Object back()
	{
        try
        {
            OperationStatus status = cursor.getPrevNoDup(key, data, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS)
                return converter.fromByteArray(key.getData());
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
	
    public KeyScanResultSet()
    {            
    }
    
    public KeyScanResultSet(Cursor cursor, DatabaseEntry key, ByteArrayConverter converter)
    {
        this.converter = converter;
        this.cursor = cursor;
        this.key = key == null ? new DatabaseEntry() : key;
	    try
	    {
	        cursor.getCurrent(key, data, LockMode.DEFAULT);
	        next = converter.fromByteArray(key.getData());
	    }
	    catch (Throwable t)
	    {
	        throw new HGException(t);
	    }         
    } 	
    
    public GotoResult goTo(Object value, boolean exactMatch)
    {
    	byte [] B = converter.toByteArray(value);
    	key.setData(B);
    	try
    	{
    		if (exactMatch)
    			return cursor.getSearchKey(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS ?
    				   GotoResult.found : GotoResult.nothing;
    		else 
    		{
    			byte [] save = new byte[key.getData().length];
    			System.arraycopy(key.getData(), 0, save, 0, save.length);    		
    			if (cursor.getSearchKeyRange(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)    		
    				return HGUtils.eq(save, key.getData()) ? GotoResult.found : GotoResult.close;
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
