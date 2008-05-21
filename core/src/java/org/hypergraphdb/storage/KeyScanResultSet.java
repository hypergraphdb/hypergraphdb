package org.hypergraphdb.storage;

import org.hypergraphdb.HGException;
import org.hypergraphdb.transaction.BDBTxCursor;
import org.hypergraphdb.util.HGUtils;

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
public class KeyScanResultSet<T> extends IndexResultSet<T>
{

	@Override
	protected T advance()
	{
        try
        {
            OperationStatus status = cursor.cursor().getNextNoDup(key, data, LockMode.DEFAULT);
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
	protected T back()
	{
        try
        {
            OperationStatus status = cursor.cursor().getPrevNoDup(key, data, LockMode.DEFAULT);
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
    
    public KeyScanResultSet(BDBTxCursor cursor, DatabaseEntry key, ByteArrayConverter<T> converter)
    {
        this.converter = converter;
        this.cursor = cursor;
        this.key = key == null ? new DatabaseEntry() : key;
	    try
	    {
	        cursor.cursor().getCurrent(key, data, LockMode.DEFAULT);
	        next = converter.fromByteArray(key.getData());
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
    	key.setData(B);
    	try
    	{
    		if (exactMatch)
    		{
    			if (cursor.cursor().getSearchKey(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
    			{
    				positionToCurrent(key.getData());
    				return GotoResult.found;
    			}
    			else
    				return GotoResult.nothing;
    		}
    		else 
    		{
    			byte [] save = new byte[key.getData().length];
    			System.arraycopy(key.getData(), 0, save, 0, save.length);    		
    			if (cursor.cursor().getSearchKeyRange(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
    			{
    				positionToCurrent(key.getData());    				
    				return HGUtils.eq(save, key.getData()) ? GotoResult.found : GotoResult.close;
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
