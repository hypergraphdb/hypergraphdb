package org.hypergraphdb.transaction;

import org.hypergraphdb.HGException;
import org.hypergraphdb.util.CloseMe;

import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseException;

public final class BDBTxCursor implements CloseMe
{
	private TransactionBDBImpl tx;
	private Cursor cursor = null;
	private boolean open = true;
	
	public BDBTxCursor(Cursor cursor, TransactionBDBImpl tx)
	{
		this.cursor = cursor;
		this.tx = tx;
		open = cursor != null;
	}

	public Cursor cursor()
	{
		return cursor;
	}
	
	public boolean isOpen()
	{
		return open;
	}
	
	public void close()
	{
		if (!open)
			return;
		try 
		{
			cursor.close();
		}
		catch (DatabaseException ex)
		{
			throw new HGException(ex);
		}
		finally
		{
			open = false;
			cursor = null;
			if (tx != null)
				tx.removeCursor(this);
		}		
	}
}