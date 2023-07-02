package org.hypergraphdb.storage.lmdb;

import org.hypergraphdb.transaction.HGStorageTransaction;
import org.hypergraphdb.transaction.HGTransactionException;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

public class StorageTransactionLMDB<BufferType> implements HGStorageTransaction
{
	private Txn<BufferType> txn;
	private Env<BufferType> env;
	
	public static final <BufferType> StorageTransactionLMDB<BufferType> nullTransaction()
	{
		return new StorageTransactionLMDB<BufferType>(null, null);
	}
	
	
	public StorageTransactionLMDB(Txn<BufferType> txn, Env<BufferType> env)
	{
		this.txn = txn;
		this.env = env;
	}
	
	public Txn<BufferType> lmdbTxn()
	{
		return this.txn;
	}
	
	@Override
	public void commit() throws HGTransactionException
	{
	    boolean retry = true;
	    boolean resized = false;
	    while (retry) 
	    {
	        retry = false;
    	    try
    	    {
        		if (this.txn != null)
        			txn.commit();
        		if (resized)
        		    System.out.println("SUCCESS AFTER RESIZE!!");
            }
            catch (Exception ex)
            {
                if (!resized && ex.toString().contains("Environment mapsize reached"))
                {
                    System.out.println("RETRY AFTER RESIZE");
                    this.env.setMapSize(0);
                    retry = true;
                    resized = true;
                }
            }
	    }
	}

	@Override
	public void abort() throws HGTransactionException
	{
		if (this.txn != null)
			txn.abort();
	}
}
