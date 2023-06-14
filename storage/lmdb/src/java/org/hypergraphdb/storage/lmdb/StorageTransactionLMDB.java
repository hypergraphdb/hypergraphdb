package org.hypergraphdb.storage.lmdb;

import org.hypergraphdb.transaction.HGStorageTransaction;
import org.hypergraphdb.transaction.HGTransactionException;
import org.lmdbjava.Txn;

public class StorageTransactionLMDB<BufferType> implements HGStorageTransaction
{
	private Txn<BufferType> txn;

	public static final <BufferType> StorageTransactionLMDB<BufferType> nullTransaction()
	{
		return new StorageTransactionLMDB<BufferType>(null);
	}
	
	
	public StorageTransactionLMDB(Txn<BufferType> txn)
	{
		this.txn = txn;
	}
	
	public Txn<BufferType> lmdbTxn()
	{
		return this.txn;
	}
	
	@Override
	public void commit() throws HGTransactionException
	{
		if (this.txn != null)
			txn.commit();
	}

	@Override
	public void abort() throws HGTransactionException
	{
		if (this.txn != null)
			txn.abort();
	}
}
