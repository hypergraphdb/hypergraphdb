package org.hypergraphdb.transaction;

public interface HGTransactionFactory
{
	HGTransaction createTransaction(HGTransaction parent);
}
