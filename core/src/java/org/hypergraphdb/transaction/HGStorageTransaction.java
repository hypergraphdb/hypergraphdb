package org.hypergraphdb.transaction;


/*
 * 
 * <p>
 * Represents a transaction object. The interface is minimal on purpose, exposing
 * only the two essential operations one can do one a transaction.
 * </p>
*/
public interface HGStorageTransaction
{
    void commit() throws HGTransactionException;
    void abort() throws HGTransactionException;
}