package org.hypergraphdb.transaction;

public class TransactionIsReadonlyException extends RuntimeException 
{
	private static final long serialVersionUID = 8459438200787796597L;
	
	public TransactionIsReadonlyException()
	{
	    super("Transaction configured as read-only was used to modify data!");
	}
}