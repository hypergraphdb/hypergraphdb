package org.hypergraphdb.transaction;

public class TransactionIsReadonlyException extends RuntimeException 
{
	private static final long serialVersionUID = 8459438200787796597L;
	
	private String additionalInfo = null;
	
	public TransactionIsReadonlyException()
	{
	    super("Transaction configured as read-only was used to modify data!");
	}
	
	public TransactionIsReadonlyException(String additionalInfo)
	{
	    this();
	    this.additionalInfo = additionalInfo;
	}	
	
	public String toString()
	{
		String msg = super.toString();
		if (additionalInfo != null)
			msg += " More context: " + additionalInfo;
		return msg;
	}
}