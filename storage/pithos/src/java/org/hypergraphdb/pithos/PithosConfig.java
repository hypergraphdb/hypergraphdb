package org.hypergraphdb.pithos;

public class PithosConfig
{
	// Whether memory buffers should be written to disk as soon
	// new data is written to them
	private boolean forceWrite;

	public int getHandleSize()
	{
		return 16;
	}
	
	public int getPageSize()
	{
		return 4096;
	}
	
	public int getPointerSize()
	{
		return 8;
	}
	
	public boolean isForceWrite()
	{
		return forceWrite;
	}

	public void setForceWrite(boolean forceWrite)
	{
		this.forceWrite = forceWrite;
	}	
}