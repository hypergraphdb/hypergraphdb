package org.hypergraphdb;

public class HGEMissingData extends HGException
{
	private static final long serialVersionUID = -1;
	private HGPersistentHandle handle;

	public HGEMissingData()
	{
		super("Missing data in storage.");
	}
	
	public HGEMissingData(HGPersistentHandle handle)
	{
		super("Missing data in storage for handle '" + handle + "'");
		this.handle = handle;
	}
	
	public HGPersistentHandle getHandle()
	{
		return handle;
	}

	public void setHandle(HGPersistentHandle handle)
	{
		this.handle = handle;
	}	
}