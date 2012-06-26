package org.hypergraphdb.pithos;

import org.hypergraphdb.HGPersistentHandle;

public class UPHandle implements HGPersistentHandle
{
	private static final long serialVersionUID = 1L;
	
	private long global, local;

	public UPHandle(long global, long local)
	{
		this.global = global;
		this.local = local;
	}
	
	public long getGlobal() { return global; }
	public long getLocal() { return local; }
	
	public HGPersistentHandle getPersistent()
	{
		return this;
	}

	public int compareTo(HGPersistentHandle o)
	{
		UPHandle x = (UPHandle)o;
		if (global == x.global)
			return local > x.local ? 1 : local < x.local ? -1 : 0;
		else if (global > x.global)
			return 1;
		else
			return -1;
	}

	public byte[] toByteArray()
	{
		byte [] A = new byte[16];
		return A;
	}

	public String toStringValue()
	{
		return "" + global + ":" + local;
	}
}