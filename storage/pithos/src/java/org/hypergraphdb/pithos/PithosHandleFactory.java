package org.hypergraphdb.pithos;

import java.util.UUID;

import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;


public class PithosHandleFactory implements HGHandleFactory
{
	private long global = 1;
	private long local = 0;
	
	public UPHandle makeHandle()
	{
		return new UPHandle(global, local++);
	}

	public UPHandle makeHandle(String handleAsString)
	{
		UUID uuid = UUID.fromString(handleAsString);
		return new UPHandle(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
	}

	public UPHandle makeHandle(byte[] buffer)
	{
		UUID uuid = UUID.nameUUIDFromBytes(buffer);
		return new UPHandle(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
	}

	public UPHandle makeHandle(byte[] data, int offset)
	{
        long msb = 0;
        long lsb = 0;
        for (int i=offset; i<8+offset; i++)
            msb = (msb << 8) | (data[i+offset] & 0xff);
        for (int i=8+offset; i<16+offset; i++)
            lsb = (lsb << 8) | (data[i+offset] & 0xff);
        return new UPHandle(msb, lsb);
	}

	public UPHandle nullHandle()
	{
		return new UPHandle(0,0);
	}

	public UPHandle anyHandle()
	{
		return new UPHandle(0, 1);
	}

	public UPHandle topTypeHandle()
	{
		return new UPHandle(0,2);		
	}

	public UPHandle nullTypeHandle()
	{
		return new UPHandle(0,3);		
	}

	public UPHandle linkTypeHandle()
	{
		return new UPHandle(0,4);		
	}

	public UPHandle subsumesTypeHandle()
	{
		return new UPHandle(0,5);		
	}
}