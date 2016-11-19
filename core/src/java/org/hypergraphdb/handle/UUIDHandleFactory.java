package org.hypergraphdb.handle;

import java.io.IOException;
import java.io.InputStream;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;

/**
 * 
 * <p>
 * A persistent handle factory based on type IV (crypto-strong) UUIDs. This is 
 * the default handle factory. The handle produced by it a universally unique 
 * and play well in a distributed environment.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class UUIDHandleFactory implements HGHandleFactory
{
    private final HGPersistentHandle anyHandle = makeHandle("332c5a05-37c2-11dc-b44d-8884da7d2355");
    private final HGPersistentHandle topType = makeHandle("bb8854fb-0d34-11da-ac60-932fd7ea200d");
    private final HGPersistentHandle linkType = makeHandle("db8854fb-0d34-11da-ac60-932fd7ea200d");
    private final HGPersistentHandle nullType = makeHandle("db733325-19d5-11db-8b55-23bc8177d6ec");
    private final HGPersistentHandle subsumesType = makeHandle("eb8854fb-0d34-11da-ac60-932fd7ea200d");

    public static final UUIDHandleFactory I = new UUIDHandleFactory();
    
    public HGPersistentHandle anyHandle()
    {
        return anyHandle;
    }
    
    public HGPersistentHandle makeHandle()
    {
        return UUIDPersistentHandle.makeHandle();
    }

    public HGPersistentHandle makeHandle(String handleAsString)
    {
        return UUIDPersistentHandle.makeHandle(handleAsString);
    }


    public HGPersistentHandle makeHandle(byte[] buffer)
    {
        return UUIDPersistentHandle.makeHandle(buffer);
    }

    public HGPersistentHandle makeHandle(byte[] buffer, int offset)
    {
        return UUIDPersistentHandle.makeHandle(buffer, offset);
    }

    @Override
	public HGPersistentHandle makeHandle(InputStream in)
	{
		try
		{
			byte[] toBuf = new byte[UUIDPersistentHandle.SIZE];
			int total = in.read(toBuf, 0, UUIDPersistentHandle.SIZE);			
			while (total < UUIDPersistentHandle.SIZE) 
			{
				int n = in.read(toBuf, total, UUIDPersistentHandle.SIZE - total);
				if (n <= 0)
					break;
				else
					total += n;
			}
			if (UUIDPersistentHandle.SIZE != total) {
				throw new IllegalArgumentException("Attempt to construct UUIDPersistentHandle with not enough bytes left in the input.");
			}
			return UUIDPersistentHandle.makeHandle(toBuf, 0);
		}
		catch (IOException e)
		{
			throw new HGException(e);
		}
	}
    
    public HGPersistentHandle nullHandle()
    {
        return UUIDPersistentHandle.nullHandle();
    }
    
    public HGPersistentHandle topTypeHandle()
    {
        return topType;
    }

    public HGPersistentHandle nullTypeHandle()
    {
        return nullType;
    }

    public HGPersistentHandle linkTypeHandle()
    {
        return linkType;
    }

    public HGPersistentHandle subsumesTypeHandle()
    {
        return subsumesType;
    }    
}