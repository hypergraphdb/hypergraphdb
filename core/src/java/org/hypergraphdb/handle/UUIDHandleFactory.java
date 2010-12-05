package org.hypergraphdb.handle;

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

    public HGPersistentHandle nullHandle()
    {
        return UUIDPersistentHandle.nullHandle();
    }
}