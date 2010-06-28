package org.hypergraphdb.handle;

import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;

public class UUIDHandleFactory implements HGHandleFactory
{
    private final HGPersistentHandle anyHandle = makeHandle("332c5a05-37c2-11dc-b44d-8884da7d2355");

    public static final UUIDHandleFactory I = new UUIDHandleFactory();
    
    public HGPersistentHandle anyHandle()
    {
        return anyHandle;
    }

    private static long seed = 100;
    
    public HGPersistentHandle makeHandle()
    {
//        byte [] data = new byte[16];
//        long v = ++seed;
//        data[8] = (byte) ((v >>> 56)); 
//        data[9] = (byte) ((v >>> 48));
//        data[10] = (byte) ((v >>> 40)); 
//        data[11] = (byte) ((v >>> 32));
//        data[12] = (byte) ((v >>> 24)); 
//        data[13] = (byte) ((v >>> 16));
//        data[14] = (byte) ((v >>> 8)); 
//        data[15] = (byte) ((v >>> 0));
//        return UUIDPersistentHandle.makeHandle(data);        
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